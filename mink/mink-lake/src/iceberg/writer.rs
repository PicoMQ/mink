//! Writes tiered batches as Parquet data and equality-delete files, keeping only the last change per key.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_row::{RowConverter, SortField};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use async_trait::async_trait;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::spec::{Literal, PartitionKey, PartitionSpec, Schema, Struct, Transform};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use mink_record::ChangeType;
use mink_table::PartitionName;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::iceberg::arrow::Layout;
use crate::iceberg::compact::{RewriteDataFiles, RewriteResult, rolling};
use crate::iceberg::write::{FileContext, WriteResult};
use crate::writer::{TieredBatch, WriterContext};

type DataWriter = <DataFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
> as IcebergWriterBuilder>::R;
type DeleteWriter = <EqualityDeleteFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
> as IcebergWriterBuilder>::R;

pub(crate) struct Writer {
    arrow: Layout,
    context: FileContext,
    mode: Mode,
    compaction: Option<Compaction>,
}

enum Mode {
    Append {
        data: DataWriter,
    },
    Delta {
        data: DataWriter,
        deletes: Deletes,
        keys: RowConverter,
        key_columns: Vec<usize>,
        buffered: Vec<RecordBatch>,
        latest: BTreeMap<Vec<u8>, Slot>,
        retract_inserts: bool,
    },
}

enum Deletes {
    Equality(Box<DeleteWriter>),
}

impl Deletes {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        match self {
            Deletes::Equality(writer) => Ok(writer.write(batch).await?),
        }
    }

    async fn close(&mut self) -> Result<Vec<iceberg::spec::DataFile>> {
        match self {
            Deletes::Equality(writer) => Ok(writer.close().await?),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    batch: usize,
    row: usize,
    offset: i64,
    live: bool,
    // An equality delete is needed only if the key's first change this round was a retract.
    needs_delete: bool,
}

impl Writer {
    pub(crate) async fn open(table: &Table, context: &WriterContext) -> Result<Self> {
        let metadata = table.metadata();
        let schema = metadata.current_schema().clone();
        let spec = metadata.default_partition_spec().clone();
        let properties = metadata.table_properties()?;
        if !properties
            .write_format_default
            .eq_ignore_ascii_case("parquet")
        {
            return Err(Error::invalid(format!(
                "tables are written as Parquet; `write.format.default` is {}",
                properties.write_format_default
            )));
        }

        let bucket = context.bucket.bucket().0 as i32;
        let arrow = Layout::new(&schema)?;
        let partition_key =
            partition_key(&spec, schema.clone(), context.partition.as_ref(), bucket)?;
        let file_context = FileContext {
            partition_type: spec.partition_type(&schema)?,
            partition_spec_id: spec.spec_id(),
            format_version: metadata.format_version(),
            schema: schema.as_ref().clone(),
        };

        let prefix = format!("{bucket:05}-0-{}", Uuid::now_v7());
        let data = DataFileWriterBuilder::new(rolling(table, &prefix, "data", schema.clone())?)
            .build(Some(partition_key.clone()))
            .await?;

        let compaction = context.descriptor.options().lake_auto_compaction.then(|| {
            Compaction::spawn(
                RewriteDataFiles::new(table.clone(), partition_key.clone()),
                prefix.clone(),
            )
        });

        let identifiers: Vec<i32> = schema.identifier_field_ids().collect();
        let mode = if identifiers.is_empty() {
            Mode::Append { data }
        } else {
            let config = EqualityDeleteWriterConfig::new(identifiers.clone(), schema.clone())?;
            let delete_schema =
                Arc::new(arrow_schema_to_schema(config.projected_arrow_schema_ref())?);
            let deletes = EqualityDeleteFileWriterBuilder::new(
                rolling(table, &prefix, "deletes", delete_schema)?,
                config,
            )
            .build(Some(partition_key))
            .await?;
            let top_level = schema.as_struct().fields();
            let key_columns: Vec<usize> = identifiers
                .iter()
                .map(|id| {
                    top_level.iter().position(|f| f.id == *id).ok_or_else(|| {
                        Error::invalid(format!("identifier field {id} is not top-level"))
                    })
                })
                .collect::<Result<_>>()?;
            let keys = RowConverter::new(
                key_columns
                    .iter()
                    .map(|i| SortField::new(arrow.schema().field(*i).data_type().clone()))
                    .collect(),
            )?;
            Mode::Delta {
                data,
                deletes: Deletes::Equality(Box::new(deletes)),
                keys,
                key_columns,
                buffered: Vec::new(),
                latest: BTreeMap::new(),
                retract_inserts: context.descriptor.options().lake_attach,
            }
        };

        Ok(Writer {
            arrow,
            context: file_context,
            mode,
            compaction,
        })
    }
}

struct Compaction(Option<JoinHandle<Result<Option<RewriteResult>>>>);

impl Compaction {
    fn spawn(rewrite: RewriteDataFiles, prefix: String) -> Self {
        Compaction(Some(tokio::spawn(
            async move { rewrite.execute(&prefix).await },
        )))
    }

    async fn finish(mut self) -> Option<RewriteResult> {
        let task = self.0.take().expect("finished once");
        let result = task.await.map_err(Error::other).and_then(|r| r);

        match result {
            Ok(result) => result,
            Err(e) => {
                tracing::info!(error = %e, "iceberg data file compaction failed");
                None
            }
        }
    }
}

impl Drop for Compaction {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl crate::writer::Writer<WriteResult> for Writer {
    async fn write(&mut self, batch: &TieredBatch) -> Result<()> {
        if batch.rows.num_rows() == 0 {
            return Ok(());
        }

        let converted = self.arrow.convert(batch)?;
        match &mut self.mode {
            Mode::Append { data } => data.write(converted).await?,
            Mode::Delta {
                keys,
                key_columns,
                buffered,
                latest,
                retract_inserts,
                ..
            } => {
                let key_arrays: Vec<ArrayRef> = key_columns
                    .iter()
                    .map(|i| converted.column(*i).clone())
                    .collect();
                let rows = keys.convert_columns(&key_arrays)?;
                let index = buffered.len();
                for row in 0..converted.num_rows() {
                    let live = match batch.changes.get(row) {
                        ChangeType::Insert | ChangeType::UpdateAfter | ChangeType::AppendOnly => {
                            true
                        }
                        ChangeType::UpdateBefore | ChangeType::Delete => false,
                    };
                    let key = rows.row(row).as_ref().to_vec();
                    let needs_delete = match latest.get(&key) {
                        Some(seen) => seen.needs_delete,
                        None => !live || *retract_inserts,
                    };
                    latest.insert(
                        key,
                        Slot {
                            batch: index,
                            row,
                            offset: batch.base_offset + row as i64,
                            live,
                            needs_delete,
                        },
                    );
                }
                buffered.push(converted);
            }
        }

        Ok(())
    }

    async fn complete(self: Box<Self>) -> Result<WriteResult> {
        let this = *self;
        let (data_files, delete_files) = match this.mode {
            Mode::Append { mut data } => (data.close().await?, Vec::new()),
            Mode::Delta {
                mut data,
                mut deletes,
                buffered,
                latest,
                ..
            } => {
                if !buffered.is_empty() {
                    let all = concat_batches(this.arrow.schema(), &buffered)?;
                    let starts: Vec<usize> = buffered
                        .iter()
                        .scan(0usize, |acc, b| {
                            let start = *acc;
                            *acc += b.num_rows();
                            Some(start)
                        })
                        .collect();
                    let at = |slot: &Slot| (starts[slot.batch] + slot.row) as u64;

                    let mut survivors: Vec<&Slot> = latest.values().filter(|s| s.live).collect();
                    survivors.sort_by_key(|slot| slot.offset);
                    let rows = UInt64Array::from_iter_values(survivors.iter().map(|s| at(s)));
                    if !rows.is_empty() {
                        data.write(take_record_batch(&all, &rows)?).await?;
                    }

                    let retracted = UInt64Array::from_iter_values(
                        latest.values().filter(|s| s.needs_delete).map(at),
                    );
                    if !retracted.is_empty() {
                        deletes.write(take_record_batch(&all, &retracted)?).await?;
                    }
                }
                (data.close().await?, deletes.close().await?)
            }
        };

        let rewrite = match this.compaction {
            Some(compaction) => compaction.finish().await,
            None => None,
        };

        Ok(WriteResult {
            data_files,
            delete_files,
            rewrite,
            context: this.context,
        })
    }
}

pub fn partition_key(
    spec: &PartitionSpec,
    schema: Arc<Schema>,
    partition: Option<&PartitionName>,
    bucket: i32,
) -> Result<PartitionKey> {
    let mut values = partition
        .map(|name| {
            name.as_str()
                .split('$')
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
        .into_iter();

    let mut data = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            Error::invalid(format!("unknown partition source {}", field.source_id))
        })?;
        let literal = match &field.transform {
            Transform::Bucket(_) => Literal::int(bucket),
            Transform::Identity => {
                let value = values.next().ok_or_else(|| {
                    Error::invalid(format!(
                        "Iceberg table is partitioned by `{}` but the bucket has no partition",
                        source.name
                    ))
                })?;
                Literal::string(value)
            }
            other => {
                return Err(Error::invalid(format!(
                    "unsupported partition transform {other} on {}",
                    source.name
                )));
            }
        };
        data.push(Some(literal));
    }
    if values.next().is_some() {
        return Err(Error::invalid(format!(
            "partition {} has more values than the Iceberg spec has identity fields",
            partition.map(|p| p.as_str()).unwrap_or_default()
        )));
    }

    Ok(PartitionKey::new(
        spec.clone(),
        schema,
        Struct::from_iter(data),
    ))
}
