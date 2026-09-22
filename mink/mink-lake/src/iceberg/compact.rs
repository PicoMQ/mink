//! Rewrites groups of small data files in a partition into larger ones, alongside a tiering write.

use std::mem;
use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use iceberg::Runtime;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, PartitionKey, Schema,
    SnapshotRef,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use crate::error::Result;

const MIN_FILES_TO_COMPACT: usize = 3;
const DEFAULT_TARGET_SIZE: u64 = 128 * 1024 * 1024;
const SPLIT_SIZE_PROPERTY: &str = "read.split.target-size";

pub(crate) type Rolling = RollingFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

pub(crate) fn rolling(
    table: &Table,
    prefix: &str,
    suffix: &str,
    schema: Arc<Schema>,
) -> Result<Rolling> {
    let metadata = table.metadata();
    let properties = metadata.table_properties()?;

    Ok(RollingFileWriterBuilder::new(
        ParquetWriterBuilder::new(WriterProperties::default(), schema),
        properties.write_target_file_size_bytes,
        table.file_io().clone(),
        DefaultLocationGenerator::new(metadata)?,
        DefaultFileNameGenerator::new(
            prefix.to_string(),
            Some(suffix.to_string()),
            DataFileFormat::Parquet,
        ),
    ))
}

#[derive(Debug, Clone, PartialEq)]
pub struct RewriteResult {
    pub snapshot_id: i64,
    pub deleted: Vec<DataFile>,
    pub added: Vec<DataFile>,
}

pub(crate) struct RewriteDataFiles {
    table: Table,
    partition: PartitionKey,
    target_size: u64,
}

struct Candidate {
    file: DataFile,
    sequence_number: i64,
}

impl RewriteDataFiles {
    pub(crate) fn new(table: Table, partition: PartitionKey) -> Self {
        RewriteDataFiles {
            target_size: target_size_of(&table),
            table,
            partition,
        }
    }

    pub(crate) async fn execute(&self, prefix: &str) -> Result<Option<RewriteResult>> {
        let Some(snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(None);
        };

        let groups = self.plan(snapshot).await?;
        if groups.is_empty() {
            return Ok(None);
        }

        tracing::info!(
            table = %self.table.identifier(),
            groups = groups.len(),
            files = groups.iter().map(Vec::len).sum::<usize>(),
            "rewriting small iceberg files"
        );
        let mut deleted = Vec::new();
        let mut added = Vec::new();
        for (index, group) in groups.into_iter().enumerate() {
            match self.rewrite(&group, &format!("{prefix}-c{index}")).await {
                Ok(files) => {
                    added.extend(files);
                    deleted.extend(group);
                }
                Err(e) => {
                    for file in &added {
                        let _ = self.table.file_io().delete(file.file_path()).await;
                    }
                    return Err(e);
                }
            }
        }

        Ok(Some(RewriteResult {
            snapshot_id: snapshot.snapshot_id(),
            deleted,
            added,
        }))
    }

    async fn plan(&self, snapshot: &SnapshotRef) -> Result<Vec<Vec<DataFile>>> {
        let io = self.table.file_io();
        let list = self.table.manifest_list_reader(snapshot).load().await?;
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut delete_floor: Option<i64> = None;
        for manifest_file in list.entries() {
            let manifest = manifest_file.load_manifest(io).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() || entry.data_file().partition() != self.partition.data() {
                    continue;
                }
                let sequence_number = entry.sequence_number().unwrap_or(0);
                match (manifest_file.content, entry.data_file().content_type()) {
                    (ManifestContentType::Data, DataContentType::Data) => {
                        candidates.push(Candidate {
                            file: entry.data_file().clone(),
                            sequence_number,
                        });
                    }
                    _ => {
                        delete_floor = Some(
                            delete_floor
                                .map_or(sequence_number, |floor| floor.max(sequence_number)),
                        );
                    }
                }
            }
        }

        // Rewritten rows take a new sequence number and would escape older deletes.
        if let Some(floor) = delete_floor {
            candidates.retain(|c| c.sequence_number > floor);
        }

        let small = candidates
            .iter()
            .filter(|c| c.file.file_size_in_bytes() < self.target_size)
            .count();
        if small < MIN_FILES_TO_COMPACT {
            return Ok(Vec::new());
        }

        candidates.sort_by(|a, b| {
            (a.sequence_number, a.file.file_path()).cmp(&(b.sequence_number, b.file.file_path()))
        });

        let mut groups: Vec<Vec<Candidate>> = Vec::new();
        let mut current: Vec<Candidate> = Vec::new();
        let mut weight = 0u64;
        for candidate in candidates {
            let size = candidate.file.file_size_in_bytes();
            if !current.is_empty() && weight + size > self.target_size {
                groups.push(mem::take(&mut current));
                weight = 0;
            }
            weight += size;
            current.push(candidate);
        }
        if !current.is_empty() {
            groups.push(current);
        }

        Ok(groups
            .into_iter()
            .filter(|group| group.len() > 1)
            .map(|group| group.into_iter().map(|c| c.file).collect())
            .collect())
    }

    async fn rewrite(&self, group: &[DataFile], prefix: &str) -> Result<Vec<DataFile>> {
        let metadata = self.table.metadata();
        let schema = metadata.current_schema().clone();
        let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?);
        let project_field_ids: Vec<i32> =
            schema.as_struct().fields().iter().map(|f| f.id).collect();
        let spec = Arc::new(metadata.default_partition_spec().as_ref().clone());
        let tasks: Vec<iceberg::Result<FileScanTask>> = group
            .iter()
            .map(|file| {
                Ok(FileScanTask {
                    file_size_in_bytes: file.file_size_in_bytes(),
                    start: 0,
                    length: file.file_size_in_bytes(),
                    record_count: Some(file.record_count()),
                    data_file_path: file.file_path().to_string(),
                    data_file_format: file.file_format(),
                    schema: schema.clone(),
                    project_field_ids: project_field_ids.clone(),
                    predicate: None,
                    deletes: Vec::new(),
                    partition: Some(file.partition().clone()),
                    partition_spec: Some(spec.clone()),
                    name_mapping: None,
                    case_sensitive: true,
                })
            })
            .collect();
        let mut rows =
            ArrowReaderBuilder::new(self.table.file_io().clone(), Runtime::try_current()?)
                .with_data_file_concurrency_limit(1)
                .build()
                .read(Box::pin(futures::stream::iter(tasks)))?
                .stream();

        let mut writer = DataFileWriterBuilder::new(rolling(&self.table, prefix, "data", schema)?)
            .build(Some(self.partition.clone()))
            .await?;
        while let Some(batch) = rows.try_next().await? {
            let columns = batch
                .columns()
                .iter()
                .zip(arrow_schema.fields())
                .map(|(column, field)| {
                    if column.data_type() == field.data_type() {
                        Ok(column.clone())
                    } else {
                        arrow_cast::cast(column, field.data_type())
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            writer
                .write(RecordBatch::try_new(arrow_schema.clone(), columns)?)
                .await?;
        }

        Ok(writer.close().await?)
    }
}

fn target_size_of(table: &Table) -> u64 {
    let metadata = table.metadata();
    let split = metadata
        .properties()
        .get(SPLIT_SIZE_PROPERTY)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_TARGET_SIZE);
    let target = metadata
        .table_properties()
        .map(|p| p.write_target_file_size_bytes as u64)
        .unwrap_or(DEFAULT_TARGET_SIZE);

    split.min(target)
}
