//! The `read` and `write` commands: rows of a table out as JSON lines, JSON lines from stdin in.

use std::io;
use std::io::{Cursor, Write as _};
use std::sync::Arc;

use anyhow::{Context, bail};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_json::{LineDelimitedWriter, ReaderBuilder};
use arrow_schema::{DataType as ArrowType, Field, Schema, SchemaRef};
use futures::StreamExt;
use futures::stream::BoxStream;
use mink_client::{Batch, Cluster, proto};
use mink_record::ChangeType;
use mink_table::{Bucket, PartitionName, Path};
use tokio::io::AsyncReadExt;

use crate::buckets;
use crate::output::Printer;

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    #[value(help = "the log (the changelog, for a primary-key table)")]
    Log,
    #[value(help = "current rows of a primary-key table, in key order")]
    Snapshot,
    #[value(help = "lake history then log tail, merged")]
    Union,
}

#[derive(clap::Args)]
pub struct Read {
    path: Path,
    #[arg(long, help = "one bucket; every bucket when omitted")]
    bucket: Option<u32>,
    #[arg(long)]
    partition: Option<PartitionName>,
    #[arg(long, value_enum, default_value_t = Mode::Log)]
    mode: Mode,
    #[arg(
        long,
        default_value = "earliest",
        help = "earliest, latest, or an offset (log mode)"
    )]
    from: String,
    #[arg(long, short = 'f', help = "keep reading as rows arrive (log mode)")]
    follow: bool,
    #[arg(long, short = 'n', help = "stop after this many rows")]
    limit: Option<usize>,
    #[arg(
        long,
        help = "add __bucket, __offset and __change to each row (log mode)"
    )]
    meta: bool,
}

impl Read {
    pub async fn run(self, cluster: &Cluster) -> anyhow::Result<()> {
        let admin = cluster.admin();
        let table = cluster.table(&self.path).await?;
        let info = admin.get_table(&self.path).await?;
        let buckets = buckets::resolve(&admin, &info, self.bucket, self.partition.as_ref()).await?;

        let mut stdout = io::stdout().lock();
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        let mut emit = |batch: RecordBatch| -> anyhow::Result<bool> {
            let take = batch.num_rows().min(remaining);
            let batch = batch.slice(0, take);
            let mut writer = LineDelimitedWriter::new(Vec::new());
            writer.write(&batch)?;
            writer.finish()?;
            stdout.write_all(&writer.into_inner())?;
            stdout.flush()?;
            remaining -= take;
            Ok(remaining > 0)
        };

        match self.mode {
            Mode::Snapshot | Mode::Union => {
                if self.follow {
                    bail!("--follow applies to --mode log");
                }
                if self.mode == Mode::Snapshot && !info.descriptor.has_primary_key() {
                    bail!(
                        "{} is a log table; a snapshot needs a primary key",
                        self.path
                    );
                }
                'buckets: for bucket in buckets {
                    let mut rows: BoxStream<'static, Result<RecordBatch, mink_client::Error>> =
                        match self.mode {
                            Mode::Snapshot => table.snapshot(bucket, None).await?.batches,
                            _ => table.union(bucket, None).await?,
                        };

                    while let Some(batch) = rows.next().await {
                        if !emit(batch?)? {
                            break 'buckets;
                        }
                    }
                }
            }
            Mode::Log if self.follow => {
                let mut tails = Vec::with_capacity(buckets.len());
                for bucket in buckets {
                    let from = self.start(&table, bucket).await?;
                    tails.push(
                        table
                            .tail(bucket, from, None)
                            .await?
                            .map(move |b| (bucket, b)),
                    );
                }

                let mut merged = futures::stream::select_all(tails);
                loop {
                    let next = tokio::select! {
                        _ = tokio::signal::ctrl_c() => break,
                        next = merged.next() => next,
                    };
                    let Some((bucket, batch)) = next else { break };
                    let batch = batch?;
                    if batch.rows.num_rows() == 0 {
                        continue;
                    }
                    if !emit(self.decorate(bucket, batch)?)? {
                        break;
                    }
                }
            }
            Mode::Log => {
                'buckets: for bucket in buckets {
                    let from = self.start(&table, bucket).await?;
                    let (_, high_watermark) = table.offsets(bucket).await?;
                    if from >= high_watermark {
                        continue;
                    }

                    let mut scan = table.scan(bucket, from, high_watermark, None).await?;
                    while let Some(batch) = scan.next().await {
                        if !emit(self.decorate(bucket, batch?)?)? {
                            break 'buckets;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn start(&self, table: &mink_client::Table, bucket: Bucket) -> anyhow::Result<i64> {
        Ok(match self.from.as_str() {
            "earliest" => {
                table
                    .list_offset(bucket, proto::OffsetSpec::Earliest)
                    .await?
            }
            "latest" => table.list_offset(bucket, proto::OffsetSpec::Latest).await?,
            text => text.parse().with_context(|| {
                format!("--from: expected earliest, latest or an offset, got {text:?}")
            })?,
        })
    }

    fn decorate(&self, bucket: Bucket, batch: Batch) -> anyhow::Result<RecordBatch> {
        if !self.meta {
            return Ok(batch.rows);
        }

        let rows = batch.rows.num_rows();
        let label = buckets::text(bucket);
        let changes: Vec<&str> = match &batch.meta.changes {
            Some(bytes) => bytes
                .iter()
                .map(|b| ChangeType::from_byte(*b).map(ChangeType::short))
                .collect::<Result<_, _>>()?,
            None => vec![ChangeType::AppendOnly.short(); rows],
        };

        let mut fields: Vec<Field> = batch
            .rows
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new("__bucket", ArrowType::Utf8, false));
        fields.push(Field::new("__offset", ArrowType::Int64, false));
        fields.push(Field::new("__change", ArrowType::Utf8, false));
        let mut columns: Vec<ArrayRef> = batch.rows.columns().to_vec();
        columns.push(Arc::new(StringArray::from(vec![label.as_str(); rows])));
        columns.push(Arc::new(Int64Array::from_iter_values(
            (0..rows as i64).map(|i| batch.meta.base_offset + i),
        )));
        columns.push(Arc::new(StringArray::from(changes)));

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }
}

#[derive(clap::Args)]
pub struct Write {
    path: Path,
    #[arg(
        long,
        conflicts_with = "columns",
        help = "delete the keys in the rows instead of upserting them"
    )]
    delete: bool,
    #[arg(
        long,
        value_delimiter = ',',
        help = "only these columns are in the input; must include the primary key"
    )]
    columns: Option<Vec<String>>,
    #[arg(long, default_value_t = 1024, help = "rows per request")]
    batch_rows: usize,
}

impl Write {
    pub async fn run(self, cluster: &Cluster, out: &Printer) -> anyhow::Result<()> {
        let table = cluster.table(&self.path).await?;
        let full = table.arrow_schema();
        let primary_key = table.descriptor().has_primary_key();
        if (self.delete || self.columns.is_some()) && !primary_key {
            bail!(
                "{} is a log table; --delete and --columns need a primary key",
                self.path
            );
        }

        let (schema, projection): (SchemaRef, Option<Vec<usize>>) = match &self.columns {
            Some(names) => {
                let mut indexes = Vec::with_capacity(names.len());
                for name in names {
                    indexes.push(
                        full.index_of(name)
                            .with_context(|| format!("{} has no column {name}", self.path))?,
                    );
                }
                (Arc::new(full.project(&indexes)?), Some(indexes))
            }
            None => (full, None),
        };

        let mut input = Vec::new();
        tokio::io::stdin().read_to_end(&mut input).await?;
        let reader = ReaderBuilder::new(schema)
            .with_batch_size(self.batch_rows)
            .build(Cursor::new(input))?;

        let mut routed: Vec<proto::RoutedBucket> = Vec::new();
        let mut rows = 0usize;
        if primary_key {
            let mut writer = match projection {
                Some(indexes) => table.partial_update_writer(indexes).await?,
                None => table.upsert_writer().await?,
            };
            for batch in reader {
                let batch = batch.context("parse JSON rows")?;
                rows += batch.num_rows();
                let acks = if self.delete {
                    writer.delete(&batch).await?
                } else {
                    writer.upsert(&batch).await?
                };
                routed.extend(acks);
            }
        } else {
            let mut writer = table.append_writer().await?;
            for batch in reader {
                let batch = batch.context("parse JSON rows")?;
                rows += batch.num_rows();
                routed.extend(writer.append(&batch).await?);
            }
        }

        out.emit(
            &proto::Routed {
                buckets: routed.clone(),
            },
            || {
                let verb = if self.delete { "deleted" } else { "wrote" };
                let mut text = format!("{verb} {rows} rows to {}", self.path);
                for r in &routed {
                    let partition = r
                        .partition
                        .as_ref()
                        .map(|p| format!(" ({p})"))
                        .unwrap_or_default();
                    text.push_str(&format!(
                        "\n  {}{partition}: {} rows at offsets {}..={}",
                        buckets::text(r.bucket),
                        r.rows,
                        r.first_offset,
                        r.last_offset
                    ));
                }
                text
            },
        )
    }
}
