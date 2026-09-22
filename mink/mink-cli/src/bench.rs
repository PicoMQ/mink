//! The `bench` command: concurrent append or upsert load against one table with latency
//! percentiles and a summary the load and chaos scenarios assert on.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use clap::ValueEnum;
use mink_client::{Cluster, Error, Table};
use mink_table::{Column, Descriptor, LakeFormat, Options, Path, PrimaryKey, Schema};
use mink_types::DataType;
use serde::Serialize;

use crate::output::Printer;
use crate::parse;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Append,
    Upsert,
}

#[derive(clap::Args)]
pub struct Bench {
    #[arg(help = "database.table to load")]
    table: String,
    #[arg(long, value_enum, default_value = "append")]
    mode: Mode,
    #[arg(long, default_value_t = 100_000, help = "rows to write in total")]
    rows: u64,
    #[arg(long, default_value_t = 1_000, help = "rows per write")]
    batch: usize,
    #[arg(long, default_value_t = 4, help = "concurrent writers")]
    concurrency: usize,
    #[arg(
        long,
        default_value_t = 0,
        help = "first id; ids are unique across writers"
    )]
    start: i64,
    #[arg(
        long,
        default_value_t = 0,
        help = "upsert: ids wrap modulo this so later writes update earlier rows (0 = never)"
    )]
    key_space: u64,
    #[arg(long, default_value_t = 64, help = "bytes of payload per row")]
    payload: usize,
    #[arg(long, help = "stop after this long even if rows remain, e.g. 30s")]
    duration: Option<String>,
    #[arg(
        long,
        help = "create the table first (bench schema), with this many buckets"
    )]
    create: Option<u32>,
    #[arg(long, help = "with --create: tier the table to the lake")]
    lake: bool,
    #[arg(
        long,
        default_value = "60s",
        help = "how long a write keeps retrying while a bucket has no leader"
    )]
    leader_wait: String,
    #[arg(
        long,
        default_value_t = 0,
        help = "sleep this many ms between writes per writer"
    )]
    pause_ms: u64,
}

#[derive(Serialize)]
pub struct Report {
    pub table: String,
    pub mode: Mode,
    pub concurrency: usize,
    pub batch: usize,
    pub rows_requested: u64,
    pub rows_acked: u64,
    pub batches_acked: u64,
    pub errors: u64,
    pub stalls: u64,
    pub elapsed_ms: u64,
    pub rows_per_sec: f64,
    pub bytes_per_sec: f64,
    pub latency_ms: Latency,
    pub ids: IdRange,
}

#[derive(Serialize)]
pub struct Latency {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub mean: f64,
}

#[derive(Serialize)]
pub struct IdRange {
    pub first: i64,
    pub last: i64,
    pub distinct: u64,
}

pub fn schema() -> Schema {
    Schema::builder()
        .column(Column::new("id", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("ts", DataType::big_int()).unwrap())
        .column(Column::new("writer", DataType::int()).unwrap())
        .column(Column::new("payload", DataType::string()).unwrap())
        .build()
        .unwrap()
}

pub fn pk_schema() -> Schema {
    Schema::builder()
        .column(Column::new("id", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("ts", DataType::big_int()).unwrap())
        .column(Column::new("writer", DataType::int()).unwrap())
        .column(Column::new("payload", DataType::string()).unwrap())
        .primary_key(PrimaryKey::new(vec!["id".into()]).unwrap())
        .build()
        .unwrap()
}

pub fn descriptor(mode: Mode, buckets: u32, lake: bool) -> Descriptor {
    let schema = match mode {
        Mode::Append => schema(),
        Mode::Upsert => pk_schema(),
    };
    let mut builder = Descriptor::builder(schema).bucket_count(buckets);
    if mode == Mode::Append {
        builder = builder.bucket_keys(["id"]);
    }
    if lake {
        builder = builder.options(Options {
            lake: Some(LakeFormat::Iceberg),
            lake_freshness: Duration::from_secs(1),
            ..Options::default()
        });
    }
    builder.build().unwrap()
}

struct Shared {
    acked_rows: AtomicU64,
    acked_batches: AtomicU64,
    errors: AtomicU64,
    stalls: AtomicU64,
}

impl Bench {
    pub async fn run(self, cluster: &Cluster, out: &Printer) -> anyhow::Result<()> {
        let path: Path = self.table.parse()?;
        let leader_wait = parse::duration("leader-wait", &self.leader_wait)?;
        let cluster = cluster.clone().with_leader_wait(leader_wait);
        let admin = cluster.admin();
        if let Some(buckets) = self.create {
            admin
                .create_table(&path, &descriptor(self.mode, buckets, self.lake), true)
                .await?;
        }
        let table = cluster.table(&path).await?;
        if self.mode == Mode::Upsert && !table.descriptor().has_primary_key() {
            anyhow::bail!("{path} has no primary key; use --mode append");
        }
        if self.mode == Mode::Append && table.descriptor().has_primary_key() {
            anyhow::bail!("{path} is a primary key table; use --mode upsert");
        }
        let deadline = match &self.duration {
            Some(text) => Some(Instant::now() + parse::duration("duration", text)?),
            None => None,
        };

        let shared = Arc::new(Shared {
            acked_rows: AtomicU64::new(0),
            acked_batches: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            stalls: AtomicU64::new(0),
        });
        let concurrency = self.concurrency.max(1);
        let per_writer = self.rows.div_ceil(concurrency as u64);
        let started = Instant::now();
        let mut handles = Vec::with_capacity(concurrency);
        for w in 0..concurrency {
            let table = table.clone();
            let shared = shared.clone();
            let first = self.start + (w as u64 * per_writer) as i64;
            let rows = per_writer.min(self.rows.saturating_sub(w as u64 * per_writer));
            let params = Params {
                mode: self.mode,
                batch: self.batch.max(1),
                payload: self.payload,
                key_space: self.key_space,
                writer: w as i32,
                pause: Duration::from_millis(self.pause_ms),
                deadline,
            };
            handles.push(tokio::spawn(async move {
                writer(table, shared, first, rows, params).await
            }));
        }
        let mut latencies: Vec<f64> = Vec::new();
        let mut first_id = i64::MAX;
        let mut last_id = i64::MIN;
        let mut distinct = 0u64;
        for handle in handles {
            let done = handle.await??;
            latencies.extend(done.latencies_ms);
            if done.rows > 0 {
                first_id = first_id.min(done.first);
                last_id = last_id.max(done.last);
                distinct += done.distinct;
            }
        }
        let elapsed = started.elapsed();
        latencies.sort_by(|a, b| a.total_cmp(b));
        let pct = |p: f64| -> f64 {
            if latencies.is_empty() {
                return 0.0;
            }
            let idx = ((latencies.len() as f64 - 1.0) * p).round() as usize;
            latencies[idx]
        };
        let rows_acked = shared.acked_rows.load(Ordering::Relaxed);
        let secs = elapsed.as_secs_f64().max(f64::EPSILON);
        let row_bytes = (8 + 8 + 4 + self.payload) as f64;
        let report = Report {
            table: path.to_string(),
            mode: self.mode,
            concurrency,
            batch: self.batch,
            rows_requested: self.rows,
            rows_acked,
            batches_acked: shared.acked_batches.load(Ordering::Relaxed),
            errors: shared.errors.load(Ordering::Relaxed),
            stalls: shared.stalls.load(Ordering::Relaxed),
            elapsed_ms: elapsed.as_millis() as u64,
            rows_per_sec: rows_acked as f64 / secs,
            bytes_per_sec: rows_acked as f64 * row_bytes / secs,
            latency_ms: Latency {
                p50: pct(0.50),
                p95: pct(0.95),
                p99: pct(0.99),
                max: latencies.last().copied().unwrap_or(0.0),
                mean: if latencies.is_empty() {
                    0.0
                } else {
                    latencies.iter().sum::<f64>() / latencies.len() as f64
                },
            },
            ids: IdRange {
                first: if rows_acked > 0 { first_id } else { self.start },
                last: if rows_acked > 0 { last_id } else { self.start },
                distinct: if self.key_space > 0 {
                    distinct.min(self.key_space)
                } else {
                    distinct
                },
            },
        };
        out.emit(&report, || {
            format!(
                "{} {:?} x{}: {} rows acked in {:.2}s ({:.0} rows/s, {:.1} MiB/s), \
                 {} errors, {} stalls over 1s\nlatency ms p50 {:.1} p95 {:.1} p99 {:.1} max {:.1}\nids {}..={} ({} distinct)",
                report.table,
                report.mode,
                report.concurrency,
                report.rows_acked,
                elapsed.as_secs_f64(),
                report.rows_per_sec,
                report.bytes_per_sec / (1024.0 * 1024.0),
                report.errors,
                report.stalls,
                report.latency_ms.p50,
                report.latency_ms.p95,
                report.latency_ms.p99,
                report.latency_ms.max,
                report.ids.first,
                report.ids.last,
                report.ids.distinct,
            )
        })?;
        if report.rows_acked < report.rows_requested && deadline.is_none() {
            anyhow::bail!(
                "only {} of {} rows acked",
                report.rows_acked,
                report.rows_requested
            );
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Params {
    mode: Mode,
    batch: usize,
    payload: usize,
    key_space: u64,
    writer: i32,
    pause: Duration,
    deadline: Option<Instant>,
}

struct Done {
    latencies_ms: Vec<f64>,
    rows: u64,
    first: i64,
    last: i64,
    distinct: u64,
}

async fn writer(
    table: Table,
    shared: Arc<Shared>,
    first: i64,
    rows: u64,
    params: Params,
) -> Result<Done, Error> {
    enum Writer {
        Append(mink_client::Append),
        Upsert(mink_client::Upsert),
    }
    let mut writer = match params.mode {
        Mode::Append => Writer::Append(table.append_writer().await?),
        Mode::Upsert => Writer::Upsert(table.upsert_writer().await?),
    };
    let arrow = table.arrow_schema();
    let payload: String = "x".repeat(params.payload);
    let mut latencies_ms = Vec::with_capacity((rows as usize).div_ceil(params.batch).min(1 << 16));
    let mut written = 0u64;
    let mut last = first;
    while written < rows {
        if params.deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        let n = params.batch.min((rows - written) as usize);
        let base = first + written as i64;
        let ids: Vec<i64> = (0..n as i64)
            .map(|i| {
                let id = base + i;
                if params.key_space > 0 {
                    id.rem_euclid(params.key_space as i64)
                } else {
                    id
                }
            })
            .collect();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default();
        let batch = RecordBatch::try_new(
            arrow.clone(),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(vec![now; n])),
                Arc::new(arrow_array::Int32Array::from(vec![params.writer; n])),
                Arc::new(StringArray::from(vec![payload.as_str(); n])),
            ],
        )?;
        let started = Instant::now();
        let result = match &mut writer {
            Writer::Append(w) => w.append(&batch).await,
            Writer::Upsert(w) => w.upsert(&batch).await,
        };
        match result {
            Ok(routed) => {
                let acked: usize = routed.iter().map(|b| b.rows).sum();
                let latency = started.elapsed();
                latencies_ms.push(latency.as_secs_f64() * 1000.0);
                if latency > Duration::from_secs(1) {
                    shared.stalls.fetch_add(1, Ordering::Relaxed);
                }
                shared.acked_rows.fetch_add(acked as u64, Ordering::Relaxed);
                shared.acked_batches.fetch_add(1, Ordering::Relaxed);
                written += n as u64;
                last = base + n as i64 - 1;
            }
            Err(e) => {
                shared.errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(writer = params.writer, error = %e, "write failed");
                return Err(e);
            }
        }
        if !params.pause.is_zero() {
            tokio::time::sleep(params.pause).await;
        }
    }

    Ok(Done {
        latencies_ms,
        rows: written,
        first,
        last,
        distinct: written,
    })
}
