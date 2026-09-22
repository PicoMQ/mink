//! Drives the `mink bench` load generator as a separate process and parses its report.

use std::env;
use std::process::Stdio;

use mink_table::{Column, Descriptor, Schema};
use mink_types::DataType;
use serde::Deserialize;
use tokio::process::Command;

#[derive(Debug, Clone, Deserialize)]
pub struct Report {
    pub table: String,
    pub mode: String,
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

#[derive(Debug, Clone, Deserialize)]
pub struct Latency {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdRange {
    pub first: i64,
    pub last: i64,
    pub distinct: u64,
}

#[derive(Debug, Clone)]
pub struct Bench {
    pub bootstrap: String,
    pub table: String,
    pub args: Vec<String>,
}

pub fn descriptor(buckets: u32) -> Descriptor {
    let schema = Schema::builder()
        .column(Column::new("id", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("ts", DataType::big_int()).unwrap())
        .column(Column::new("writer", DataType::int()).unwrap())
        .column(Column::new("payload", DataType::string()).unwrap())
        .build()
        .unwrap();
    Descriptor::builder(schema)
        .bucket_count(buckets)
        .bucket_keys(["id"])
        .build()
        .unwrap()
}

pub fn binary() -> String {
    env::var("MINK_BIN").unwrap_or_else(|_| "/target/debug/mink".to_owned())
}

impl Bench {
    pub fn new(bootstrap: &str, table: &str) -> Bench {
        Bench {
            bootstrap: bootstrap.to_owned(),
            table: table.to_owned(),
            args: Vec::new(),
        }
    }

    pub fn arg(mut self, flag: &str, value: impl ToString) -> Bench {
        if let Some(i) = self.args.iter().position(|a| a == flag) {
            self.args.drain(i..i + 2);
        }
        self.args.push(flag.to_owned());
        self.args.push(value.to_string());
        self
    }

    pub fn flag(mut self, flag: &str) -> Bench {
        self.args.push(flag.to_owned());
        self
    }

    pub fn append(self, rows: u64, batch: usize, concurrency: usize) -> Bench {
        self.arg("--mode", "append")
            .arg("--rows", rows)
            .arg("--batch", batch)
            .arg("--concurrency", concurrency)
    }

    pub fn upsert(self, rows: u64, batch: usize, concurrency: usize, key_space: u64) -> Bench {
        self.arg("--mode", "upsert")
            .arg("--rows", rows)
            .arg("--batch", batch)
            .arg("--concurrency", concurrency)
            .arg("--key-space", key_space)
    }

    pub fn create(self, buckets: u32, lake: bool) -> Bench {
        let bench = self.arg("--create", buckets);
        if lake { bench.flag("--lake") } else { bench }
    }

    pub fn sustained(self, duration: &str, pause_ms: u64, leader_wait: &str) -> Bench {
        self.arg("--rows", 1u64 << 40)
            .arg("--duration", duration)
            .arg("--pause-ms", pause_ms)
            .arg("--leader-wait", leader_wait)
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<Report> {
        tokio::spawn(async move { self.run().await })
    }

    pub async fn run(&self) -> Report {
        let output = Command::new(binary())
            .arg("-b")
            .arg(&self.bootstrap)
            .arg("--json")
            .arg("bench")
            .arg(&self.table)
            .args(&self.args)
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap_or_else(|e| panic!("spawn {}: {e}", binary()));
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "mink bench {} {:?} failed ({}):\n{stdout}\n{stderr}",
            self.table,
            self.args,
            output.status
        );
        let report: Report = serde_json::from_str(&stdout)
            .unwrap_or_else(|e| panic!("bench report {e}:\n{stdout}\n{stderr}"));
        tracing::info!(
            table = %report.table,
            mode = %report.mode,
            rows = report.rows_acked,
            errors = report.errors,
            stalls = report.stalls,
            rows_per_sec = report.rows_per_sec as u64,
            p50_ms = report.latency_ms.p50,
            p99_ms = report.latency_ms.p99,
            "bench done"
        );
        report
    }
}
