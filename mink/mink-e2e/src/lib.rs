//! Shared harness for the end to end scenarios: the stack description from `MINK_E2E_*`, a client,
//! table and batch builders, readers over every tier, an independent Iceberg REST view of the
//! lake, docker control for chaos, a Kafka client and a Flight SQL client.

pub mod bench;
pub mod docker;
pub mod kafka;
pub mod lake;
pub mod rows;
pub mod sql;
pub mod tables;

use std::env;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mink_client::{Admin, Cluster, Error};
use tokio::time::Instant;

pub use mink_client;
pub use mink_table;
pub use mink_types;

pub const WAIT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub struct Env {
    pub scenario: String,
    pub nodes: Vec<String>,
    pub kafka: Vec<String>,
    pub lake: bool,
    pub query: Vec<String>,
    pub iceberg_rest: String,
    pub warehouse: String,
    pub s3_endpoint: String,
    pub compose_project: String,
}

impl Env {
    pub fn load() -> Option<Env> {
        if env::var("MINK_E2E").ok().as_deref() != Some("1") {
            return None;
        }
        let list = |key: &str, fallback: &str| -> Vec<String> {
            env::var(key)
                .unwrap_or_else(|_| fallback.to_owned())
                .split(',')
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect()
        };
        let var = |key: &str, fallback: &str| env::var(key).unwrap_or_else(|_| fallback.to_owned());
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_test_writer()
            .try_init();

        Some(Env {
            scenario: var("MINK_E2E_SCENARIO", "single"),
            nodes: list("MINK_E2E_NODES", "grpc://127.0.0.1:9123"),
            kafka: list("MINK_E2E_KAFKA", "127.0.0.1:9092"),
            lake: var("MINK_E2E_LAKE", "1") == "1",
            query: list("MINK_E2E_QUERY", ""),
            iceberg_rest: var("MINK_E2E_ICEBERG_REST", "http://127.0.0.1:8181"),
            warehouse: var("MINK_E2E_WAREHOUSE", "s3://mink-lake"),
            s3_endpoint: var("MINK_E2E_S3_ENDPOINT", "http://127.0.0.1:9000"),
            compose_project: var("MINK_E2E_COMPOSE_PROJECT", "mink-e2e"),
        })
    }

    pub fn multi_node(&self) -> bool {
        self.nodes.len() > 1
    }

    pub fn cluster(&self) -> Cluster {
        Cluster::connect_all(self.nodes.clone()).expect("bootstrap addresses parse")
    }

    pub fn kafka_bootstrap(&self) -> String {
        self.kafka.join(",")
    }

    pub async fn ready(&self) -> Cluster {
        let cluster = self.cluster();
        let admin = cluster.admin();
        let want = self.nodes.len();
        wait_for("cluster ready", async || {
            let info = admin.describe_cluster().await?;
            let live = info.nodes.iter().filter(|n| n.live).count();
            Ok((live >= want && info.coordinator.is_some()).then_some(()))
        })
        .await;
        cluster
    }
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

pub fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{:x}_{n}", (nanos as u64) & 0xffff_ffff_ffff)
}

pub async fn fresh_database(admin: &Admin, prefix: &str) -> String {
    let name = unique(prefix);
    admin
        .create_database(&name, None, Default::default(), false)
        .await
        .expect("create database");
    name
}

pub async fn wait_for<T, F>(what: &str, mut probe: impl FnMut() -> F) -> T
where
    F: Future<Output = Result<Option<T>, Error>>,
{
    wait_for_within(what, WAIT, &mut probe).await
}

pub async fn wait_for_within<T, F>(what: &str, within: Duration, probe: &mut impl FnMut() -> F) -> T
where
    F: Future<Output = Result<Option<T>, Error>>,
{
    let deadline = Instant::now() + within;
    let mut last: Option<Error> = None;
    loop {
        match probe().await {
            Ok(Some(value)) => return value,
            Ok(None) => {}
            Err(e) if e.is_retriable() || transient(&e) => last = Some(e),
            Err(e) => panic!("{what}: {e}"),
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {within:?}: {what} (last error: {last:?})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn transient(error: &Error) -> bool {
    let text = error.to_string();
    matches!(error, Error::Status(s) if matches!(
        s.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Aborted
            | tonic::Code::FailedPrecondition
            | tonic::Code::Unknown
    )) || text.contains("transport error")
        || text.contains("connection refused")
        || text.contains("broken pipe")
        || text.contains("not the leader")
        || text.contains("no leader")
        || text.contains("fenced")
}

pub async fn retry<T, F>(what: &str, mut op: impl FnMut() -> F) -> T
where
    F: Future<Output = Result<T, Error>>,
{
    wait_for(what, || {
        let fut = op();
        async move { fut.await.map(Some) }
    })
    .await
}
