//! Sink throughput and snapshot compaction at scale on SQLite.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mink_metadata::{Command, CommandSink, Outcome};
use mink_sql::{Config, Sink, SqliteStore, Store};
use tokio::task::JoinSet;

const NODE: i32 = 1;
const EPOCH: i64 = 1;
const SNAPSHOT_EVERY: u64 = 512;
const CONCURRENCY: usize = 1024;

fn config() -> Config {
    Config {
        poll_interval: Duration::from_millis(1),
        snapshot_every: SNAPSHOT_EVERY,
        snapshot_min_interval: Duration::ZERO,
        ..Config::default()
    }
}

async fn run_gate(total: u64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scale.db");
    let store: Arc<dyn Store> = Arc::new(SqliteStore::open(&path).await.unwrap());

    let (sink, views) = Sink::open(store.clone(), config()).await.unwrap();
    let sink = Arc::new(sink);
    sink.propose(Command::RegisterNode {
        node_id: NODE,
        node_epoch: EPOCH,
        http_address: String::new(),
        slots: 1,
        protocol_addresses: Default::default(),
    })
    .await
    .unwrap();

    let started = Instant::now();
    let mut ids: Vec<u64> = Vec::with_capacity(total as usize);
    let mut inflight = JoinSet::new();
    let mut submitted = 0u64;
    while ids.len() < total as usize {
        while submitted < total && inflight.len() < CONCURRENCY {
            let sink = sink.clone();
            inflight.spawn(async move {
                sink.propose(Command::CreateStream {
                    node_id: NODE,
                    node_epoch: EPOCH,
                })
                .await
                .unwrap()
                .result
            });
            submitted += 1;
        }
        match inflight
            .join_next()
            .await
            .expect("inflight not empty")
            .unwrap()
        {
            Outcome::Id(id) => ids.push(id),
            other => panic!("unexpected result {other:?}"),
        }
    }
    let elapsed = started.elapsed();
    let per_sec = total as f64 / elapsed.as_secs_f64();
    println!("{total} creates through Sink: {elapsed:?} ({per_sec:.0}/s)");

    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len() as u64, total, "every create got a unique id");
    assert_eq!(views.load().state.streams.len() as u64, total);

    let last = store.last_idx().await.unwrap();
    println!(
        "log rows for {total} commands: {last} ({:.1} commands/row)",
        total as f64 / last as f64
    );
    assert!(
        last < total / 2,
        "group commit should coalesce: {last} rows for {total} commands"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    let surviving = loop {
        let rows = store.fetch_after(0, u32::MAX).await.unwrap().len() as u64;
        if rows <= 2 * SNAPSHOT_EVERY {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "log never truncated: {rows} rows"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    println!("surviving log rows: {surviving} (snapshot_every = {SNAPSHOT_EVERY})");
    drop(sink);

    let started = Instant::now();
    let store: Arc<dyn Store> = Arc::new(SqliteStore::open(&path).await.unwrap());
    let (sink, views) = Sink::open(store, config()).await.unwrap();
    println!("cold start with {total} streams: {:?}", started.elapsed());
    assert_eq!(views.load().state.streams.len() as u64, total);
    let next = sink
        .propose(Command::CreateStream {
            node_id: NODE,
            node_epoch: EPOCH,
        })
        .await
        .unwrap();
    assert_eq!(
        next.result,
        Outcome::Id(total),
        "id counter survived at scale"
    );
}

#[tokio::test]
async fn twenty_k_creates_gate() {
    run_gate(20_000).await;
}

#[tokio::test]
#[ignore = "run explicitly: cargo test --release -p mink-sql --test scale -- --ignored --nocapture"]
async fn million_creates_gate() {
    run_gate(1_000_000).await;
}
