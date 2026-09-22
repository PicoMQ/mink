//! Two sinks sharing one Postgres: leadership handover and consistent views.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use mink_metadata::{Command, CommandSink, Outcome};
use mink_sql::{Config, Lease, LeaseConfig, PgStore, Sink, Store};
use sqlx::PgPool;
use tokio::time::Instant;

fn config() -> Config {
    Config {
        poll_interval: Duration::from_millis(5),
        snapshot_every: 4,
        snapshot_min_interval: Duration::ZERO,
        ..Config::default()
    }
}

fn register(node_id: i32, node_epoch: i64) -> Command {
    Command::RegisterNode {
        node_id,
        node_epoch,
        http_address: String::new(),
        slots: 1,
        protocol_addresses: Default::default(),
    }
}

#[tokio::test]
async fn postgres_sink_multi_writer_snapshot_and_lease() {
    let Ok(url) = env::var("MINK_PG_URL") else {
        eprintln!("MINK_PG_URL not set; skipping postgres e2e");
        return;
    };

    let admin = PgPool::connect(&url).await.expect("connect for cleanup");
    sqlx::query("DROP TABLE IF EXISTS meta_log, meta_snapshot, meta_lease")
        .execute(&admin)
        .await
        .expect("drop tables");
    admin.close().await;

    let store_a: Arc<dyn Store> = Arc::new(PgStore::connect(&url).await.unwrap());
    let store_b: Arc<dyn Store> = Arc::new(PgStore::connect(&url).await.unwrap());
    let (sink_a, views_a) = Sink::open(store_a.clone(), config()).await.unwrap();
    let (sink_b, views_b) = Sink::open(store_b.clone(), config()).await.unwrap();
    let (sink_a, sink_b) = (Arc::new(sink_a), Arc::new(sink_b));

    sink_a.propose(register(1, 10)).await.unwrap();
    sink_b.propose(register(2, 20)).await.unwrap();

    let mut handles = Vec::new();
    for i in 0..60u32 {
        let (sink, node_id, node_epoch) = if i % 2 == 0 {
            (sink_a.clone(), 1, 10)
        } else {
            (sink_b.clone(), 2, 20)
        };
        handles.push(tokio::spawn(async move {
            sink.propose(Command::CreateStream {
                node_id,
                node_epoch,
            })
            .await
            .unwrap()
            .result
        }));
    }
    let mut ids = Vec::new();
    for handle in handles {
        match handle.await.unwrap() {
            Outcome::Id(id) => ids.push(id),
            other => panic!("unexpected result {other:?}"),
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..60).collect::<Vec<u64>>());

    let target = views_a
        .load()
        .applied_index
        .max(views_b.load().applied_index);
    let view_a = views_a.wait_applied(target).await;
    let view_b = views_b.wait_applied(target).await;
    assert_eq!(view_a.state, view_b.state);
    assert_eq!(view_a.state.streams.len(), 60);

    let deadline = Instant::now() + Duration::from_secs(10);
    while store_a.load_snapshot().await.unwrap().is_none() {
        assert!(Instant::now() < deadline, "snapshot never written");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let lease = LeaseConfig {
        ttl_ms: 1_000,
        check_interval: Duration::from_millis(50),
    };
    let keeper_a = Lease::spawn(store_a.clone(), "node-a".into(), lease.clone());
    let mut rx_a = keeper_a.leadership();
    tokio::time::timeout(Duration::from_secs(5), rx_a.wait_for(|v| *v))
        .await
        .expect("a never became leader")
        .unwrap();
    let keeper_b = Lease::spawn(store_b.clone(), "node-b".into(), lease);
    let mut rx_b = keeper_b.leadership();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!*rx_b.borrow(), "two leaders on one lease");
    keeper_a.shutdown().await;
    tokio::time::timeout(Duration::from_secs(5), rx_b.wait_for(|v| *v))
        .await
        .expect("b never took over")
        .unwrap();
    keeper_b.shutdown().await;

    drop(sink_a);
    drop(sink_b);
    let store: Arc<dyn Store> = Arc::new(PgStore::connect(&url).await.unwrap());
    let (sink, views) = Sink::open(store, config()).await.unwrap();
    assert_eq!(views.load().state.streams.len(), 60);
    let next = sink
        .propose(Command::CreateStream {
            node_id: 1,
            node_epoch: 10,
        })
        .await
        .unwrap();
    assert_eq!(next.result, Outcome::Id(60), "id counter survived restart");
}
