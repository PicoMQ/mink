//! The stream engine over the SQL sink end to end, including a cold restart from disk.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mink_metadata::{Command, CommandSink, Handle};
use mink_metadata::{Lifecycle, ObjectCleaner};
use mink_sql::{Lease, LeaseConfig, Sink, SqliteStore, Store};
use s3stream::{
    AppendContext, Config, CreateStreamOptions, FetchContext, KVClient as _, KeyValue,
    MemoryObjectStorage, ObjectStorageTrait, ObjectWalConfig, ObjectWalService, OpenStreamOptions,
    RecordBatch, S3StreamBuilder,
};
use tokio::time::Instant;

const NODE_ID: i32 = 1;
const NODE_EPOCH: i64 = 1;

fn config() -> mink_sql::Config {
    mink_sql::Config {
        poll_interval: Duration::from_millis(1),
        snapshot_every: 8,
        snapshot_min_interval: Duration::ZERO,
        ..mink_sql::Config::default()
    }
}

#[tokio::test]
async fn engine_end_to_end_on_sql_metadata_plane() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("meta.db");

    let store: Arc<dyn Store> = Arc::new(SqliteStore::open(&db_path).await.unwrap());
    let (sink, views) = Sink::open(store.clone(), config()).await.unwrap();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let handle = Handle::new(NODE_ID, NODE_EPOCH, sink.clone(), views.clone());
    handle
        .register("http://node-1:9090", 1, Default::default())
        .await
        .unwrap();

    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
    let mut wal_config = ObjectWalConfig::defaults();
    wal_config.cluster_id = "sql-e2e".into();
    wal_config.node_id = NODE_ID as u32;
    wal_config.epoch = NODE_EPOCH as u64;

    let engine = S3StreamBuilder::new(Config::default())
        .object_storage(object_storage.clone())
        .write_ahead_log(Arc::new(ObjectWalService::new(wal_storage, wal_config)))
        .stream_manager(Arc::new(handle.stream_manager()))
        .object_manager(Arc::new(handle.object_manager()))
        .kv_client(Arc::new(handle.kv_client()))
        .build()
        .await
        .unwrap();

    let client = engine.stream_client();
    let stream = client
        .create_and_open_stream(CreateStreamOptions {
            epoch: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let stream_id = stream.stream_id();
    for i in 0..10u64 {
        let result = stream
            .append(
                AppendContext::default(),
                RecordBatch::new(1, 0, Bytes::from(vec![i as u8; 128])),
            )
            .await
            .unwrap();
        assert_eq!(result.base_offset, i);
    }
    let second = client
        .create_and_open_stream(CreateStreamOptions {
            epoch: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    second
        .append(
            AppendContext::default(),
            RecordBatch::new(1, 0, Bytes::from(vec![9u8; 64])),
        )
        .await
        .unwrap();
    stream.close().await.unwrap();
    let index = engine.range_index_cache();
    let deadline = Instant::now() + Duration::from_secs(5);
    while index.search_object_id(stream_id, 0).is_none() {
        assert!(
            Instant::now() < deadline,
            "range index never saw the commit"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    second.close().await.unwrap();

    {
        let view = views.load();
        assert_eq!(view.state.get_stream(stream_id).unwrap().end_offset, 10);
        assert!(view.state.objects_count() >= 1);
        assert!(view.state.get_opening_streams(NODE_ID).is_empty());
    }

    let reopened = client
        .open_stream(
            stream_id,
            OpenStreamOptions {
                epoch: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(reopened.next_offset(), 10);
    let fetched = reopened
        .fetch(FetchContext::default(), 0, 10, usize::MAX)
        .await
        .unwrap();
    let total: u64 = fetched
        .records
        .iter()
        .map(|r| r.last_offset - r.base_offset)
        .sum();
    assert_eq!(total, 10);
    reopened.close().await.unwrap();

    let kv = handle.kv_client();
    kv.put_kv(KeyValue {
        key: "topic/0".into(),
        value: Bytes::from_static(b"42"),
    })
    .await
    .unwrap();
    assert_eq!(
        kv.get_kv("topic/0").await.unwrap(),
        Some(Bytes::from_static(b"42"))
    );

    engine.shutdown().await;

    sink.propose(Command::PrepareObject {
        node_id: NODE_ID,
        node_epoch: NODE_EPOCH,
        count: 1,
        ttl_ms: 1,
        now_ms: 0,
    })
    .await
    .unwrap();
    assert!(views.load().state.prepared_objects_count() >= 1);

    let keeper = Lease::spawn(
        store.clone(),
        format!("node-{NODE_ID}"),
        LeaseConfig {
            ttl_ms: 500,
            check_interval: Duration::from_millis(20),
        },
    );
    let cleaner = Arc::new(ObjectCleaner::new(
        sink.clone(),
        views.clone(),
        Some(object_storage),
    ));
    let lifecycle = Arc::new(Lifecycle::new(
        sink.clone(),
        cleaner,
        Duration::from_millis(5),
    ));
    let driver = lifecycle.clone().drive(keeper.leadership());

    let deadline = Instant::now() + Duration::from_secs(5);
    while views.load().state.prepared_objects_count() != 0 {
        assert!(
            Instant::now() < deadline,
            "lifecycle never expired the prepare"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    driver.abort();
    keeper.shutdown().await;

    let snapshot_state = views.load().state.clone();
    drop(handle);
    drop(sink);

    let store: Arc<dyn Store> = Arc::new(SqliteStore::open(&db_path).await.unwrap());
    let (sink, views) = Sink::open(store, config()).await.unwrap();
    let restored = views.load();
    assert_eq!(
        restored.state, snapshot_state,
        "cold start must reproduce the exact state"
    );
    assert_eq!(restored.state.get_stream(stream_id).unwrap().end_offset, 10);
    assert_eq!(restored.state.get_stream(stream_id).unwrap().epoch, 2);
    assert_eq!(
        restored.state.get_kv("topic/0"),
        Some(Bytes::from_static(b"42"))
    );

    let handle = Handle::new(NODE_ID, NODE_EPOCH, Arc::new(sink), views);
    use s3stream::StreamManagerTrait as _;

    handle
        .stream_manager()
        .close_stream(stream_id, 1)
        .await
        .expect_err("stale epoch must be fenced after restart");
}
