//! Node behavior on the local sink: opening and closing buckets, leadership changes, takeover, snapshots and retention.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use mink_common::{Clock, ManualClock};
use mink_coordinator::{Coordinator, MemoryLakeCatalog, NoopCleaner, StaticMembership};
use mink_kv::MemoryEngine;
use mink_metadata::{Command, CommandSink, Handle, LakeSnapshotRow, LocalSink, ViewPublisher};
use mink_record::{Batch, ChangeType, Compression, Spec, build, codec};
use mink_server::{
    Config, Error, Failover, Node, OffsetSpec, Owner, Retention, RetentionFrontier, Service,
};
use mink_table::{
    Bucket, BucketId, Column, Descriptor, Id, LakeFormat, LogFormat, Options, Path, PrimaryKey,
    Schema, SchemaId,
};
use mink_tablet::Put;
use mink_types::DataType;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use s3stream::{
    Client, MemoryObjectStorage, ObjectStorageTrait, ObjectWalConfig, ObjectWalService,
    S3StreamBuilder, StreamState,
};
use tokio::time::Instant;

const CLUSTER: &str = "mink-server-test";
const WAL_URI: &str = "0@mem://wal";

struct Cluster {
    sink: Arc<LocalSink>,
    views: Arc<ViewPublisher>,
    membership: Arc<StaticMembership>,
    clock: Arc<ManualClock>,
    data: Arc<dyn ObjectStorageTrait>,
    wal: Arc<dyn ObjectStorageTrait>,
    snapshots: Arc<dyn ObjectStore>,
    dir: tempfile::TempDir,
    coordinator: Coordinator,
}

impl Cluster {
    async fn new(nodes: &[i32]) -> Self {
        let (sink, views) = LocalSink::new();
        let sink = Arc::new(sink);
        let membership = Arc::new(StaticMembership::new(nodes.iter().copied()));
        let clock = Arc::new(ManualClock::new(1_700_000_000_000));
        let coordinator = Coordinator::new(
            100,
            "coordinator:9000",
            sink.clone(),
            views.clone(),
            membership.clone(),
            Arc::new(NoopCleaner),
            clock.clone(),
            mink_coordinator::Config::default(),
        )
        .with_lake_catalog(Arc::new(MemoryLakeCatalog::default()));
        let cluster = Cluster {
            sink,
            views,
            membership,
            clock,
            data: Arc::new(MemoryObjectStorage::new(0)),
            wal: Arc::new(MemoryObjectStorage::new(1)),
            snapshots: Arc::new(InMemory::new()),
            dir: tempfile::tempdir().unwrap(),
            coordinator,
        };
        for node in nodes {
            cluster.register(*node, 1).await;
        }
        cluster.coordinator.become_leader().await.unwrap();
        cluster
            .coordinator
            .create_database("db", None, Default::default(), false)
            .await
            .unwrap();
        cluster
    }

    async fn register(&self, node_id: i32, epoch: i64) -> Handle {
        let handle = Handle::new(node_id, epoch, self.sink.clone(), self.views.clone());
        handle
            .register(&format!("node{node_id}:9000"), 1, Default::default())
            .await
            .unwrap();
        handle
    }

    async fn node(&self, node_id: i32, epoch: i64) -> Arc<Node> {
        let handle = self.register(node_id, epoch).await;
        let mut wal_config = ObjectWalConfig::defaults();
        wal_config.cluster_id = CLUSTER.into();
        wal_config.node_id = node_id as u32;
        wal_config.epoch = epoch as u64;
        let engine = S3StreamBuilder::new(s3stream::Config {
            wal_upload_interval_ms: 20,
            ..s3stream::Config::default()
        })
        .object_storage(self.data.clone())
        .write_ahead_log(Arc::new(ObjectWalService::new(
            self.wal.clone(),
            wal_config,
        )))
        .stream_manager(Arc::new(handle.stream_manager()))
        .object_manager(Arc::new(handle.object_manager()))
        .kv_client(Arc::new(handle.kv_client()))
        .failover_factory(Arc::new(Failover::new(
            handle.clone(),
            CLUSTER,
            self.wal.clone(),
        )))
        .build()
        .await
        .unwrap();
        let engine: Arc<dyn Client> = Arc::new(engine);
        let mut config = Config::new(
            CLUSTER,
            WAL_URI,
            self.dir.path().join(format!("node{node_id}-e{epoch}")),
        );
        config.kv.compression = Compression::None;
        Arc::new(Node::new(
            handle,
            engine,
            self.snapshots.clone(),
            Arc::new(MemoryEngine),
            self.membership.clone(),
            self.clock.clone(),
            config,
        ))
    }

    async fn create(&self, name: &str, descriptor: Descriptor) -> Id {
        self.coordinator
            .create_table(&path(name), &descriptor, false)
            .await
            .unwrap()
            .unwrap()
    }

    fn leader_of(&self, bucket: Bucket) -> i32 {
        self.views.load().state.catalog.buckets[&bucket].leader
    }

    fn stream_state(&self, bucket: Bucket) -> (StreamState, i32, i64) {
        let view = self.views.load();
        let stream_id = view.state.catalog.buckets[&bucket].stream_id;
        let stream = view.state.streams[&stream_id];
        (stream.state, stream.node_id, stream.epoch)
    }

    async fn committed(&self, bucket: Bucket, offset: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let view = self.views.load();
            let stream_id = view.state.catalog.buckets[&bucket].stream_id;
            if view.state.streams[&stream_id].end_offset >= offset {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "stream {stream_id} never committed up to {offset}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn lead(&self, bucket: Bucket, node_id: i32) {
        let epoch = self
            .views
            .load()
            .state
            .catalog
            .coordinator
            .as_ref()
            .unwrap()
            .epoch;
        self.sink
            .propose(Command::LeadBucket {
                bucket,
                node_id,
                coordinator_epoch: epoch,
            })
            .await
            .unwrap();
    }
}

fn path(name: &str) -> Path {
    format!("db.{name}").parse().unwrap()
}

fn schema(primary_key: bool) -> Schema {
    let mut builder = Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap());
    if primary_key {
        builder = builder.primary_key(PrimaryKey::new(vec!["k".into()]).unwrap());
    }
    builder.build().unwrap()
}

fn log_table(buckets: u32) -> Descriptor {
    Descriptor::builder(schema(false))
        .bucket_count(buckets)
        .build()
        .unwrap()
}

fn pk_table(buckets: u32) -> Descriptor {
    Descriptor::builder(schema(true))
        .bucket_count(buckets)
        .build()
        .unwrap()
}

fn rows(rows: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(schema(false).fields())),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn batch(rows_: &[(i64, &str)]) -> Bytes {
    let codec = codec(LogFormat::Arrow, Compression::None);
    Bytes::from(
        build(
            Spec::new(SchemaId(0), true),
            &vec![ChangeType::AppendOnly; rows_.len()],
            &rows(rows_),
            codec.as_ref(),
        )
        .unwrap(),
    )
}

fn decode(batches: &[Bytes]) -> Vec<(i64, String)> {
    let codec = codec(LogFormat::Arrow, Compression::None);
    let arrow = Arc::new(arrow_schema::Schema::from(schema(false).fields()));
    let mut out = Vec::new();
    for bytes in batches {
        let batch = Batch::parse(bytes.clone()).unwrap();
        let records = batch.records(codec.as_ref(), arrow.clone(), None).unwrap();
        let k = records
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v = records
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..records.batch.num_rows() {
            out.push((k.value(i), v.value(i).to_owned()));
        }
    }
    out
}

fn key(k: i64) -> Vec<u8> {
    let schema = schema(true);
    let encoder = mink_record::KeyEncoder::new(
        schema.fields(),
        &["k".to_owned()],
        mink_table::Bucketing::Native,
    )
    .unwrap();
    encoder
        .bind(&rows(&[(k, "")]))
        .unwrap()
        .encode_vec(0)
        .unwrap()
}

fn value_of(service: &Service, bucket: Bucket, k: i64) -> Option<String> {
    let value = service.lookup(bucket, &key(k)).unwrap()?;
    let hosted = service.node().registry().get(bucket).unwrap();
    let batch = hosted.kv().unwrap().to_batch([value]).unwrap();
    let v = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    Some(v.value(0).to_owned())
}

fn buckets(table: Id, count: u32) -> Vec<Bucket> {
    (0..count)
        .map(|b| Bucket::new(table, BucketId(b)))
        .collect()
}

#[tokio::test]
async fn log_buckets_open_serve_and_route() {
    let cluster = Cluster::new(&[1, 2]).await;
    let table = cluster.create("logs", log_table(2)).await;
    let [b0, b1] = buckets(table, 2)[..] else {
        unreachable!()
    };
    assert_ne!(cluster.leader_of(b0), cluster.leader_of(b1), "round-robin");

    let node1 = cluster.node(1, 1).await;
    let node2 = cluster.node(2, 1).await;
    let report = node1.sync().await;
    assert_eq!(report.opened.len(), 1, "{report:?}");
    node2.sync().await;
    assert!(node1.sync().await.is_quiet());

    let (mine, theirs) = if cluster.leader_of(b0) == 1 {
        (b0, b1)
    } else {
        (b1, b0)
    };
    let service = Service::new(node1.clone());
    assert_eq!(service.owner(mine), Owner::Local);
    assert_eq!(
        service.owner(theirs),
        Owner::Remote {
            node_id: 2,
            address: "node2:9000".into()
        }
    );
    let info = service.table(&path("logs")).unwrap();
    assert_eq!(info.table_id, table);
    assert_eq!(info.leaders.len(), 2);

    let appended = service
        .append(mine, batch(&[(1, "a"), (2, "b")]))
        .await
        .unwrap();
    assert_eq!((appended.first_offset, appended.last_offset), (0, 1));
    service.append(mine, batch(&[(3, "c")])).await.unwrap();

    let fetched = service.fetch(mine, 0, usize::MAX, None).await.unwrap();
    assert_eq!(
        decode(&fetched.batches),
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]
    );
    assert_eq!(fetched.high_watermark, 3);
    assert_eq!(
        service
            .list_offset(mine, OffsetSpec::Earliest)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        service.list_offset(mine, OffsetSpec::Latest).await.unwrap(),
        3
    );

    assert!(matches!(
        service.append(theirs, batch(&[(9, "z")])).await,
        Err(Error::NotLeader {
            leader: Some(2),
            ..
        })
    ));
    assert!(matches!(
        service
            .put(mine, Put::upsert(SchemaId(0), rows(&[(1, "a")])))
            .await,
        Err(Error::NotKvTable(_))
    ));
    assert!(matches!(
        service
            .append(Bucket::new(Id(999), BucketId(0)), batch(&[(1, "a")]))
            .await,
        Err(Error::BucketNotExist(_))
    ));

    node1.shutdown().await;
    node2.shutdown().await;
    assert_eq!(cluster.stream_state(mine).0, StreamState::Closed);
}

#[tokio::test]
async fn kv_bucket_snapshots_and_restores_on_restart() {
    let cluster = Cluster::new(&[1]).await;
    let table = cluster.create("users", pk_table(1)).await;
    let bucket = Bucket::new(table, BucketId(0));

    let node = cluster.node(1, 1).await;
    node.sync().await;
    let service = Service::new(node.clone());
    service
        .put(
            bucket,
            Put::upsert(SchemaId(0), rows(&[(1, "ann"), (2, "bob")])),
        )
        .await
        .unwrap();
    assert_eq!(value_of(&service, bucket, 1).as_deref(), Some("ann"));
    assert_eq!(value_of(&service, bucket, 3), None);
    assert!(matches!(
        service.append(bucket, batch(&[(1, "a")])).await,
        Err(Error::NotLogTable(_))
    ));

    let hosted = node.registry().get(bucket).unwrap();
    let first = node.snapshot(hosted.clone()).await.unwrap();
    assert!(first.is_some());
    assert_eq!(node.snapshot(hosted.clone()).await.unwrap(), None);
    let committed = cluster
        .views
        .load()
        .state
        .catalog
        .latest_kv_snapshot(bucket)
        .cloned();
    assert_eq!(committed.as_ref().map(|s| s.snapshot_id), first);
    assert_eq!(committed.as_ref().map(|s| s.log_offset), Some(2));
    assert_eq!(committed.as_ref().map(|s| s.row_count), Some(2));

    service
        .put(
            bucket,
            Put::upsert(SchemaId(0), rows(&[(2, "bobby"), (3, "cyd")])),
        )
        .await
        .unwrap();
    node.shutdown().await;

    let node = cluster.node(1, 2).await;
    let report = node.sync().await;
    assert_eq!(report.opened, vec![bucket], "{report:?}");
    let service = Service::new(node.clone());
    assert_eq!(value_of(&service, bucket, 1).as_deref(), Some("ann"));
    assert_eq!(value_of(&service, bucket, 2).as_deref(), Some("bobby"));
    assert_eq!(value_of(&service, bucket, 3).as_deref(), Some("cyd"));
    let hosted = node.registry().get(bucket).unwrap();
    assert_eq!(hosted.kv().unwrap().row_count().await, 3);
    assert_eq!(hosted.leader_epoch, 1, "a restart is a new leader term");
    assert_eq!(cluster.stream_state(bucket), (StreamState::Opened, 1, 1));
    let second = node.snapshot(hosted).await.unwrap().unwrap();
    assert!(second > first.unwrap());
    node.shutdown().await;
}

#[tokio::test]
async fn dead_leader_is_taken_over_with_its_wal_tail() {
    let cluster = Cluster::new(&[1, 2]).await;
    let table = cluster.create("events", log_table(2)).await;
    let bucket = buckets(table, 2)
        .into_iter()
        .find(|b| cluster.leader_of(*b) == 2)
        .unwrap();

    let node1 = cluster.node(1, 1).await;
    let node2 = cluster.node(2, 1).await;
    node1.sync().await;
    node2.sync().await;
    let on_two = Service::new(node2.clone());
    on_two
        .append(bucket, batch(&[(1, "a"), (2, "b")]))
        .await
        .unwrap();
    on_two.append(bucket, batch(&[(3, "c")])).await.unwrap();
    assert_eq!(cluster.stream_state(bucket), (StreamState::Opened, 2, 0));

    cluster.membership.remove(2);
    let report = cluster.coordinator.reconcile().await.unwrap();
    assert_eq!(report.buckets_reled, 1);
    assert_eq!(cluster.leader_of(bucket), 1);

    let report = node1.sync().await;
    assert_eq!(report.opened, vec![bucket], "{report:?}");
    let view = cluster.views.load();
    assert_eq!(view.state.nodes[&2].epoch, 2, "dead node fenced");
    assert_eq!(cluster.stream_state(bucket), (StreamState::Opened, 1, 1));

    let on_one = Service::new(node1.clone());
    let fetched = on_one.fetch(bucket, 0, usize::MAX, None).await.unwrap();
    assert_eq!(
        decode(&fetched.batches),
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())],
        "the tail node 2 never uploaded came through its WAL"
    );
    let appended = on_one.append(bucket, batch(&[(4, "d")])).await.unwrap();
    assert_eq!(appended.first_offset, 3);

    node2.sync().await;
    assert!(node2.registry().is_empty());
    assert!(matches!(
        on_two.append(bucket, batch(&[(5, "e")])).await,
        Err(Error::NotLeader {
            leader: Some(1),
            ..
        })
    ));
    let fetched = on_one.fetch(bucket, 3, usize::MAX, None).await.unwrap();
    assert_eq!(decode(&fetched.batches), vec![(4, "d".into())]);
    node1.shutdown().await;
}

#[tokio::test]
async fn live_leader_is_waited_for_not_fenced() {
    let cluster = Cluster::new(&[1, 2]).await;
    let table = cluster.create("moves", log_table(1)).await;
    let bucket = Bucket::new(table, BucketId(0));
    let node1 = cluster.node(1, 1).await;
    let node2 = cluster.node(2, 1).await;
    node1.sync().await;
    node2.sync().await;
    let old = cluster.leader_of(bucket);
    let (from, to) = if old == 1 {
        (node1.clone(), node2.clone())
    } else {
        (node2.clone(), node1.clone())
    };
    Service::new(from.clone())
        .append(bucket, batch(&[(1, "a")]))
        .await
        .unwrap();

    cluster.lead(bucket, to.node_id()).await;
    let report = to.sync().await;
    assert!(report.opened.is_empty());
    assert!(
        matches!(
            report.failed.as_slice(),
            [(b, Error::HeldBy { node_id, .. })] if *b == bucket && *node_id == from.node_id()
        ),
        "{report:?}"
    );
    assert_eq!(cluster.views.load().state.nodes[&from.node_id()].epoch, 1);

    let report = from.sync().await;
    assert_eq!(report.closed, vec![bucket]);
    let report = to.sync().await;
    assert_eq!(report.opened, vec![bucket]);
    let fetched = Service::new(to.clone())
        .fetch(bucket, 0, usize::MAX, None)
        .await
        .unwrap();
    assert_eq!(decode(&fetched.batches), vec![(1, "a".into())]);
    assert!(matches!(
        Service::new(from.clone())
            .append(bucket, batch(&[(2, "b")]))
            .await,
        Err(Error::NotLeader { .. })
    ));
    node1.shutdown().await;
    node2.shutdown().await;
}

#[tokio::test]
async fn dropped_table_destroys_its_buckets() {
    let cluster = Cluster::new(&[1]).await;
    let table = cluster.create("gone", log_table(2)).await;
    let node = cluster.node(1, 1).await;
    assert_eq!(node.sync().await.opened.len(), 2);
    let stream_ids: BTreeSet<u64> = cluster
        .views
        .load()
        .state
        .catalog
        .buckets_of(table, None)
        .map(|(_, row)| row.stream_id)
        .collect();

    cluster
        .coordinator
        .drop_table(&path("gone"), false)
        .await
        .unwrap();
    let report = node.sync().await;
    assert_eq!(report.destroyed.len(), 2);
    assert!(node.registry().is_empty());
    let view = cluster.views.load();
    for id in stream_ids {
        assert!(view.state.streams.get(&id).is_none(), "stream {id} deleted");
    }
    node.shutdown().await;
}

#[tokio::test]
async fn spawned_node_follows_the_view_and_closes_on_shutdown() {
    let cluster = Cluster::new(&[1]).await;
    let node = cluster.node(1, 1).await;
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let task = node.clone().spawn(shutdown);

    let table = cluster.create("live", log_table(1)).await;
    let bucket = Bucket::new(table, BucketId(0));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !node.registry().contains(bucket) {
        assert!(Instant::now() < deadline, "never opened");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let service = Service::new(node.clone());
    service.append(bucket, batch(&[(1, "a")])).await.unwrap();

    stop.send(true).unwrap();
    task.await.unwrap();
    assert!(node.registry().is_empty());
    assert_eq!(cluster.stream_state(bucket), (StreamState::Closed, 1, 0));
    assert!(matches!(
        service.append(bucket, batch(&[(2, "b")])).await,
        Err(Error::Unavailable(_))
    ));
}

const TTL: Duration = Duration::from_secs(60 * 60);

fn with_ttl(descriptor: Descriptor, lake: bool) -> Descriptor {
    let options = Options {
        log_ttl: Some(TTL),
        lake: lake.then_some(LakeFormat::Iceberg),
        ..descriptor.options().clone()
    };
    Descriptor::builder(descriptor.schema().clone())
        .bucket_count(1)
        .options(options)
        .build()
        .unwrap()
}

fn retention_of(results: &[(Bucket, Result<Retention, Error>)], bucket: Bucket) -> Retention {
    *results
        .iter()
        .find(|(b, _)| *b == bucket)
        .map(|(_, r)| r.as_ref().unwrap())
        .unwrap()
}

#[tokio::test]
async fn log_ttl_trims_expired_records_by_commit_time() {
    let cluster = Cluster::new(&[1]).await;
    let table = cluster
        .create("events", with_ttl(log_table(1), false))
        .await;
    let forever = {
        let options = Options {
            log_ttl: None,
            ..Options::default()
        };
        Descriptor::builder(schema(false))
            .bucket_count(1)
            .options(options)
            .build()
            .unwrap()
    };
    let kept = cluster.create("audit", forever).await;
    let bucket = Bucket::new(table, BucketId(0));
    let kept_bucket = Bucket::new(kept, BucketId(0));
    let node = cluster.node(1, 1).await;
    node.sync().await;
    let service = Service::new(node.clone());

    service.append(bucket, batch(&[(1, "a")])).await.unwrap();
    service
        .append(kept_bucket, batch(&[(1, "a")]))
        .await
        .unwrap();
    cluster.clock.advance(TTL / 2);
    service.append(bucket, batch(&[(2, "b")])).await.unwrap();
    service
        .append(kept_bucket, batch(&[(2, "b")]))
        .await
        .unwrap();

    let first_committed = cluster.clock.millis() - (TTL / 2).as_millis() as i64;
    let results = node.retain_all().await;
    assert_eq!(retention_of(&results, bucket), Retention::Kept);
    let hosted = node.registry().get(bucket).unwrap();
    assert_eq!(
        *hosted.retention.lock().unwrap(),
        Some(RetentionFrontier {
            offset: 0,
            timestamp: Some(first_committed)
        })
    );

    cluster.clock.advance(TTL / 2 + Duration::from_millis(1));
    cluster.committed(bucket, 2).await;
    let results = node.retain_all().await;
    assert_eq!(
        retention_of(&results, bucket),
        Retention::Trimmed {
            new_start: 1,
            expired_to: 1
        }
    );
    assert_eq!(
        retention_of(&results, kept_bucket),
        Retention::Kept,
        "ttl -1 keeps everything"
    );
    assert_eq!(hosted.log.log_start_offset(), 1);
    assert_eq!(hosted.log.high_watermark(), 2);
    assert_eq!(
        *hosted.retention.lock().unwrap(),
        Some(RetentionFrontier {
            offset: 1,
            timestamp: Some(first_committed + (TTL / 2).as_millis() as i64)
        })
    );
    let fetched = service.fetch(bucket, 1, usize::MAX, None).await.unwrap();
    assert_eq!(decode(&fetched.batches), vec![(2, "b".to_owned())]);
    assert!(matches!(
        service.fetch(bucket, 0, usize::MAX, None).await,
        Err(Error::Log(mink_log::Error::OutOfRange { .. }))
    ));
    assert_eq!(
        service
            .list_offset(bucket, OffsetSpec::Earliest)
            .await
            .unwrap(),
        1
    );

    cluster.clock.advance(TTL);
    let results = node.retain_all().await;
    assert_eq!(
        retention_of(&results, bucket),
        Retention::Trimmed {
            new_start: 2,
            expired_to: 2
        }
    );
    assert_eq!(retention_of(&results, kept_bucket), Retention::Kept);
    assert_eq!(
        node.registry()
            .get(kept_bucket)
            .unwrap()
            .log
            .log_start_offset(),
        0
    );
    node.shutdown().await;
}

#[tokio::test]
async fn log_ttl_never_trims_past_the_lake() {
    let cluster = Cluster::new(&[1]).await;
    let table = cluster.create("tiered", with_ttl(log_table(1), true)).await;
    let bucket = Bucket::new(table, BucketId(0));
    let node = cluster.node(1, 1).await;
    node.sync().await;
    let service = Service::new(node.clone());
    service.append(bucket, batch(&[(1, "a")])).await.unwrap();
    service.append(bucket, batch(&[(2, "b")])).await.unwrap();
    service.append(bucket, batch(&[(3, "c")])).await.unwrap();
    cluster.clock.advance(TTL * 2);
    cluster.committed(bucket, 3).await;

    let results = node.retain_all().await;
    assert_eq!(
        retention_of(&results, bucket),
        Retention::Held {
            expired_to: 3,
            held_at: 0
        }
    );
    assert_eq!(
        node.registry().get(bucket).unwrap().log.log_start_offset(),
        0
    );

    cluster
        .coordinator
        .commit_lake_snapshot(
            table,
            LakeSnapshotRow {
                snapshot_id: 1,
                bucket_log_end_offset: [(bucket, 2)].into_iter().collect(),
            },
        )
        .await
        .unwrap();
    let results = node.retain_all().await;
    assert_eq!(
        retention_of(&results, bucket),
        Retention::Trimmed {
            new_start: 2,
            expired_to: 3
        }
    );
    assert_eq!(
        node.registry().get(bucket).unwrap().log.log_start_offset(),
        2
    );
    node.shutdown().await;
}

#[tokio::test]
async fn log_ttl_never_trims_past_the_kv_snapshot() {
    let cluster = Cluster::new(&[1]).await;
    let table = cluster.create("users", with_ttl(pk_table(1), false)).await;
    let bucket = Bucket::new(table, BucketId(0));
    let node = cluster.node(1, 1).await;
    node.sync().await;
    let service = Service::new(node.clone());
    service
        .put(bucket, Put::upsert(SchemaId(0), rows(&[(1, "ann")])))
        .await
        .unwrap();
    service
        .put(bucket, Put::upsert(SchemaId(0), rows(&[(2, "bob")])))
        .await
        .unwrap();
    cluster.clock.advance(TTL * 2);
    cluster.committed(bucket, 2).await;

    let results = node.retain_all().await;
    assert_eq!(
        retention_of(&results, bucket),
        Retention::Held {
            expired_to: 2,
            held_at: 0
        }
    );

    let hosted = node.registry().get(bucket).unwrap();
    node.snapshot(hosted.clone()).await.unwrap().unwrap();
    service
        .put(bucket, Put::upsert(SchemaId(0), rows(&[(3, "cyd")])))
        .await
        .unwrap();
    cluster.clock.advance(TTL * 2);
    cluster.committed(bucket, 3).await;
    let results = node.retain_all().await;
    assert_eq!(
        retention_of(&results, bucket),
        Retention::Trimmed {
            new_start: 2,
            expired_to: 3
        }
    );
    assert_eq!(hosted.log.log_start_offset(), 2);
    node.shutdown().await;

    let node = cluster.node(1, 2).await;
    let report = node.sync().await;
    assert_eq!(report.opened, vec![bucket], "{report:?}");
    let service = Service::new(node.clone());
    assert_eq!(value_of(&service, bucket, 1).as_deref(), Some("ann"));
    assert_eq!(value_of(&service, bucket, 2).as_deref(), Some("bob"));
    assert_eq!(value_of(&service, bucket, 3).as_deref(), Some("cyd"));
    node.shutdown().await;
}
