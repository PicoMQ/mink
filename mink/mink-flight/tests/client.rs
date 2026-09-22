//! The client library against a live Flight server: catalog, writes, reads, lookups and redirects.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use futures::StreamExt;
use mink_client::{Cluster, Error, proto};
use mink_common::ManualClock;
use mink_coordinator::{Coordinator, NoopCleaner, StaticMembership};
use mink_flight::{Flight, Server};
use mink_kv::MemoryEngine;
use mink_metadata::{Handle, LocalSink};
use mink_server::{Config, Failover, Node, Service};
use mink_table::{
    Bucket, BucketId, Change, Column, Descriptor, PartitionSpec, Path, PrimaryKey, Schema, SchemaId,
};
use mink_types::DataType;
use object_store::memory::InMemory;
use s3stream::{
    Client, MemoryObjectStorage, ObjectStorageTrait, ObjectWalConfig, ObjectWalService,
    S3StreamBuilder,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tonic::Code;

const CLUSTER: &str = "mink-client-test";
const NODE: i32 = 1;

struct Harness {
    server: Server,
    node: Arc<Node>,
    coordinator: Arc<Coordinator>,
    stop: watch::Sender<bool>,
    node_task: JoinHandle<()>,
    cluster: Cluster,
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn start() -> Self {
        let (sink, views) = LocalSink::new();
        let sink = Arc::new(sink);
        let membership = Arc::new(StaticMembership::new([NODE]));
        let clock = Arc::new(ManualClock::new(1_700_000_000_000));
        let data: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
        let wal: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
        let dir = tempfile::tempdir().unwrap();

        let handle = Handle::new(NODE, 1, sink.clone(), views.clone());
        let mut wal_config = ObjectWalConfig::defaults();
        wal_config.cluster_id = CLUSTER.into();
        wal_config.node_id = NODE as u32;
        wal_config.epoch = 1;
        let engine = S3StreamBuilder::new(s3stream::Config::default())
            .object_storage(data)
            .write_ahead_log(Arc::new(ObjectWalService::new(wal.clone(), wal_config)))
            .stream_manager(Arc::new(handle.stream_manager()))
            .object_manager(Arc::new(handle.object_manager()))
            .kv_client(Arc::new(handle.kv_client()))
            .failover_factory(Arc::new(Failover::new(handle.clone(), CLUSTER, wal)))
            .build()
            .await
            .unwrap();
        let engine: Arc<dyn Client> = Arc::new(engine);
        let mut config = Config::new(CLUSTER, "0@mem://wal", dir.path());
        config.sync_interval = Duration::from_millis(50);
        let node = Arc::new(Node::new(
            handle.clone(),
            engine,
            Arc::new(InMemory::new()),
            Arc::new(MemoryEngine),
            membership.clone(),
            clock.clone(),
            config,
        ));
        let (stop, shutdown) = watch::channel(false);
        let node_task = node.clone().spawn(shutdown);

        let flight_placeholder = Flight::new(
            Service::new(node.clone()),
            Arc::new(Coordinator::new(
                NODE,
                "grpc://127.0.0.1:0",
                sink.clone(),
                views.clone(),
                membership.clone(),
                Arc::new(NoopCleaner),
                clock.clone(),
                mink_coordinator::Config::default(),
            )),
            mink_flight::Config::default(),
        );
        let probe = mink_flight::serve("127.0.0.1:0".parse().unwrap(), flight_placeholder)
            .await
            .unwrap();
        let addr = probe.local_addr();
        probe.shutdown().await;
        let advertise = format!("grpc://{addr}");
        handle
            .register(&advertise, 1, Default::default())
            .await
            .unwrap();

        let coordinator = Arc::new(Coordinator::new(
            NODE,
            advertise.clone(),
            sink,
            views,
            membership,
            Arc::new(NoopCleaner),
            clock,
            mink_coordinator::Config::default(),
        ));
        coordinator.become_leader().await.unwrap();
        let flight = Flight::new(
            Service::new(node.clone()),
            coordinator.clone(),
            mink_flight::Config::default(),
        );
        let server = mink_flight::serve(addr, flight).await.unwrap();
        let cluster = Cluster::connect(&advertise).unwrap();
        Harness {
            server,
            node,
            coordinator,
            stop,
            node_task,
            cluster,
            _dir: dir,
        }
    }

    async fn stop(self) {
        self.server.shutdown().await;
        let _ = self.stop.send(true);
        let _ = self.node_task.await;
    }

    async fn create(&self, path: &str, descriptor: Descriptor) -> mink_client::Table {
        let path: Path = path.parse().unwrap();
        let table_id = self
            .cluster
            .admin()
            .create_table(&path, &descriptor, false)
            .await
            .unwrap()
            .unwrap();
        let table = self.cluster.table(&path).await.unwrap();
        if !descriptor.is_partitioned() {
            for bucket in 0..descriptor.bucket_count().unwrap_or(1) {
                self.wait_led(Bucket::new(table_id, BucketId(bucket))).await;
            }
        }
        table
    }

    async fn wait_led(&self, bucket: Bucket) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.node.registry().contains(bucket) {
            assert!(Instant::now() < deadline, "bucket never opened");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn schema(primary_key: bool) -> Schema {
    let mut builder = Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("region", DataType::string().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap());
    if primary_key {
        builder = builder.primary_key(PrimaryKey::new(vec!["k".into(), "region".into()]).unwrap());
    }
    builder.build().unwrap()
}

fn descriptor(primary_key: bool, buckets: u32, partitioned: bool) -> Descriptor {
    let mut builder = Descriptor::builder(schema(primary_key)).bucket_count(buckets);
    if partitioned {
        builder = builder.partitioned_by(["region"]);
    }
    builder.build().unwrap()
}

fn rows(rows: &[(i64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(schema(false).fields())),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn keys(keys: &[(i64, &str)]) -> RecordBatch {
    let fields = schema(true).fields().project(&[0, 1]).unwrap();
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(&fields)),
        vec![
            Arc::new(Int64Array::from(
                keys.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                keys.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn triples(batch: &RecordBatch) -> Vec<(i64, String, String)> {
    let k = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let region = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let v = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| {
            (
                k.value(i),
                region.value(i).to_owned(),
                v.value(i).to_owned(),
            )
        })
        .collect()
}

#[tokio::test]
async fn admin_covers_ddl_partitions_and_producer_offsets() {
    let h = Harness::start().await;
    let admin = h.cluster.admin();

    let metadata = admin.metadata().await.unwrap();
    assert_eq!(metadata.nodes.len(), 1);
    assert_eq!(metadata.coordinator.unwrap().node_id, NODE);

    admin
        .create_database("db", Some("c"), BTreeMap::new(), false)
        .await
        .unwrap();
    assert!(admin.database_exists("db").await.unwrap());
    assert_eq!(admin.list_databases().await.unwrap(), vec!["db"]);
    let exists = admin
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap_err();
    assert!(matches!(exists, Error::Status(s) if s.code() == Code::AlreadyExists));
    admin
        .create_database("db", None, BTreeMap::new(), true)
        .await
        .unwrap();

    let path: Path = "db.parted".parse().unwrap();
    let table_id = admin
        .create_table(&path, &descriptor(false, 2, true), false)
        .await
        .unwrap()
        .unwrap();
    assert!(admin.table_exists(&path).await.unwrap());
    assert_eq!(admin.list_tables("db").await.unwrap(), vec!["parted"]);
    let info = admin.get_table(&path).await.unwrap();
    assert_eq!(info.table_id, table_id);
    assert!(info.buckets.is_empty(), "no partitions, no buckets");

    let spec = PartitionSpec::new(vec![("region".into(), "us".parse().unwrap())]).unwrap();
    let partition_id = admin
        .create_partition(&path, &spec, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        admin.create_partition(&path, &spec, true).await.unwrap(),
        None
    );
    let partitions = admin.list_partitions(&path).await.unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].partition_id, partition_id);
    assert_eq!(partitions[0].name.as_str(), "us");
    assert_eq!(admin.get_table(&path).await.unwrap().buckets.len(), 2);

    let schema_id = admin
        .alter_table(&path, vec![Change::add_column("w", DataType::int())], false)
        .await
        .unwrap();
    assert_eq!(schema_id, Some(SchemaId(1)));
    assert_eq!(admin.get_table(&path).await.unwrap().schemas.len(), 2);

    let bucket = Bucket::partitioned(table_id, partition_id, BucketId(0));
    h.wait_led(bucket).await;
    assert_eq!(
        admin
            .list_offset(&path, bucket, proto::OffsetSpec::Latest)
            .await
            .unwrap(),
        0
    );
    assert!(admin.latest_kv_snapshot(bucket).await.unwrap().is_none());
    assert!(admin.lake_snapshot(&path).await.unwrap().is_none());

    let offsets: BTreeMap<Bucket, i64> = [(bucket, 0)].into();
    assert!(
        admin
            .register_producer_offsets("job", &offsets, Some(Duration::from_secs(60)))
            .await
            .unwrap()
    );
    assert!(
        !admin
            .register_producer_offsets("job", &offsets, None)
            .await
            .unwrap()
    );
    let registered = admin.producer_offsets("job").await.unwrap().unwrap();
    assert_eq!(registered.offsets[0].bucket, bucket);
    admin.delete_producer_offsets("job").await.unwrap();
    assert!(admin.producer_offsets("job").await.unwrap().is_none());

    admin.drop_partition(&path, &spec, false).await.unwrap();
    admin.drop_table(&path, false).await.unwrap();
    assert!(!admin.table_exists(&path).await.unwrap());
    admin.drop_database("db", false, false).await.unwrap();
    assert!(admin.list_databases().await.unwrap().is_empty());
    h.stop().await;
}

#[tokio::test]
async fn append_writer_routes_by_bucket_key_and_scans_read_back() {
    let h = Harness::start().await;
    h.cluster
        .admin()
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let keyed = Descriptor::builder(schema(false))
        .bucket_keys(["k"])
        .bucket_count(3)
        .build()
        .unwrap();
    let table = h.create("db.log", keyed).await;
    let mut writer = table.append_writer().await.unwrap();
    assert!(writer.writer_id() >= 0);

    let batch = rows(&[(1, "us", "a"), (2, "eu", "b"), (3, "us", "d")]);
    let routed = writer.append(&batch).await.unwrap();
    let total: usize = routed.iter().map(|r| r.rows).sum();
    assert_eq!(total, 3);
    assert!(routed.iter().all(|r| r.bucket.bucket().0 < 3));
    let once = writer.append(&rows(&[(1, "us", "c")])).await.unwrap();
    let again = writer.append(&rows(&[(1, "us", "e")])).await.unwrap();
    assert_eq!(once.len(), 1);
    assert_eq!(again[0].bucket, once[0].bucket);
    assert_eq!(again[0].first_offset, once[0].last_offset + 1);

    let mut seen = Vec::new();
    for bucket in table.buckets() {
        let (start, end) = table.offsets(bucket).await.unwrap();
        let mut scan = table.scan(bucket, start, end, None).await.unwrap();
        while let Some(batch) = scan.next().await {
            seen.extend(triples(&batch.unwrap().rows));
        }
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            (1, "us".into(), "a".into()),
            (1, "us".into(), "c".into()),
            (1, "us".into(), "e".into()),
            (2, "eu".into(), "b".into()),
            (3, "us".into(), "d".into()),
        ]
    );

    let pk = h.create("db.pk", descriptor(true, 1, false)).await;
    assert!(matches!(
        pk.append_writer().await.err(),
        Some(Error::PrimaryKey(_))
    ));
    h.stop().await;
}

#[tokio::test]
async fn upsert_lookup_and_prefix_lookup_on_a_partitioned_pk_table() {
    let h = Harness::start().await;
    h.cluster
        .admin()
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table = h.create("db.pk", descriptor(true, 2, true)).await;
    let mut writer = table.upsert_writer().await.unwrap();

    let routed = writer
        .upsert(&rows(&[(1, "us", "a"), (2, "eu", "b"), (3, "us", "c")]))
        .await
        .unwrap();
    let partitions: Vec<String> = routed
        .iter()
        .filter_map(|r| r.partition.as_ref().map(|p| p.to_string()))
        .collect();
    assert!(partitions.contains(&"us".to_string()));
    assert!(partitions.contains(&"eu".to_string()));
    assert_eq!(
        h.cluster
            .admin()
            .list_partitions(table.path())
            .await
            .unwrap()
            .len(),
        2
    );

    let lookuper = table.lookuper().unwrap();
    assert_eq!(
        lookuper.key_columns().collect::<Vec<_>>(),
        vec!["k", "region"]
    );
    assert_eq!(
        lookuper.prefix_columns().collect::<Vec<_>>(),
        vec!["k", "region"],
        "partition key + bucket key, in schema order"
    );
    let found = lookuper
        .lookup(&keys(&[(1, "us"), (2, "eu"), (9, "us"), (1, "eu")]))
        .await
        .unwrap();
    assert_eq!(
        triples(found[0].as_ref().unwrap()),
        vec![(1, "us".into(), "a".into())]
    );
    assert_eq!(
        triples(found[1].as_ref().unwrap()),
        vec![(2, "eu".into(), "b".into())]
    );
    assert!(found[2].is_none(), "unknown key");
    assert!(found[3].is_none(), "right key, wrong partition");

    writer.upsert(&rows(&[(1, "us", "A")])).await.unwrap();
    writer.delete(&rows(&[(2, "eu", "")])).await.unwrap();
    let one = lookuper
        .lookup_one(&keys(&[(1, "us")]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(triples(&one), vec![(1, "us".into(), "A".into())]);
    assert!(
        lookuper
            .lookup_one(&keys(&[(2, "eu")]))
            .await
            .unwrap()
            .is_none()
    );

    let prefixed = lookuper
        .prefix_lookup(&keys(&[(3, "us"), (3, "eu")]))
        .await
        .unwrap();
    assert_eq!(
        triples(prefixed[0].as_ref().unwrap()),
        vec![(3, "us".into(), "c".into())]
    );
    assert!(prefixed[1].is_none());

    assert!(table.partial_update_writer(vec![2]).await.is_err());
    let mut partial = table.partial_update_writer(vec![0, 1, 2]).await.unwrap();
    partial.upsert(&rows(&[(3, "us", "C")])).await.unwrap();
    let three = lookuper
        .lookup_one(&keys(&[(3, "us")]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(triples(&three), vec![(3, "us".into(), "C".into())]);

    assert!(matches!(
        lookuper.lookup(&rows(&[(1, "us", "x")])).await.unwrap_err(),
        Error::Protocol(_)
    ));
    let mut total = 0;
    for bucket in h
        .cluster
        .admin()
        .get_table(table.path())
        .await
        .unwrap()
        .buckets
    {
        let snapshot = table.snapshot(bucket.bucket, None).await.unwrap();
        let batches: Vec<RecordBatch> = snapshot.batches.map(|b| b.unwrap()).collect().await;
        total += batches.iter().map(|b| b.num_rows()).sum::<usize>();
    }
    assert_eq!(total, 2, "(1,us) and (3,us) remain");
    h.stop().await;
}

#[tokio::test]
async fn tail_follows_the_log_and_tracks_the_resume_offset() {
    let h = Harness::start().await;
    h.cluster
        .admin()
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table = h.create("db.log", descriptor(false, 1, false)).await;
    let bucket = table.buckets().next().unwrap();
    let mut writer = table.append_writer().await.unwrap();
    writer.append(&rows(&[(1, "us", "a")])).await.unwrap();

    let mut tail = table.tail(bucket, 0, None).await.unwrap();
    assert_eq!(tail.next_offset(), 0);
    let first = tail.next().await.unwrap().unwrap();
    assert_eq!(triples(&first.rows), vec![(1, "us".into(), "a".into())]);
    assert_eq!(tail.next_offset(), 1);

    writer
        .append(&rows(&[(2, "eu", "b"), (3, "us", "c")]))
        .await
        .unwrap();
    let second = loop {
        let batch = tail.next().await.unwrap().unwrap();
        if batch.rows.num_rows() > 0 {
            break batch;
        }
        assert!(batch.meta.high_watermark >= 1);
    };
    assert_eq!(
        triples(&second.rows),
        vec![(2, "eu".into(), "b".into()), (3, "us".into(), "c".into())]
    );
    assert_eq!(tail.next_offset(), 3);
    assert_eq!(second.meta.high_watermark, 3);
    drop(tail);
    h.stop().await;
}

#[tokio::test]
async fn coordinator_redirects_are_followed_and_bounded() {
    let h = Harness::start().await;
    let admin = h.cluster.admin();
    admin
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();

    h.coordinator.resign();
    let err = admin
        .create_database("db2", None, BTreeMap::new(), false)
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Redirects { attempts, .. } if attempts == mink_client::MAX_REDIRECTS),
        "{err}"
    );
    assert_eq!(admin.list_databases().await.unwrap(), vec!["db"]);

    h.coordinator.become_leader().await.unwrap();
    admin
        .create_database("db2", None, BTreeMap::new(), false)
        .await
        .unwrap();
    assert_eq!(admin.list_databases().await.unwrap(), vec!["db", "db2"]);
    h.stop().await;
}
