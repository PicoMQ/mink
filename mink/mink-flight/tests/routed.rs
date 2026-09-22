//! Table-addressed writes routed across several nodes, with partition creation and forwarding.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData, FlightDescriptor, Ticket};
use arrow_ipc::reader::StreamReader;
use futures::{StreamExt, TryStreamExt};
use mink_common::{ManualClock, net};
use mink_coordinator::{Coordinator, NoopCleaner, StaticMembership};
use mink_flight::proto::{self, action};
use mink_flight::{Flight, Server};
use mink_kv::MemoryEngine;
use mink_metadata::{Handle, LocalSink, ViewPublisher};
use mink_record::ChangeType;
use mink_server::{Config, Failover, Node, Service};
use mink_table::{
    Bucket, BucketId, Column, Descriptor, PartitionName, Path, PrimaryKey, Schema, SchemaId,
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
use tonic::transport::Channel;
use tonic::{Code, Status};

const CLUSTER: &str = "mink-flight-routed";

struct Member {
    id: i32,
    client: FlightServiceClient<Channel>,
    server: Server,
    node: Arc<Node>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct Cluster {
    members: Vec<Member>,
    coordinator: Arc<Coordinator>,
    views: Arc<ViewPublisher>,
    membership: Arc<StaticMembership>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn start(ids: &[i32]) -> Self {
        let (sink, views) = LocalSink::new();
        let sink = Arc::new(sink);
        let membership = Arc::new(StaticMembership::new(ids.iter().copied()));
        let clock = Arc::new(ManualClock::new(1_700_000_000_000));
        let data: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
        let wal: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
        let snapshots = Arc::new(InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let ports: BTreeMap<i32, u16> = ids.iter().map(|id| (*id, net::free_port())).collect();
        let address = |id: i32| format!("grpc://127.0.0.1:{}", ports[&id]);

        let mut members = Vec::new();
        let mut leader = None;
        for &id in ids {
            let handle = Handle::new(id, 1, sink.clone(), views.clone());
            handle
                .register(&address(id), 1, Default::default())
                .await
                .unwrap();
            let mut wal_config = ObjectWalConfig::defaults();
            wal_config.cluster_id = CLUSTER.into();
            wal_config.node_id = id as u32;
            wal_config.epoch = 1;
            let engine = S3StreamBuilder::new(s3stream::Config::default())
                .object_storage(data.clone())
                .write_ahead_log(Arc::new(ObjectWalService::new(wal.clone(), wal_config)))
                .stream_manager(Arc::new(handle.stream_manager()))
                .object_manager(Arc::new(handle.object_manager()))
                .kv_client(Arc::new(handle.kv_client()))
                .failover_factory(Arc::new(Failover::new(
                    handle.clone(),
                    CLUSTER,
                    wal.clone(),
                )))
                .build()
                .await
                .unwrap();
            let engine: Arc<dyn Client> = Arc::new(engine);
            let mut config = Config::new(CLUSTER, "0@mem://wal", dir.path().join(format!("n{id}")));
            config.sync_interval = Duration::from_millis(50);
            let node = Arc::new(Node::new(
                handle,
                engine,
                snapshots.clone(),
                Arc::new(MemoryEngine),
                membership.clone(),
                clock.clone(),
                config,
            ));
            let (stop, shutdown) = watch::channel(false);
            let task = node.clone().spawn(shutdown);

            let coordinator = Arc::new(Coordinator::new(
                id,
                address(id),
                sink.clone(),
                views.clone(),
                membership.clone(),
                Arc::new(NoopCleaner),
                clock.clone(),
                mink_coordinator::Config::default(),
            ));
            if leader.is_none() {
                coordinator.become_leader().await.unwrap();
                leader = Some(coordinator.clone());
            }
            let flight_config = mink_flight::Config {
                leader_wait: Duration::from_secs(10),
                ..Default::default()
            };
            let flight = Flight::new(Service::new(node.clone()), coordinator, flight_config);
            let server =
                mink_flight::serve(format!("127.0.0.1:{}", ports[&id]).parse().unwrap(), flight)
                    .await
                    .unwrap();
            let channel = Channel::from_shared(format!("http://{}", server.local_addr()))
                .unwrap()
                .connect()
                .await
                .unwrap();
            members.push(Member {
                id,
                client: FlightServiceClient::new(channel),
                server,
                node,
                stop,
                task,
            });
        }
        let coordinator = leader.unwrap();
        coordinator
            .create_database("db", None, Default::default(), false)
            .await
            .unwrap();
        Cluster {
            members,
            coordinator,
            views,
            membership,
            _dir: dir,
        }
    }

    async fn stop(self) {
        for member in self.members {
            member.server.shutdown().await;
            let _ = member.stop.send(true);
            let _ = member.task.await;
        }
    }

    fn address(&self, id: i32) -> String {
        let member = self.members.iter().find(|m| m.id == id).unwrap();
        format!("grpc://{}", member.server.local_addr())
    }

    fn node(&self, id: i32) -> &Arc<Node> {
        &self.members.iter().find(|m| m.id == id).unwrap().node
    }

    fn client(&self, id: i32) -> FlightServiceClient<Channel> {
        self.members
            .iter()
            .find(|m| m.id == id)
            .unwrap()
            .client
            .clone()
    }

    fn leader_client(&self, bucket: Bucket) -> FlightServiceClient<Channel> {
        let view = self.views.load();
        let leader = view.state.catalog.buckets[&bucket].leader;
        self.client(leader)
    }

    async fn create(&self, path: &str, descriptor: Descriptor) -> mink_table::Id {
        let table_id = self
            .coordinator
            .create_table(&path.parse().unwrap(), &descriptor, false)
            .await
            .unwrap()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let view = self.views.load();
            let open = view
                .state
                .catalog
                .buckets_of(table_id, None)
                .all(|(bucket, row)| {
                    self.members
                        .iter()
                        .find(|m| m.id == row.leader)
                        .is_some_and(|m| m.node.registry().contains(*bucket))
                });
            if open {
                return table_id;
            }
            assert!(Instant::now() < deadline, "buckets never opened");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn put(
        &self,
        via: i32,
        write: &proto::Write,
        batches: Vec<(RecordBatch, proto::WriteBatch)>,
    ) -> Result<Vec<proto::Routed>, Status> {
        let descriptor = FlightDescriptor::new_cmd(serde_json::to_vec(write).unwrap());
        let schema = batches[0].0.schema();
        let mut frames: Vec<FlightData> = Vec::new();
        for (batch, meta) in batches {
            let encoded: Vec<FlightData> = FlightDataEncoderBuilder::new()
                .with_schema(schema.clone())
                .build(futures::stream::once(async { Ok(batch) }))
                .try_collect()
                .await
                .unwrap();
            for frame in encoded {
                if !frame.data_body.is_empty() {
                    let mut frame = frame;
                    frame.app_metadata = serde_json::to_vec(&meta).unwrap().into();
                    frames.push(frame);
                } else if frames.is_empty() {
                    frames.push(frame);
                }
            }
        }
        frames[0].flight_descriptor = Some(descriptor);
        let acks = self
            .client(via)
            .do_put(futures::stream::iter(frames))
            .await?
            .into_inner();
        acks.map_ok(|ack| serde_json::from_slice(&ack.app_metadata).unwrap())
            .try_collect()
            .await
    }

    async fn scan(&self, bucket: Bucket) -> Vec<(i64, String, String)> {
        let ticket = Ticket::new(
            serde_json::to_vec(&proto::Read::Scan {
                bucket,
                offset: 0,
                max_bytes: None,
                columns: None,
            })
            .unwrap(),
        );
        let stream = self
            .leader_client(bucket)
            .do_get(ticket)
            .await
            .unwrap()
            .into_inner();
        let mut decoder =
            FlightDataDecoder::new(stream.map_err(|s| FlightError::Tonic(Box::new(s))));
        let mut out = Vec::new();
        while let Some(frame) = decoder.next().await {
            if let DecodedPayload::RecordBatch(batch) = frame.unwrap().payload {
                out.extend(triples(&batch));
            }
        }
        out
    }

    async fn lookup(&self, bucket: Bucket, keys: Vec<Vec<u8>>) -> Vec<Vec<(i64, String, String)>> {
        let action = Action::new(
            action::LOOKUP,
            serde_json::to_vec(&proto::Lookup { bucket, keys }).unwrap(),
        );
        let results: Vec<_> = self
            .leader_client(bucket)
            .do_action(action)
            .await
            .unwrap()
            .into_inner()
            .try_collect()
            .await
            .unwrap();
        results
            .iter()
            .map(|r| {
                let reader = StreamReader::try_new(Cursor::new(&r.body[..]), None).unwrap();
                reader.flat_map(|b| triples(&b.unwrap())).collect()
            })
            .collect()
    }
}

fn schema(primary_key: bool) -> Schema {
    let mut builder = Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("region", DataType::string().with_nullable(!primary_key)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap());
    if primary_key {
        builder = builder.primary_key(PrimaryKey::new(vec!["k".into(), "region".into()]).unwrap());
    }
    builder.build().unwrap()
}

fn rows(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
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

fn plain() -> proto::WriteBatch {
    proto::WriteBatch::default()
}

fn path(name: &str) -> Path {
    format!("db.{name}").parse().unwrap()
}

#[tokio::test]
async fn keyed_appends_fan_out_to_every_leader() {
    let cluster = Cluster::start(&[1, 2]).await;
    let descriptor = Descriptor::builder(schema(false))
        .bucket_keys(["k"])
        .bucket_count(4)
        .build()
        .unwrap();
    let table_id = cluster.create("db.keyed", descriptor).await;
    {
        let view = cluster.views.load();
        let leaders: BTreeSet<i32> = view
            .state
            .catalog
            .buckets_of(table_id, None)
            .map(|(_, row)| row.leader)
            .collect();
        assert_eq!(leaders.len(), 2, "buckets spread over both nodes");
    }

    let batch = rows(&(0..40).map(|k| (k, Some("x"), "v")).collect::<Vec<_>>());
    let write = proto::Write::AppendTable {
        path: path("keyed"),
        schema_id: SchemaId(0),
    };
    let acks = cluster
        .put(
            2,
            &write,
            vec![(batch.clone(), plain()), (batch.clone(), plain())],
        )
        .await
        .unwrap();
    assert_eq!(acks.len(), 2);
    let first = &acks[0];
    assert_eq!(first.buckets.len(), 4, "40 keys land in all 4 buckets");
    assert_eq!(first.buckets.iter().map(|b| b.rows).sum::<usize>(), 40);
    assert!(first.buckets.iter().all(|b| b.partition.is_none()));
    assert!(first.buckets.iter().all(|b| b.first_offset == 0));
    for (a, b) in first.buckets.iter().zip(&acks[1].buckets) {
        assert_eq!(a.bucket, b.bucket);
        assert_eq!(a.rows, b.rows);
        assert_eq!(b.first_offset, a.last_offset + 1);
    }

    let encoder = mink_record::KeyEncoder::new(
        schema(false).fields(),
        &["k".to_owned()],
        mink_table::Bucketing::Native,
    )
    .unwrap();
    let bound = encoder.bind(&batch).unwrap();
    let mut expected: BTreeMap<BucketId, Vec<i64>> = BTreeMap::new();
    for row in 0..40 {
        let key = bound.encode_vec(row).unwrap();
        let bucket = mink_table::Bucketing::Native.bucket(&key, 4).unwrap();
        expected.entry(bucket).or_default().push(row as i64);
    }
    for (bucket, keys) in expected {
        let got = cluster.scan(Bucket::new(table_id, bucket)).await;
        let got: Vec<i64> = got.into_iter().map(|r| r.0).collect();
        let mut twice = keys.clone();
        twice.extend(keys);
        assert_eq!(got, twice, "bucket {bucket:?}");
    }
    cluster.stop().await;
}

#[tokio::test]
async fn keyless_batches_rotate_over_buckets() {
    let cluster = Cluster::start(&[1, 2]).await;
    let descriptor = Descriptor::builder(schema(false))
        .bucket_count(2)
        .build()
        .unwrap();
    let table_id = cluster.create("db.keyless", descriptor).await;
    let write = proto::Write::AppendTable {
        path: path("keyless"),
        schema_id: SchemaId(0),
    };
    let batches = (0..3)
        .map(|i| {
            (
                rows(&[(i, Some("r"), "a"), (i + 10, Some("r"), "b")]),
                plain(),
            )
        })
        .collect();
    let acks = cluster.put(1, &write, batches).await.unwrap();
    let buckets: Vec<u32> = acks
        .iter()
        .map(|ack| {
            assert_eq!(ack.buckets.len(), 1, "a key-less batch stays whole");
            assert_eq!(ack.buckets[0].rows, 2);
            ack.buckets[0].bucket.bucket().0
        })
        .collect();
    assert_eq!(buckets, vec![0, 1, 0]);
    assert_eq!(
        cluster.scan(Bucket::new(table_id, BucketId(0))).await.len(),
        4
    );
    assert_eq!(
        cluster.scan(Bucket::new(table_id, BucketId(1))).await.len(),
        2
    );
    cluster.stop().await;
}

#[tokio::test]
async fn puts_create_partitions_on_demand_through_the_coordinator() {
    let cluster = Cluster::start(&[1, 2]).await;
    let descriptor = Descriptor::builder(schema(true))
        .partitioned_by(["region"])
        .bucket_count(2)
        .build()
        .unwrap();
    let table_id = cluster.create("db.pk", descriptor).await;
    assert_eq!(
        cluster
            .views
            .load()
            .state
            .catalog
            .partitions_of(table_id)
            .count(),
        0
    );

    let write = proto::Write::PutTable {
        path: path("pk"),
        schema_id: SchemaId(0),
        target_columns: None,
    };
    let acks = cluster
        .put(
            2,
            &write,
            vec![(
                rows(&[
                    (1, Some("us"), "a"),
                    (2, Some("eu"), "b"),
                    (3, Some("us"), "c"),
                    (1, Some("eu"), "d"),
                ]),
                plain(),
            )],
        )
        .await
        .unwrap();
    let ack = &acks[0];
    assert_eq!(ack.buckets.iter().map(|b| b.rows).sum::<usize>(), 4);
    let partitions: BTreeSet<&str> = ack
        .buckets
        .iter()
        .map(|b| b.partition.as_ref().unwrap().as_str())
        .collect();
    assert_eq!(partitions, ["eu", "us"].into_iter().collect());
    {
        let view = cluster.views.load();
        let names: Vec<PartitionName> = view
            .state
            .catalog
            .partitions_of(table_id)
            .map(|p| p.name.clone())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(ack.buckets.iter().all(|b| b.bucket.partition().is_some()));
    }

    let acks = cluster
        .put(
            1,
            &write,
            vec![(
                rows(&[(1, Some("us"), "A"), (3, Some("us"), "gone")]),
                proto::WriteBatch {
                    batch_sequence: None,
                    changes: Some(vec![
                        ChangeType::UpdateAfter as u8,
                        ChangeType::Delete as u8,
                    ]),
                },
            )],
        )
        .await
        .unwrap();
    assert_eq!(acks[0].buckets.iter().map(|b| b.rows).sum::<usize>(), 2);
    assert_eq!(
        cluster
            .views
            .load()
            .state
            .catalog
            .partitions_of(table_id)
            .count(),
        2
    );

    let pk = schema(true);
    let full = mink_record::KeyEncoder::new(
        pk.fields(),
        &["k".to_owned(), "region".to_owned()],
        mink_table::Bucketing::Native,
    )
    .unwrap();
    let bucket_key = mink_record::KeyEncoder::new(
        pk.fields(),
        &["k".to_owned()],
        mink_table::Bucketing::Native,
    )
    .unwrap();
    let locate = |k: i64, region: &str| -> (Bucket, Vec<u8>) {
        let probe = rows(&[(k, Some(region), "")]);
        let bucket = mink_table::Bucketing::Native
            .bucket(&bucket_key.bind(&probe).unwrap().encode_vec(0).unwrap(), 2)
            .unwrap();
        let view = cluster.views.load();
        let partition = view
            .state
            .catalog
            .partitions_of(table_id)
            .find(|p| p.name.as_str() == region)
            .unwrap()
            .partition_id;
        (
            Bucket::partitioned(table_id, partition, bucket),
            full.bind(&probe).unwrap().encode_vec(0).unwrap(),
        )
    };
    for (k, region, value) in [
        (1, "us", Some("A")),
        (3, "us", None),
        (2, "eu", Some("b")),
        (1, "eu", Some("d")),
    ] {
        let (bucket, key) = locate(k, region);
        let want = value
            .map(|v| vec![(k, region.to_owned(), v.to_owned())])
            .unwrap_or_default();
        assert_eq!(
            cluster.lookup(bucket, vec![key]).await,
            vec![want],
            "{k} {region}"
        );
    }
    for routed in &ack.buckets {
        let (bucket, _) = locate(1, routed.partition.as_ref().unwrap().as_str());
        assert_eq!(bucket.partition(), routed.bucket.partition());
    }

    let err = cluster
        .put(2, &write, vec![(rows(&[(9, None, "z")]), plain())])
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    let err = cluster
        .put(
            2,
            &proto::Write::AppendTable {
                path: path("nope"),
                schema_id: SchemaId(0),
            },
            vec![(rows(&[(1, Some("us"), "a")]), plain())],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    let err = cluster
        .put(
            2,
            &proto::Write::PutTable {
                path: path("pk"),
                schema_id: SchemaId(7),
                target_columns: None,
            },
            vec![(rows(&[(1, Some("us"), "a")]), plain())],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    cluster.stop().await;
}

#[tokio::test]
async fn ops_actions_describe_the_cluster_and_rebalance_leaders() {
    let cluster = Cluster::start(&[1, 2]).await;
    let admin = mink_client::Cluster::connect(cluster.address(2))
        .unwrap()
        .admin();

    cluster.membership.remove(2);
    let descriptor = Descriptor::builder(schema(false))
        .bucket_keys(["k"])
        .bucket_count(4)
        .build()
        .unwrap();
    let table_id = cluster.create("db.ops", descriptor).await;

    let info = admin.describe_cluster().await.unwrap();
    assert_eq!(info.tables, 1);
    assert_eq!(info.buckets, 4);
    assert_eq!(info.unled_buckets, 0);
    assert_eq!(info.coordinator.as_ref().unwrap().node_id, 1);
    let by_id = |id: i32| info.nodes.iter().find(|n| n.node_id == id).unwrap();
    assert!(by_id(1).live);
    assert_eq!(by_id(1).leading, 4);
    assert!(!by_id(2).live, "registered but not heart-beating");
    assert_eq!(by_id(2).leading, 0);
    assert_eq!(by_id(2).address, cluster.address(2));

    let config = admin.get_config(Some(&cluster.address(2))).await.unwrap();
    assert_eq!(config.entries["node_id"], "2");
    assert_eq!(config.entries["cluster_id"], CLUSTER);
    assert_eq!(config.entries["flight.leader_wait"], "10s");
    assert!(!config.entries.contains_key("meta_url"));

    let h1 = admin.health(Some(&cluster.address(1))).await.unwrap();
    let h2 = admin.health(Some(&cluster.address(2))).await.unwrap();
    assert!(h1.registered && h1.coordinator && h1.hosted_buckets == 4);
    assert!(h2.registered && !h2.coordinator && h2.hosted_buckets == 0);

    let routed = cluster
        .put(
            1,
            &proto::Write::AppendTable {
                path: path("ops"),
                schema_id: SchemaId(0),
            },
            vec![(rows(&[(1, Some("us"), "a"), (2, Some("us"), "b")]), plain())],
        )
        .await
        .unwrap();
    let written: BTreeMap<Bucket, i64> = routed
        .iter()
        .flat_map(|r| r.buckets.iter().map(|b| (b.bucket, b.last_offset + 1)))
        .collect();
    let stats = admin.node_stats(Some(&cluster.address(1))).await.unwrap();
    assert_eq!(stats.node_id, 1);
    assert!(stats.coordinator);
    assert_eq!(stats.buckets.len(), 4);
    assert!(stats.buckets.iter().all(|b| b.bucket.table() == table_id));
    for b in &stats.buckets {
        assert_eq!(b.path, path("ops"));
        assert_eq!(
            b.log_end_offset,
            written.get(&b.bucket).copied().unwrap_or(0)
        );
        assert!(b.kv.is_none(), "log table");
    }
    assert_eq!(
        stats.buckets.iter().map(|b| b.log_end_offset).sum::<i64>(),
        2
    );
    assert!(stats.tiering.is_empty(), "no lake tables");

    cluster.membership.add(2);
    let moves = admin.rebalance().await.unwrap();
    assert!(!moves.is_empty());
    assert!(moves.iter().all(|m| m.from == 1 && m.to == 2), "{moves:?}");
    let deadline = Instant::now() + Duration::from_secs(10);
    while cluster.node(2).registry().len() < moves.len() {
        assert!(Instant::now() < deadline, "moves never landed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let info = admin.describe_cluster().await.unwrap();
    let leading: BTreeMap<i32, usize> = info.nodes.iter().map(|n| (n.node_id, n.leading)).collect();
    assert_eq!(leading[&1] + leading[&2], 4);
    assert_eq!(leading[&2], moves.len());
    assert!(info.nodes.iter().all(|n| n.live));

    let all = admin.cluster_stats().await.unwrap();
    assert_eq!(all.len(), 2);
    for (node, stats) in &all {
        let stats = stats.as_ref().unwrap();
        assert_eq!(stats.node_id, node.node_id);
        assert_eq!(stats.buckets.len(), leading[&node.node_id]);
    }
    assert!(admin.rebalance().await.unwrap().is_empty());
    cluster.stop().await;
}
