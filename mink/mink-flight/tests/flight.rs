//! The Flight protocol end to end on one node: flights, scans, puts, tails, actions and error statuses.

use std::collections::BTreeSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Criteria, Empty, FlightData, FlightDescriptor, Ticket};
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use mink_common::ManualClock;
use mink_coordinator::{Coordinator, NoopCleaner, StaticMembership};
use mink_flight::proto::{self, action};
use mink_flight::{Flight, Server};
use mink_kv::MemoryEngine;
use mink_metadata::{Handle, LocalSink};
use mink_record::ChangeType;
use mink_server::{Config, Failover, Node, Service};
use mink_table::{Bucket, BucketId, Column, Descriptor, PrimaryKey, Schema, SchemaId};
use mink_types::DataType;
use object_store::memory::InMemory;
use s3stream::{
    Client, MemoryObjectStorage, ObjectStorageTrait, ObjectWalConfig, ObjectWalService,
    S3StreamBuilder,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tonic::transport::Channel;
use tonic::{Code, Status};

const CLUSTER: &str = "mink-flight-test";
const NODE: i32 = 1;

struct Harness {
    client: FlightServiceClient<Channel>,
    server: Server,
    node: Arc<Node>,
    coordinator: Arc<Coordinator>,
    stop: watch::Sender<bool>,
    node_task: JoinHandle<()>,
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
        handle
            .register("grpc://node1:9000", 1, Default::default())
            .await
            .unwrap();
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
            handle,
            engine,
            Arc::new(InMemory::new()),
            Arc::new(MemoryEngine),
            membership.clone(),
            clock.clone(),
            config,
        ));
        let (stop, shutdown) = watch::channel(false);
        let node_task = node.clone().spawn(shutdown);

        let coordinator = Arc::new(Coordinator::new(
            NODE,
            "grpc://node1:9000",
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
        let server = mink_flight::serve("127.0.0.1:0".parse().unwrap(), flight)
            .await
            .unwrap();
        let channel = Channel::from_shared(format!("http://{}", server.local_addr()))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = FlightServiceClient::new(channel);
        Harness {
            client,
            server,
            node,
            coordinator,
            stop,
            node_task,
            _dir: dir,
        }
    }

    async fn stop(self) {
        self.server.shutdown().await;
        let _ = self.stop.send(true);
        let _ = self.node_task.await;
    }

    async fn action<T: DeserializeOwned>(&mut self, name: &str, body: &impl Serialize) -> T {
        let mut results = self.try_action(name, body).await.unwrap();
        assert_eq!(results.len(), 1, "{name} returns one result");
        serde_json::from_slice(&results.remove(0)).unwrap()
    }

    async fn try_action(
        &mut self,
        name: &str,
        body: &impl Serialize,
    ) -> Result<Vec<Bytes>, Status> {
        let action = Action::new(name, serde_json::to_vec(body).unwrap());
        let results = self.client.do_action(action).await?.into_inner();
        results.map_ok(|r| r.body).try_collect().await
    }

    async fn unit_action(&mut self, name: &str, body: &impl Serialize) {
        assert!(self.try_action(name, body).await.unwrap().is_empty());
    }

    async fn table(&mut self, path: &str) -> proto::TableInfo {
        self.action(
            action::GET_TABLE,
            &proto::TableRef {
                path: path.parse().unwrap(),
            },
        )
        .await
    }

    async fn create(&mut self, path: &str, descriptor: Descriptor) -> Bucket {
        let created: proto::Created = self
            .action(
                action::CREATE_TABLE,
                &proto::CreateTable {
                    path: path.parse().unwrap(),
                    descriptor,
                    ignore_if_exists: false,
                },
            )
            .await;
        let bucket = Bucket::new(created.table_id.unwrap(), BucketId(0));
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.node.registry().contains(bucket) {
            assert!(Instant::now() < deadline, "bucket never opened");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bucket
    }

    async fn put(
        &mut self,
        write: &proto::Write,
        batches: Vec<(RecordBatch, proto::WriteBatch)>,
    ) -> Result<Vec<proto::Written>, Status> {
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
                let is_batch = !frame.data_body.is_empty();
                if is_batch {
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
            .client
            .do_put(futures::stream::iter(frames))
            .await?
            .into_inner();
        acks.map_ok(|ack| serde_json::from_slice(&ack.app_metadata).unwrap())
            .try_collect()
            .await
    }

    async fn get(&mut self, read: &proto::Read) -> Result<Vec<(RecordBatch, Bytes)>, Status> {
        let ticket = Ticket::new(serde_json::to_vec(read).unwrap());
        let stream = self.client.do_get(ticket).await?.into_inner();
        let mut decoder =
            FlightDataDecoder::new(stream.map_err(|s| FlightError::Tonic(Box::new(s))));
        let mut out = Vec::new();
        while let Some(frame) = decoder.next().await {
            let frame = frame.map_err(|e| match e {
                FlightError::Tonic(status) => *status,
                other => Status::internal(other.to_string()),
            })?;
            if let DecodedPayload::RecordBatch(batch) = frame.payload {
                out.push((batch, frame.inner.app_metadata));
            }
        }
        Ok(out)
    }
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

fn descriptor(primary_key: bool) -> Descriptor {
    Descriptor::builder(schema(primary_key))
        .bucket_count(1)
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

fn pairs(batch: &RecordBatch) -> Vec<(i64, String)> {
    let k = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let v = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| (k.value(i), v.value(i).to_owned()))
        .collect()
}

fn ipc_rows(body: &[u8]) -> Vec<(i64, String)> {
    let reader = StreamReader::try_new(Cursor::new(body), None).unwrap();
    reader.flat_map(|b| pairs(&b.unwrap())).collect()
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

fn seq(n: i32) -> proto::WriteBatch {
    proto::WriteBatch {
        batch_sequence: Some(n),
        changes: None,
    }
}

#[tokio::test]
async fn ddl_flight_info_and_listing() {
    let mut h = Harness::start().await;
    h.unit_action(
        action::CREATE_DATABASE,
        &proto::CreateDatabase {
            name: "db".into(),
            comment: None,
            custom: Default::default(),
            ignore_if_exists: false,
        },
    )
    .await;
    let names: proto::Names = h.action(action::LIST_DATABASES, &()).await;
    assert_eq!(names.names, vec!["db".to_owned()]);

    let bucket = h.create("db.events", descriptor(false)).await;
    let info = h.table("db.events").await;
    assert_eq!(info.buckets.len(), 1);
    assert_eq!(info.buckets[0].bucket, bucket);
    assert_eq!(
        info.buckets[0].leader.as_ref().unwrap().address,
        "grpc://node1:9000"
    );

    let flight = h
        .client
        .get_flight_info(FlightDescriptor::new_path(vec![
            "db".into(),
            "events".into(),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(flight.endpoint.len(), 1);
    assert_eq!(flight.endpoint[0].location[0].uri, "grpc://node1:9000");
    let ticket: proto::Read =
        serde_json::from_slice(&flight.endpoint[0].ticket.as_ref().unwrap().ticket).unwrap();
    assert!(matches!(ticket, proto::Read::Scan { bucket: b, offset: 0, .. } if b == bucket));
    let arrow = flight.try_decode_schema().unwrap();
    assert_eq!(arrow.fields().len(), 2);

    let listed: Vec<_> = h
        .client
        .list_flights(Criteria {
            expression: Bytes::from_static(b"db"),
        })
        .await
        .unwrap()
        .into_inner()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);

    let exists: proto::Exists = h
        .action(
            action::TABLE_EXISTS,
            &proto::TableRef {
                path: "db.nope".parse().unwrap(),
            },
        )
        .await;
    assert!(!exists.exists);
    let missing = h
        .client
        .get_flight_info(FlightDescriptor::new_path(vec!["db".into(), "nope".into()]))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);

    let dup = h
        .try_action(
            action::CREATE_TABLE,
            &proto::CreateTable {
                path: "db.events".parse().unwrap(),
                descriptor: descriptor(false),
                ignore_if_exists: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(dup.code(), Code::AlreadyExists);
    h.stop().await;
}

#[tokio::test]
async fn append_then_scan_with_offsets_and_projection() {
    let mut h = Harness::start().await;
    h.unit_action(
        action::CREATE_DATABASE,
        &proto::CreateDatabase {
            name: "db".into(),
            comment: None,
            custom: Default::default(),
            ignore_if_exists: false,
        },
    )
    .await;
    let bucket = h.create("db.log", descriptor(false)).await;
    let writer: proto::WriterId = h.action(action::INIT_WRITER, &()).await;

    let write = proto::Write::Append {
        bucket,
        schema_id: SchemaId(0),
        writer_id: Some(writer.writer_id),
    };
    let acks = h
        .put(
            &write,
            vec![
                (rows(&[(1, "a"), (2, "b")]), seq(0)),
                (rows(&[(3, "c")]), seq(1)),
            ],
        )
        .await
        .unwrap();
    assert_eq!(acks.len(), 2);
    assert_eq!((acks[0].first_offset, acks[0].last_offset), (0, 1));
    assert_eq!((acks[1].first_offset, acks[1].last_offset), (2, 2));
    assert!(!acks[1].duplicated);

    let retry = h
        .put(&write, vec![(rows(&[(3, "c")]), seq(1))])
        .await
        .unwrap();
    assert!(retry[0].duplicated);

    let latest: proto::Offset = h
        .action(
            action::LIST_OFFSETS,
            &proto::ListOffsets {
                bucket,
                spec: proto::OffsetSpec::Latest,
            },
        )
        .await;
    assert_eq!(latest.offset, 3);

    let scanned = h
        .get(&proto::Read::Scan {
            bucket,
            offset: 0,
            max_bytes: None,
            columns: None,
        })
        .await
        .unwrap();
    assert_eq!(scanned.len(), 2);
    let all: Vec<_> = scanned.iter().flat_map(|(b, _)| pairs(b)).collect();
    assert_eq!(all, vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]);
    let meta: proto::ScanBatch = serde_json::from_slice(&scanned[1].1).unwrap();
    assert_eq!((meta.base_offset, meta.last_offset), (2, 2));
    assert_eq!(meta.high_watermark, 3);
    assert!(meta.changes.is_none());

    let tail = h
        .get(&proto::Read::Scan {
            bucket,
            offset: 2,
            max_bytes: None,
            columns: Some(vec![1]),
        })
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].0.num_columns(), 1);
    assert_eq!(
        tail[0]
            .0
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "c"
    );

    let last = h
        .get(&proto::Read::LimitScan {
            bucket,
            limit: 1,
            columns: None,
        })
        .await
        .unwrap();
    assert_eq!(pairs(&last[0].0), vec![(3, "c".into())]);

    let wrong_table = h
        .put(
            &proto::Write::Put {
                bucket,
                schema_id: SchemaId(0),
                writer_id: None,
                target_columns: None,
            },
            vec![(rows(&[(9, "z")]), proto::WriteBatch::default())],
        )
        .await
        .unwrap_err();
    assert_eq!(wrong_table.code(), Code::InvalidArgument);

    let nowhere = Bucket::new(mink_table::Id(999), BucketId(0));
    let missing = h
        .get(&proto::Read::Scan {
            bucket: nowhere,
            offset: 0,
            max_bytes: None,
            columns: None,
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);
    h.stop().await;
}

#[tokio::test]
async fn upsert_lookup_delete_and_changelog() {
    let mut h = Harness::start().await;
    h.unit_action(
        action::CREATE_DATABASE,
        &proto::CreateDatabase {
            name: "db".into(),
            comment: None,
            custom: Default::default(),
            ignore_if_exists: false,
        },
    )
    .await;
    let bucket = h.create("db.kv", descriptor(true)).await;
    let write = proto::Write::Put {
        bucket,
        schema_id: SchemaId(0),
        writer_id: None,
        target_columns: None,
    };
    h.put(
        &write,
        vec![(rows(&[(1, "a"), (2, "b")]), proto::WriteBatch::default())],
    )
    .await
    .unwrap();
    h.put(
        &write,
        vec![(
            rows(&[(1, "A"), (2, "")]),
            proto::WriteBatch {
                batch_sequence: None,
                changes: Some(vec![
                    ChangeType::UpdateAfter.byte(),
                    ChangeType::Delete.byte(),
                ]),
            },
        )],
    )
    .await
    .unwrap();

    let found = h
        .try_action(
            action::LOOKUP,
            &proto::Lookup {
                bucket,
                keys: vec![key(1), key(2)],
            },
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 2);
    assert_eq!(ipc_rows(&found[0]), vec![(1, "A".into())]);
    assert!(ipc_rows(&found[1]).is_empty());

    let first = h
        .get(&proto::Read::LimitScan {
            bucket,
            limit: 10,
            columns: None,
        })
        .await
        .unwrap();
    assert_eq!(pairs(&first[0].0), vec![(1, "A".into())]);

    let changelog = h
        .get(&proto::Read::Scan {
            bucket,
            offset: 0,
            max_bytes: None,
            columns: None,
        })
        .await
        .unwrap();
    let changes: Vec<u8> = changelog
        .iter()
        .flat_map(|(_, meta)| {
            serde_json::from_slice::<proto::ScanBatch>(meta)
                .unwrap()
                .changes
                .unwrap()
        })
        .collect();
    assert_eq!(
        changes,
        vec![
            ChangeType::Insert.byte(),
            ChangeType::Insert.byte(),
            ChangeType::UpdateBefore.byte(),
            ChangeType::UpdateAfter.byte(),
            ChangeType::Delete.byte(),
        ]
    );
    let snapshot: proto::LatestKvSnapshot = h
        .action(action::LATEST_KV_SNAPSHOT, &proto::BucketRef { bucket })
        .await;
    assert!(snapshot.snapshot.is_none());
    h.stop().await;
}

#[tokio::test]
async fn snapshot_streams_pinned_rows_and_hands_off_to_the_log() {
    let mut h = Harness::start().await;
    h.unit_action(
        action::CREATE_DATABASE,
        &proto::CreateDatabase {
            name: "db".into(),
            comment: None,
            custom: Default::default(),
            ignore_if_exists: false,
        },
    )
    .await;
    let bucket = h.create("db.kv", descriptor(true)).await;
    let write = proto::Write::Put {
        bucket,
        schema_id: SchemaId(0),
        writer_id: None,
        target_columns: None,
    };
    let initial: Vec<(i64, &str)> = (1..=10).map(|k| (k, "v")).collect();
    h.put(&write, vec![(rows(&initial), proto::WriteBatch::default())])
        .await
        .unwrap();
    h.put(
        &write,
        vec![(
            rows(&[(3, ""), (5, "five")]),
            proto::WriteBatch {
                batch_sequence: None,
                changes: Some(vec![
                    ChangeType::Delete.byte(),
                    ChangeType::UpdateAfter.byte(),
                ]),
            },
        )],
    )
    .await
    .unwrap();

    let frames = h
        .get(&proto::Read::Snapshot {
            bucket,
            columns: Some(vec![0]),
            batch_rows: Some(4),
        })
        .await
        .unwrap();
    assert_eq!(frames.len(), 3, "9 rows in pages of 4");
    let offsets: BTreeSet<i64> = frames
        .iter()
        .map(|(_, meta)| {
            serde_json::from_slice::<proto::SnapshotBatch>(meta)
                .unwrap()
                .log_offset
        })
        .collect();
    assert_eq!(offsets.into_iter().collect::<Vec<_>>(), vec![13]);
    let keys: Vec<i64> = frames
        .iter()
        .flat_map(|(batch, _)| {
            assert_eq!(batch.num_columns(), 1);
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(keys, vec![1, 2, 4, 5, 6, 7, 8, 9, 10]);

    let full = h
        .get(&proto::Read::Snapshot {
            bucket,
            columns: None,
            batch_rows: None,
        })
        .await
        .unwrap();
    assert_eq!(full.len(), 1);
    let mut expected: Vec<(i64, String)> = (1..=10)
        .filter(|k| *k != 3)
        .map(|k| (k, if k == 5 { "five".into() } else { "v".into() }))
        .collect();
    expected.sort();
    assert_eq!(pairs(&full[0].0), expected);

    h.put(
        &write,
        vec![(rows(&[(11, "new")]), proto::WriteBatch::default())],
    )
    .await
    .unwrap();
    let tail = h
        .get(&proto::Read::Scan {
            bucket,
            offset: 13,
            max_bytes: None,
            columns: None,
        })
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(pairs(&tail[0].0), vec![(11, "new".into())]);

    let flight = h
        .client
        .get_flight_info(FlightDescriptor::new_path(vec!["db".into(), "kv".into()]))
        .await
        .unwrap()
        .into_inner();
    let ticket: proto::Read =
        serde_json::from_slice(&flight.endpoint[0].ticket.as_ref().unwrap().ticket).unwrap();
    assert!(matches!(ticket, proto::Read::Snapshot { bucket: b, .. } if b == bucket));

    let log_bucket = h.create("db.log", descriptor(false)).await;
    let err = h
        .get(&proto::Read::Snapshot {
            bucket: log_bucket,
            columns: None,
            batch_rows: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    h.stop().await;
}

#[tokio::test]
async fn ddl_off_the_coordinator_redirects_and_actions_are_listed() {
    let mut h = Harness::start().await;
    let actions: Vec<_> = h
        .client
        .list_actions(Empty {})
        .await
        .unwrap()
        .into_inner()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actions.len(), action::ALL.len());
    let unknown = h.try_action("frobnicate", &()).await.unwrap_err();
    assert_eq!(unknown.code(), Code::InvalidArgument);

    h.coordinator.resign();
    let bounced = h
        .try_action(
            action::CREATE_DATABASE,
            &proto::CreateDatabase {
                name: "db".into(),
                comment: None,
                custom: Default::default(),
                ignore_if_exists: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(bounced.code(), Code::FailedPrecondition);
    let redirect: proto::Redirect = serde_json::from_slice(bounced.details()).unwrap();
    assert_eq!(redirect.to.unwrap().address, "grpc://node1:9000");
    let names: proto::Names = h.action(action::LIST_DATABASES, &()).await;
    assert!(names.names.is_empty());
    h.stop().await;
}

#[tokio::test]
async fn tail_follows_appends_and_ends_when_the_client_hangs_up() {
    let mut h = Harness::start().await;
    h.unit_action(
        action::CREATE_DATABASE,
        &proto::CreateDatabase {
            name: "db".into(),
            comment: None,
            custom: Default::default(),
            ignore_if_exists: false,
        },
    )
    .await;
    let bucket = h.create("db.log", descriptor(false)).await;
    let write = proto::Write::Append {
        bucket,
        schema_id: SchemaId(0),
        writer_id: None,
    };
    h.put(
        &write,
        vec![(rows(&[(1, "a")]), proto::WriteBatch::default())],
    )
    .await
    .unwrap();

    let tail = proto::Tail {
        bucket,
        offset: 0,
        columns: None,
        max_wait_ms: Some(100),
        min_bytes: None,
    };
    let (requests, requests_rx) = tokio::sync::mpsc::channel::<FlightData>(1);
    requests
        .send(FlightData {
            flight_descriptor: Some(FlightDescriptor::new_cmd(
                serde_json::to_vec(&tail).unwrap(),
            )),
            ..Default::default()
        })
        .await
        .unwrap();
    let responses = h
        .client
        .do_exchange(tokio_stream::wrappers::ReceiverStream::new(requests_rx))
        .await
        .unwrap()
        .into_inner();
    let mut decoder =
        FlightDataDecoder::new(responses.map_err(|s| FlightError::Tonic(Box::new(s))));
    async fn next_batch(decoder: &mut FlightDataDecoder) -> (RecordBatch, proto::ScanBatch) {
        loop {
            let frame = decoder.next().await.unwrap().unwrap();
            if let DecodedPayload::RecordBatch(batch) = frame.payload {
                let meta: proto::ScanBatch =
                    serde_json::from_slice(&frame.inner.app_metadata).unwrap();
                return (batch, meta);
            }
        }
    }

    let (batch, meta) = next_batch(&mut decoder).await;
    assert_eq!(pairs(&batch), vec![(1, "a".into())]);
    assert_eq!(
        (meta.base_offset, meta.last_offset, meta.high_watermark),
        (0, 0, 1)
    );

    let (batch, meta) = next_batch(&mut decoder).await;
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(
        (meta.base_offset, meta.last_offset, meta.high_watermark),
        (1, 0, 1)
    );

    h.put(
        &write,
        vec![(rows(&[(2, "b"), (3, "c")]), proto::WriteBatch::default())],
    )
    .await
    .unwrap();
    let (batch, meta) = loop {
        let (batch, meta) = next_batch(&mut decoder).await;
        if batch.num_rows() > 0 {
            break (batch, meta);
        }
    };
    assert_eq!(pairs(&batch), vec![(2, "b".into()), (3, "c".into())]);
    assert_eq!(
        (meta.base_offset, meta.last_offset, meta.high_watermark),
        (1, 2, 3)
    );

    drop(requests);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "tail did not end");
        match decoder.next().await {
            None => break,
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("tail failed: {e}"),
        }
    }
    h.stop().await;
}

#[tokio::test]
async fn alter_table_adds_a_column_and_scans_widen_old_batches() {
    let mut h = Harness::start().await;
    h.unit_action(
        action::CREATE_DATABASE,
        &proto::CreateDatabase {
            name: "db".into(),
            comment: None,
            custom: Default::default(),
            ignore_if_exists: false,
        },
    )
    .await;
    let bucket = h.create("db.evolving", descriptor(false)).await;
    h.put(
        &proto::Write::Append {
            bucket,
            schema_id: SchemaId(0),
            writer_id: None,
        },
        vec![(rows(&[(1, "a")]), proto::WriteBatch::default())],
    )
    .await
    .unwrap();

    let altered: proto::Altered = h
        .action(
            action::ALTER_TABLE,
            &proto::AlterTable {
                path: "db.evolving".parse().unwrap(),
                changes: vec![
                    mink_table::Change::add_column("w", DataType::int()),
                    mink_table::Change::set("owner", "ann"),
                ],
                ignore_if_not_exists: false,
            },
        )
        .await;
    assert_eq!(altered.schema_id, Some(SchemaId(1)));
    let info = h.table("db.evolving").await;
    assert_eq!(info.schemas.len(), 2);
    assert_eq!(info.descriptor.schema().columns()[2].name(), "w");
    assert_eq!(
        info.descriptor.custom().get("owner").map(String::as_str),
        Some("ann")
    );

    let wide = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(info.schemas[1].fields())),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let result = h
            .put(
                &proto::Write::Append {
                    bucket,
                    schema_id: SchemaId(1),
                    writer_id: None,
                },
                vec![(wide.clone(), proto::WriteBatch::default())],
            )
            .await;
        if result.is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node never learned schema 1: {result:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let scanned = h
        .get(&proto::Read::Scan {
            bucket,
            offset: 0,
            max_bytes: None,
            columns: None,
        })
        .await
        .unwrap();
    assert_eq!(scanned.len(), 2);
    for (batch, _) in &scanned {
        assert_eq!(batch.num_columns(), 3, "widened to the latest schema");
    }
    let old = scanned[0]
        .0
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(Array::is_null(old, 0));
    let new = scanned[1]
        .0
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(new.value(0), 20);
    let meta: proto::ScanBatch = serde_json::from_slice(&scanned[0].1).unwrap();
    assert_eq!(meta.schema_id, SchemaId(0));

    let altered: proto::Altered = h
        .action(
            action::ALTER_TABLE,
            &proto::AlterTable {
                path: "db.evolving".parse().unwrap(),
                changes: vec![
                    mink_table::Change::RenameColumn {
                        name: "v".into(),
                        new_name: "value".into(),
                    },
                    mink_table::Change::ModifyColumn {
                        name: "w".into(),
                        data_type: DataType::big_int(),
                        comment: None,
                    },
                ],
                ignore_if_not_exists: false,
            },
        )
        .await;
    assert_eq!(altered.schema_id, Some(SchemaId(2)));
    let deadline = Instant::now() + Duration::from_secs(10);
    let scanned = loop {
        let scanned = h
            .get(&proto::Read::Scan {
                bucket,
                offset: 0,
                max_bytes: None,
                columns: Some(vec![2, 1]),
            })
            .await
            .unwrap();
        if scanned[0].0.schema().field(0).data_type() == &arrow_schema::DataType::Int64 {
            break scanned;
        }
        assert!(Instant::now() < deadline, "node never learned schema 2");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(scanned.len(), 2);
    let schema = scanned[1].0.schema();
    assert_eq!(schema.field(0).name(), "w");
    assert_eq!(schema.field(1).name(), "value");
    let promoted = scanned[1]
        .0
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(promoted.value(0), 20);
    assert!(Array::is_null(scanned[0].0.column(0), 0));
    let renamed = scanned[0]
        .0
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(renamed.value(0), "a");

    let err = h
        .try_action(
            action::ALTER_TABLE,
            &proto::AlterTable {
                path: "db.evolving".parse().unwrap(),
                changes: vec![mink_table::Change::ModifyColumn {
                    name: "k".into(),
                    data_type: DataType::string(),
                    comment: None,
                }],
                ignore_if_not_exists: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    let err = h
        .try_action(
            action::ALTER_TABLE,
            &proto::AlterTable {
                path: "db.evolving".parse().unwrap(),
                changes: vec![mink_table::Change::set("table.datalake.enabled", "true")],
                ignore_if_not_exists: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "no lake on this cluster: {err}"
    );
    let missing = h
        .try_action(
            action::ALTER_TABLE,
            &proto::AlterTable {
                path: "db.nope".parse().unwrap(),
                changes: vec![mink_table::Change::set("k", "v")],
                ignore_if_not_exists: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);
    h.stop().await;
}

#[tokio::test]
async fn producer_offsets_are_registered_once_read_back_and_deleted() {
    let mut h = Harness::start().await;
    h.unit_action(
        action::CREATE_DATABASE,
        &proto::CreateDatabase {
            name: "db".into(),
            comment: None,
            custom: Default::default(),
            ignore_if_exists: false,
        },
    )
    .await;
    let bucket = h.create("db.sink", descriptor(false)).await;
    h.put(
        &proto::Write::Append {
            bucket,
            schema_id: SchemaId(0),
            writer_id: None,
        },
        vec![(rows(&[(1, "a"), (2, "b")]), proto::WriteBatch::default())],
    )
    .await
    .unwrap();
    let latest: proto::Offset = h
        .action(
            action::LIST_OFFSETS,
            &proto::ListOffsets {
                bucket,
                spec: proto::OffsetSpec::Latest,
            },
        )
        .await;
    assert_eq!(latest.offset, 2);

    let none: proto::ProducerOffsetsResult = h
        .action(
            action::GET_PRODUCER_OFFSETS,
            &proto::ProducerRef {
                producer_id: "flink-job".into(),
            },
        )
        .await;
    assert_eq!(none.snapshot, None);

    let register = proto::RegisterProducerOffsets {
        producer_id: "flink-job".into(),
        offsets: vec![proto::BucketOffset {
            bucket,
            offset: latest.offset,
        }],
        ttl_ms: Some(3_600_000),
    };
    let first: proto::ProducerOffsetsRegistered =
        h.action(action::REGISTER_PRODUCER_OFFSETS, &register).await;
    assert!(first.created);
    let second: proto::ProducerOffsetsRegistered = h
        .action(
            action::REGISTER_PRODUCER_OFFSETS,
            &proto::RegisterProducerOffsets {
                offsets: vec![proto::BucketOffset { bucket, offset: 99 }],
                ..register.clone()
            },
        )
        .await;
    assert!(!second.created, "already registered");

    let got: proto::ProducerOffsetsResult = h
        .action(
            action::GET_PRODUCER_OFFSETS,
            &proto::ProducerRef {
                producer_id: "flink-job".into(),
            },
        )
        .await;
    let snapshot = got.snapshot.unwrap();
    assert_eq!(snapshot.producer_id, "flink-job");
    assert_eq!(
        snapshot.offsets, register.offsets,
        "the first registration stands"
    );
    assert_eq!(snapshot.expires_ms, 1_700_000_000_000 + 3_600_000);

    let bad = h
        .try_action(
            action::REGISTER_PRODUCER_OFFSETS,
            &proto::RegisterProducerOffsets {
                producer_id: "not a name".into(),
                ..register.clone()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(bad.code(), Code::InvalidArgument);
    let negative = h
        .try_action(
            action::REGISTER_PRODUCER_OFFSETS,
            &proto::RegisterProducerOffsets {
                ttl_ms: Some(-1),
                ..register.clone()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(negative.code(), Code::InvalidArgument);

    h.unit_action(
        action::DELETE_PRODUCER_OFFSETS,
        &proto::ProducerRef {
            producer_id: "flink-job".into(),
        },
    )
    .await;
    let gone: proto::ProducerOffsetsResult = h
        .action(
            action::GET_PRODUCER_OFFSETS,
            &proto::ProducerRef {
                producer_id: "flink-job".into(),
            },
        )
        .await;
    assert_eq!(gone.snapshot, None);
    h.unit_action(
        action::DELETE_PRODUCER_OFFSETS,
        &proto::ProducerRef {
            producer_id: "flink-job".into(),
        },
    )
    .await;
    h.stop().await;
}
