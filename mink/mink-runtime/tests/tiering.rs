//! Tiering to an in-memory Iceberg lake across a running cluster, including union reads.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_flight::FlightDescriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use bytes::Bytes;
use futures::TryStreamExt;
use mink_client::{Connection, proto};
use mink_lake::Config;
use mink_lake::iceberg::Catalog;
use mink_record::{ChangeType, Compression, Spec, build, codec};
use mink_runtime::{Server, ServerConfig, start};
use mink_table::{
    Bucket, BucketId, Column, Descriptor, LakeFormat, LogFormat, Options, Path, PrimaryKey, Schema,
    SchemaId,
};
use mink_tablet::{Op, Put};
use mink_types::DataType;
use tokio::time::Instant;
use tonic::transport::Channel;

fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(dir: &path::Path, node_id: i32, port: u16, warehouse: &str) -> ServerConfig {
    ServerConfig {
        node_id,
        cluster_id: "tiering-test".into(),
        meta_url: format!("sqlite:{}", dir.join("meta.db").display()),
        storage_uri: format!("-2@file://{}", dir.join("objects").display()),
        wal_uri: None,
        data_dir: dir.join(format!("n{node_id}")),
        listen: format!("127.0.0.1:{port}").parse().unwrap(),
        advertise: format!("grpc://127.0.0.1:{port}"),
        lease_ttl: Duration::from_secs(2),
        lake: Some(Config::Iceberg(mink_lake::iceberg::Config::memory(
            warehouse,
        ))),
        tiering_poll_interval: Duration::from_millis(100),
        ..ServerConfig::default()
    }
}

fn schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .build()
        .unwrap()
}

fn batch(rows: &[(i64, &str)]) -> Bytes {
    let records = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(schema().fields())),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let codec = codec(LogFormat::Arrow, Compression::None);
    Bytes::from(
        build(
            Spec::new(SchemaId(0), true),
            &vec![ChangeType::AppendOnly; rows.len()],
            &records,
            codec.as_ref(),
        )
        .unwrap(),
    )
}

async fn config_summary(server: &Server) -> BTreeMap<String, String> {
    mink_client::Cluster::connect(format!("grpc://{}", server.flight_addr()))
        .unwrap()
        .admin()
        .get_config(None)
        .await
        .unwrap()
        .entries
}

async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn coordinator_node(nodes: &[Server]) -> &Server {
    nodes
        .iter()
        .find(|n| n.coordinator().is_leader())
        .expect("one node leads")
}

async fn leader_of(nodes: &[Server], bucket: Bucket) -> &Server {
    let mut found = None;
    wait_until("bucket hosted", || {
        found = nodes
            .iter()
            .position(|n| n.node().registry().contains(bucket));
        found.is_some()
    })
    .await;
    &nodes[found.unwrap()]
}

#[tokio::test]
async fn coordinator_node_tiers_local_and_remote_buckets_into_one_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", dir.path().join("lake").display());
    let ports = [reserve_port(), reserve_port()];
    let first = start(config(dir.path(), 1, ports[0], &warehouse))
        .await
        .unwrap();
    let coordinator = first.coordinator().clone();
    wait_until("lease", || coordinator.is_leader()).await;
    let second = start(config(dir.path(), 2, ports[1], &warehouse))
        .await
        .unwrap();
    let nodes = [first, second];
    wait_until("two live nodes", || {
        nodes[0].service().view().state.nodes.len() == 2
    })
    .await;

    coordinator
        .create_database("db", None, Default::default(), false)
        .await
        .unwrap();
    let path: Path = "db.events".parse().unwrap();
    let descriptor = Descriptor::builder(schema())
        .bucket_count(2)
        .options(Options {
            lake: Some(LakeFormat::Iceberg),
            lake_freshness: Duration::from_millis(200),
            ..Options::default()
        })
        .build()
        .unwrap();
    let table = coordinator
        .create_table(&path, &descriptor, false)
        .await
        .unwrap()
        .unwrap();
    let buckets = [
        Bucket::new(table, BucketId(0)),
        Bucket::new(table, BucketId(1)),
    ];
    let leaders = [
        leader_of(&nodes, buckets[0]).await,
        leader_of(&nodes, buckets[1]).await,
    ];
    assert_ne!(
        leaders[0].node().node_id(),
        leaders[1].node().node_id(),
        "the two buckets should land on different nodes"
    );

    leaders[0]
        .service()
        .append(buckets[0], batch(&[(1, "a"), (2, "b")]))
        .await
        .unwrap();
    leaders[1]
        .service()
        .append(buckets[1], batch(&[(3, "c")]))
        .await
        .unwrap();

    wait_until("lake snapshot", || {
        coordinator
            .lake_snapshot(table)
            .is_some_and(|s| s.bucket_log_end_offset.len() == 2)
    })
    .await;
    let snapshot = coordinator.lake_snapshot(table).unwrap();
    assert_eq!(snapshot.bucket_log_end_offset[&buckets[0]], 2);
    assert_eq!(snapshot.bucket_log_end_offset[&buckets[1]], 1);

    let stats = coordinator_node(&nodes).stats().await;
    assert!(stats.coordinator);
    let scheduled = stats
        .tiering
        .iter()
        .find(|t| t.table_id == table)
        .expect("lake table in the schedule");
    assert_eq!(scheduled.path, path);
    assert!(scheduled.last_tiered_ms > 0);
    assert!(
        matches!(
            scheduled.state.as_str(),
            "scheduled" | "pending" | "tiering" | "tiered"
        ),
        "{}",
        scheduled.state
    );
    let other = nodes
        .iter()
        .find(|n| !n.coordinator().is_leader())
        .unwrap()
        .stats()
        .await;
    assert!(!other.coordinator && other.tiering.is_empty());
    assert_eq!(config_summary(&nodes[0]).await["lake.format"], "iceberg");

    let lake_table = coordinator_node(&nodes)
        .lake()
        .unwrap()
        .target()
        .load(&Catalog::identifier(&path))
        .await
        .unwrap();
    assert_eq!(
        lake_table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id(),
        snapshot.snapshot_id
    );
    let batches: Vec<RecordBatch> = lake_table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut lake_rows: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            assert_eq!(b.num_columns(), 2, "user columns only in the lake");
            let k = b.column_by_name("k").unwrap().as_primitive::<Int64Type>();
            (0..b.num_rows()).map(move |i| k.value(i))
        })
        .collect();
    lake_rows.sort();
    assert_eq!(lake_rows, vec![1, 2, 3]);
    assert!(
        lake_table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty(),
        "keyless log tables are not bucketed in the lake"
    );

    leaders[0]
        .service()
        .append(buckets[0], batch(&[(4, "d")]))
        .await
        .unwrap();
    leaders[1]
        .service()
        .append(buckets[1], batch(&[(5, "e"), (6, "f")]))
        .await
        .unwrap();
    let info = raw_client(&nodes[0])
        .await
        .get_flight_info(FlightDescriptor::new_path(vec![
            "db".into(),
            "events".into(),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        info.endpoint.len(),
        3,
        "one lake read for the table plus one log tail per bucket"
    );
    let mut lake_endpoints = 0;
    let mut union_rows = Vec::new();
    for endpoint in &info.endpoint {
        let ticket: proto::Read =
            serde_json::from_slice(&endpoint.ticket.as_ref().unwrap().ticket).unwrap();
        match &ticket {
            proto::Read::Lake {
                path: p,
                partition: None,
                snapshot_id,
                ..
            } => {
                assert_eq!(p, &path);
                assert_eq!(*snapshot_id, snapshot.snapshot_id);
                lake_endpoints += 1;
            }
            proto::Read::Scan { bucket, offset, .. } => {
                assert_eq!(*offset, snapshot.bucket_log_end_offset[bucket]);
            }
            other => panic!("unexpected ticket {other:?}"),
        }
        let location = &endpoint.location[0].uri;
        let batches: Vec<RecordBatch> =
            union_batches(Connection::new(location).unwrap(), &ticket).await;
        for b in &batches {
            assert_eq!(b.schema().fields().len(), 2, "user columns only");
            let k = b.column(0).as_primitive::<Int64Type>();
            let v = b.column(1).as_string::<i32>();
            union_rows.extend((0..b.num_rows()).map(|i| (k.value(i), v.value(i).to_string())));
        }
    }
    assert_eq!(lake_endpoints, 1);
    union_rows.sort();
    assert_eq!(
        union_rows,
        [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")]
            .map(|(k, v)| (k, v.to_string()))
    );
    let err = Connection::new(leaders[0].advertise())
        .unwrap()
        .union(buckets[0], None)
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("keyless log tables"),
        "per-bucket union is refused for keyless tables: {err}"
    );
    let table_wide: Vec<RecordBatch> = mink_client::Cluster::connect(nodes[0].advertise())
        .unwrap()
        .table(&path)
        .await
        .unwrap()
        .union_all(Some(vec![0]))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut keys: Vec<i64> = table_wide
        .iter()
        .flat_map(|b| {
            assert_eq!(b.num_columns(), 1);
            let k = b.column(0).as_primitive::<Int64Type>();
            (0..b.num_rows()).map(move |i| k.value(i))
        })
        .collect();
    keys.sort();
    assert_eq!(keys, vec![1, 2, 3, 4, 5, 6]);

    let users: Path = "db.users".parse().unwrap();
    let pk_schema = Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .primary_key(PrimaryKey::new(vec!["k".into()]).unwrap())
        .build()
        .unwrap();
    let pk_table = coordinator
        .create_table(
            &users,
            &Descriptor::builder(pk_schema.clone())
                .bucket_count(1)
                .options(Options {
                    lake: Some(LakeFormat::Iceberg),
                    lake_freshness: Duration::from_millis(200),
                    ..Options::default()
                })
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap()
        .unwrap();
    let pk_bucket = Bucket::new(pk_table, BucketId(0));
    let pk_leader = leader_of(&nodes, pk_bucket).await;
    pk_leader
        .service()
        .put(
            pk_bucket,
            Put::upsert(SchemaId(0), rows(&[(1, "a"), (2, "b")])),
        )
        .await
        .unwrap();
    wait_until("pk lake snapshot", || {
        coordinator
            .lake_snapshot(pk_table)
            .is_some_and(|s| s.bucket_log_end_offset.get(&pk_bucket) == Some(&2))
    })
    .await;
    pk_leader
        .service()
        .put(
            pk_bucket,
            Put::upsert(SchemaId(0), rows(&[(2, "B"), (3, "c")])),
        )
        .await
        .unwrap();
    pk_leader
        .service()
        .put(
            pk_bucket,
            Put::upsert(SchemaId(0), rows(&[(1, "a")])).with_ops(vec![Op::Delete]),
        )
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = Connection::new(pk_leader.advertise())
        .unwrap()
        .union(pk_bucket, None)
        .try_collect()
        .await
        .unwrap();
    let mut pk_rows = Vec::new();
    for b in &batches {
        let k = b.column(0).as_primitive::<Int64Type>();
        let v = b.column(1).as_string::<i32>();
        pk_rows.extend((0..b.num_rows()).map(|i| (k.value(i), v.value(i).to_string())));
    }
    assert_eq!(pk_rows, vec![(2, "B".to_string()), (3, "c".to_string())]);

    for node in nodes {
        node.shutdown().await;
    }
}

async fn union_batches(connection: Connection, read: &proto::Read) -> Vec<RecordBatch> {
    use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
    use arrow_flight::error::FlightError;
    use futures::StreamExt;

    let response = connection.get(read).await.unwrap();
    let mut decoder =
        FlightDataDecoder::new(response.map(|r| r.map_err(|s| FlightError::Tonic(Box::new(s)))));
    let mut out = Vec::new();
    while let Some(frame) = decoder.next().await {
        if let DecodedPayload::RecordBatch(batch) = frame.unwrap().payload {
            out.push(batch);
        }
    }
    out
}

async fn raw_client(node: &Server) -> FlightServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{}", node.flight_addr()))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FlightServiceClient::new(channel)
}

fn rows(kv: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(kv.iter().map(|(k, _)| *k))),
            Arc::new(StringArray::from_iter_values(kv.iter().map(|(_, v)| *v))),
        ],
    )
    .unwrap()
}
