//! Starting and stopping a node, health and configuration reporting.

use std::path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_flight::FlightDescriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use bytes::Bytes;
use mink_record::{Batch, ChangeType, Compression, Spec, build, codec};
use mink_runtime::{Server, ServerConfig, start};
use mink_table::{Bucket, BucketId, Column, Descriptor, LogFormat, Path, Schema, SchemaId};
use mink_types::DataType;
use tokio::time::Instant;
use tonic::transport::Channel;

fn config(dir: &path::Path) -> ServerConfig {
    ServerConfig {
        node_id: 1,
        cluster_id: "runtime-test".into(),
        meta_url: format!("sqlite:{}", dir.join("meta.db").display()),
        storage_uri: format!("-2@file://{}", dir.join("objects").display()),
        wal_uri: None,
        data_dir: dir.join("data"),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise: "grpc://127.0.0.1:0".into(),
        lease_ttl: Duration::from_secs(2),
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

fn keys(batches: &[Bytes]) -> Vec<i64> {
    let codec = codec(LogFormat::Arrow, Compression::None);
    let arrow = Arc::new(arrow_schema::Schema::from(schema().fields()));
    batches
        .iter()
        .flat_map(|bytes| {
            let batch = Batch::parse(bytes.clone()).unwrap();
            let records = batch.records(codec.as_ref(), arrow.clone(), None).unwrap();
            let k = records
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            k.values().to_vec()
        })
        .collect()
}

async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn hosted(server: &Server, bucket: Bucket) {
    let node = server.node().clone();
    wait_until("bucket hosted", || node.registry().contains(bucket)).await;
}

#[tokio::test]
async fn node_starts_leads_serves_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(config(dir.path())).await.unwrap();
    let coordinator = server.coordinator().clone();
    wait_until("lease", || coordinator.is_leader()).await;

    coordinator
        .create_database("db", None, Default::default(), false)
        .await
        .unwrap();
    let path: Path = "db.events".parse().unwrap();
    let descriptor = Descriptor::builder(schema())
        .bucket_count(1)
        .build()
        .unwrap();
    let table = coordinator
        .create_table(&path, &descriptor, false)
        .await
        .unwrap()
        .unwrap();
    let bucket = Bucket::new(table, BucketId(0));
    hosted(&server, bucket).await;

    let channel = Channel::from_shared(format!("http://{}", server.flight_addr()))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut flight = FlightServiceClient::new(channel);
    let info = flight
        .get_flight_info(FlightDescriptor::new_path(vec![
            "db".into(),
            "events".into(),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.endpoint.len(), 1);

    let service = server.service();
    service
        .append(bucket, batch(&[(1, "a"), (2, "b")]))
        .await
        .unwrap();
    let fetched = service.fetch(bucket, 0, usize::MAX, None).await.unwrap();
    assert_eq!(keys(&fetched.batches), vec![1, 2]);

    let health = server.health();
    assert!(health.registered && health.coordinator);
    assert_eq!(health.hosted_buckets, 1);
    let admin = mink_client::Cluster::connect(format!("grpc://{}", server.flight_addr()))
        .unwrap()
        .admin();
    let remote = admin.health(None).await.unwrap();
    assert_eq!(remote.node_epoch, health.node_epoch);
    assert_eq!(remote.hosted_buckets, 1);
    let config_entries = admin.get_config(None).await.unwrap().entries;
    assert_eq!(config_entries["cluster_id"], "runtime-test");
    assert_eq!(config_entries["lease_ttl"], "2s");
    assert!(config_entries["meta_url"].starts_with("sqlite:"));
    assert_eq!(config_entries["wal_uri"], config(dir.path()).wal_uri());
    let stats = server.stats().await;
    assert_eq!(stats.buckets.len(), 1);
    assert_eq!(stats.buckets[0].log_end_offset, 2);
    let described = admin.describe_cluster().await.unwrap();
    assert_eq!(described.nodes.len(), 1);
    assert!(described.nodes[0].live, "heartbeat counts us as alive");
    assert_eq!(described.nodes[0].leading, 1);
    server.shutdown().await;

    let server = start(config(dir.path())).await.unwrap();
    hosted(&server, bucket).await;
    let service = server.service();
    assert_eq!(service.table(&path).unwrap().table_id, table);
    let fetched = service.fetch(bucket, 0, usize::MAX, None).await.unwrap();
    assert_eq!(keys(&fetched.batches), vec![1, 2]);
    service.append(bucket, batch(&[(3, "c")])).await.unwrap();
    assert_eq!(
        service
            .list_offset(bucket, mink_server::OffsetSpec::Latest)
            .await
            .unwrap(),
        3
    );
    server.shutdown().await;
}
