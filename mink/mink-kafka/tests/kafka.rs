//! End-to-end Kafka wire protocol against a real node: versions, topics, produce/fetch,
//! list offsets, idempotent produce, and the classic group coordinator.

use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse, GroupId,
    HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest, InitProducerIdResponse,
    JoinGroupRequest, JoinGroupResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
    SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::protocol::{
    Decodable, Encodable, HeaderVersion, encode_request_header_into_buffer,
};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use mink_kafka::Config;
use mink_runtime::{Server, ServerConfig, start};
use mink_table::{Bucket, BucketId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

const WAIT: Duration = Duration::from_secs(20);

fn reserve() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(dir: &Path, node_id: i32) -> ServerConfig {
    let port = reserve();
    ServerConfig {
        node_id,
        cluster_id: "kafka-test".into(),
        meta_url: format!("sqlite:{}", dir.join("meta.db").display()),
        storage_uri: format!("-2@file://{}", dir.join("objects").display()),
        wal_uri: None,
        data_dir: dir.join(format!("n{node_id}")),
        listen: format!("127.0.0.1:{port}").parse().unwrap(),
        advertise: format!("grpc://127.0.0.1:{port}"),
        lease_ttl: Duration::from_secs(2),
        coordinator_tick: Duration::from_millis(200),
        default_bucket_count: 1,
        kafka: Some(Config {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            advertise: None,
            auto_create_topics: true,
            default_partitions: 1,
            min_session_timeout: Duration::from_millis(100),
            ..Config::default()
        }),
        ..ServerConfig::default()
    }
}

async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !ready() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn node(dir: &Path, node_id: i32) -> Server {
    let server = start(config(dir, node_id)).await.unwrap();
    let coordinator = server.coordinator().clone();
    wait_until("coordinator", move || coordinator.is_leader()).await;
    let node = server.node().clone();
    wait_until("kafka advertised", move || {
        node.metadata()
            .views()
            .load()
            .state
            .get_node_protocol_address(node_id, mink_kafka::PROTOCOL)
            .is_some_and(|a| !a.ends_with(":0"))
    })
    .await;
    server
}

fn str(s: &str) -> StrBytes {
    StrBytes::from(s.to_owned())
}

fn topic(name: &str) -> TopicName {
    TopicName(str(name))
}

fn group(name: &str) -> GroupId {
    GroupId(str(name))
}

fn record(offset: i64, key: &str, value: &str) -> Record {
    Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset,
        sequence: offset as i32,
        timestamp: 1_700_000_000_000 + offset,
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        headers: Default::default(),
    }
}

fn encode_records(records: &[Record]) -> Bytes {
    let mut out = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut out,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    out.freeze()
}

struct Client {
    stream: TcpStream,
    correlation: i32,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        Client {
            stream,
            correlation: 0,
        }
    }

    async fn call<Req, Resp>(&mut self, key: ApiKey, version: i16, body: Req) -> Resp
    where
        Req: Encodable + HeaderVersion,
        Resp: Decodable + HeaderVersion,
    {
        self.correlation += 1;
        let header = RequestHeader::default()
            .with_request_api_key(key as i16)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation)
            .with_client_id(Some(str("test")));
        let mut buf = BytesMut::new();
        encode_request_header_into_buffer(&mut buf, &header).unwrap();
        body.encode(&mut buf, version).unwrap();

        let size = i32::try_from(buf.len()).unwrap();
        self.stream.write_all(&size.to_be_bytes()).await.unwrap();
        self.stream.write_all(&buf).await.unwrap();
        self.stream.flush().await.unwrap();

        let mut prefix = [0u8; 4];
        self.stream.read_exact(&mut prefix).await.unwrap();
        let len = i32::from_be_bytes(prefix) as usize;
        let mut body = BytesMut::zeroed(len);
        self.stream.read_exact(&mut body).await.unwrap();
        let header_version = key.response_header_version(version);
        let response_header = ResponseHeader::decode(&mut body, header_version).unwrap();
        assert_eq!(response_header.correlation_id, self.correlation);
        Resp::decode(&mut body, version).unwrap()
    }
}

async fn create(client: &mut Client, name: &str, partitions: i32) {
    let created: CreateTopicsResponse = client
        .call(
            ApiKey::CreateTopics,
            7,
            CreateTopicsRequest::default()
                .with_timeout_ms(10_000)
                .with_topics(vec![
                    CreatableTopic::default()
                        .with_name(topic(name))
                        .with_num_partitions(partitions)
                        .with_replication_factor(1),
                ]),
        )
        .await;
    assert_eq!(created.topics[0].error_code, 0, "{created:?}");
}

async fn produce(client: &mut Client, name: &str, partition: i32, records: &[Record]) -> i64 {
    let produced: ProduceResponse = client
        .call(
            ApiKey::Produce,
            7,
            ProduceRequest::default()
                .with_acks(1)
                .with_timeout_ms(10_000)
                .with_topic_data(vec![
                    TopicProduceData::default()
                        .with_name(topic(name))
                        .with_partition_data(vec![
                            PartitionProduceData::default()
                                .with_index(partition)
                                .with_records(Some(encode_records(records))),
                        ]),
                ]),
        )
        .await;
    let part = &produced.responses[0].partition_responses[0];
    assert_eq!(part.error_code, 0, "{part:?}");
    part.base_offset
}

async fn fetch(client: &mut Client, name: &str, partition: i32, offset: i64) -> FetchResponse {
    client
        .call(
            ApiKey::Fetch,
            11,
            FetchRequest::default()
                .with_max_wait_ms(2_000)
                .with_min_bytes(1)
                .with_max_bytes(1024 * 1024)
                .with_topics(vec![
                    FetchTopic::default()
                        .with_topic(topic(name))
                        .with_partitions(vec![
                            FetchPartition::default()
                                .with_partition(partition)
                                .with_fetch_offset(offset)
                                .with_partition_max_bytes(1024 * 1024),
                        ]),
                ]),
        )
        .await
}

fn hosts(server: &Server, name: &str, partitions: u32) -> bool {
    let node = server.node();
    let view = node.metadata().views().load();
    let Some(table) = view
        .state
        .catalog
        .tables
        .iter()
        .find(|(p, _)| p.table().as_str() == name)
        .map(|(_, t)| t)
    else {
        return false;
    };
    (0..partitions).all(|p| {
        node.registry()
            .contains(Bucket::new(table.table_id, BucketId(p)))
    })
}

async fn hosted(server: &Server, name: &str, partitions: u32) {
    wait_until("topic hosted", || hosts(server, name, partitions)).await;
}

/// Waits until one of `servers` hosts every partition of the topic and returns its index.
async fn leader_among(servers: &[&Server], name: &str, partitions: u32) -> usize {
    let mut leader = None;
    wait_until("topic hosted", || {
        leader = servers.iter().position(|s| hosts(s, name, partitions));
        leader.is_some()
    })
    .await;
    leader.unwrap()
}

#[tokio::test]
async fn api_versions_and_metadata_create_produce_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let server = node(dir.path(), 1).await;
    let addr = server.kafka_addr().unwrap();
    let mut client = Client::connect(addr).await;

    let versions: ApiVersionsResponse = client
        .call(ApiKey::ApiVersions, 3, ApiVersionsRequest::default())
        .await;
    assert_eq!(versions.error_code, 0);
    assert!(
        versions
            .api_keys
            .iter()
            .any(|v| v.api_key == ApiKey::Produce as i16)
    );

    create(&mut client, "orders", 1).await;
    hosted(&server, "orders", 1).await;

    let meta: MetadataResponse = client
        .call(
            ApiKey::Metadata,
            12,
            MetadataRequest::default().with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(topic("orders"))),
            ])),
        )
        .await;
    assert_eq!(meta.topics[0].error_code, 0);
    assert_eq!(meta.topics[0].partitions.len(), 1);
    assert_eq!(meta.brokers.len(), 1);
    assert_eq!(meta.controller_id.0, 1);

    let offset = produce(
        &mut client,
        "orders",
        0,
        &[record(0, "k1", "v1"), record(1, "k2", "v2")],
    )
    .await;
    assert_eq!(offset, 0);

    let fetched = fetch(&mut client, "orders", 0, 0).await;
    let part = &fetched.responses[0].partitions[0];
    assert_eq!(part.error_code, 0, "{part:?}");
    assert_eq!(part.high_watermark, 2);
    let mut records = part.records.clone().unwrap();
    let sets = RecordBatchDecoder::decode_all(&mut records).unwrap();
    assert_eq!(sets[0].records.len(), 2);
    assert_eq!(sets[0].records[0].value.as_deref(), Some(b"v1".as_slice()));
    assert_eq!(sets[0].records[1].key.as_deref(), Some(b"k2".as_slice()));

    let listed: ListOffsetsResponse = client
        .call(
            ApiKey::ListOffsets,
            5,
            ListOffsetsRequest::default().with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(topic("orders"))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_timestamp(-2),
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_timestamp(-1),
                    ]),
            ]),
        )
        .await;
    assert_eq!(listed.topics[0].partitions[0].offset, 0);
    assert_eq!(listed.topics[0].partitions[1].offset, 2);

    server.shutdown().await;
}

#[tokio::test]
async fn idempotent_duplicate_returns_the_original_offset() {
    let dir = tempfile::tempdir().unwrap();
    let server = node(dir.path(), 1).await;
    let mut client = Client::connect(server.kafka_addr().unwrap()).await;
    create(&mut client, "ids", 1).await;
    hosted(&server, "ids", 1).await;

    let init: InitProducerIdResponse = client
        .call(
            ApiKey::InitProducerId,
            4,
            InitProducerIdRequest::default().with_transaction_timeout_ms(10_000),
        )
        .await;
    assert_eq!(init.error_code, 0);
    assert!(init.producer_id.0 >= 0);

    let mut first = record(0, "k", "v");
    first.producer_id = init.producer_id.0;
    first.producer_epoch = 0;
    first.sequence = 0;
    let offset = produce(&mut client, "ids", 0, &[first.clone()]).await;
    assert_eq!(produce(&mut client, "ids", 0, &[first]).await, offset);

    server.shutdown().await;
}

#[tokio::test]
async fn classic_group_join_sync_commit_and_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let server = node(dir.path(), 1).await;
    let mut client = Client::connect(server.kafka_addr().unwrap()).await;
    create(&mut client, "events", 1).await;
    hosted(&server, "events", 1).await;
    produce(&mut client, "events", 0, &[record(0, "a", "1")]).await;

    let found: FindCoordinatorResponse = client
        .call(
            ApiKey::FindCoordinator,
            0,
            FindCoordinatorRequest::default().with_key(str("g1")),
        )
        .await;
    assert_eq!(found.error_code, 0);
    assert_eq!(found.node_id.0, 1);

    let first: JoinGroupResponse = client
        .call(
            ApiKey::JoinGroup,
            4,
            JoinGroupRequest::default()
                .with_group_id(group("g1"))
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(5_000)
                .with_protocol_type(str("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(str("range"))
                        .with_metadata(Bytes::from_static(b"meta")),
                ]),
        )
        .await;
    assert_eq!(first.error_code, ResponseError::MemberIdRequired.code());
    let member = first.member_id;

    let joined: JoinGroupResponse = client
        .call(
            ApiKey::JoinGroup,
            4,
            JoinGroupRequest::default()
                .with_group_id(group("g1"))
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(5_000)
                .with_member_id(member.clone())
                .with_protocol_type(str("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(str("range"))
                        .with_metadata(Bytes::from_static(b"meta")),
                ]),
        )
        .await;
    assert_eq!(joined.error_code, 0, "{joined:?}");
    assert_eq!(joined.generation_id, 1);
    assert_eq!(joined.leader, member);
    assert_eq!(joined.members.len(), 1);

    let synced: SyncGroupResponse = client
        .call(
            ApiKey::SyncGroup,
            3,
            SyncGroupRequest::default()
                .with_group_id(group("g1"))
                .with_generation_id(1)
                .with_member_id(member.clone())
                .with_assignments(vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(member.clone())
                        .with_assignment(Bytes::from_static(b"assign")),
                ]),
        )
        .await;
    assert_eq!(synced.error_code, 0, "{synced:?}");
    assert_eq!(synced.assignment.as_ref(), b"assign");

    let beat: HeartbeatResponse = client
        .call(
            ApiKey::Heartbeat,
            4,
            HeartbeatRequest::default()
                .with_group_id(group("g1"))
                .with_generation_id(1)
                .with_member_id(member.clone()),
        )
        .await;
    assert_eq!(beat.error_code, 0);

    let committed: OffsetCommitResponse = client
        .call(
            ApiKey::OffsetCommit,
            8,
            OffsetCommitRequest::default()
                .with_group_id(group("g1"))
                .with_generation_id_or_member_epoch(1)
                .with_member_id(member)
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(topic("events"))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(0)
                                .with_committed_offset(1),
                        ]),
                ]),
        )
        .await;
    assert_eq!(
        committed.topics[0].partitions[0].error_code, 0,
        "{committed:?}"
    );

    let fetched: OffsetFetchResponse = client
        .call(
            ApiKey::OffsetFetch,
            7,
            OffsetFetchRequest::default()
                .with_group_id(group("g1"))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopic::default()
                        .with_name(topic("events"))
                        .with_partition_indexes(vec![0]),
                ])),
        )
        .await;
    assert_eq!(fetched.error_code, 0);
    assert_eq!(fetched.topics[0].partitions[0].committed_offset, 1);

    server.shutdown().await;
}

#[tokio::test]
async fn produce_to_the_wrong_node_returns_not_leader() {
    let dir = tempfile::tempdir().unwrap();
    let a = node(dir.path(), 1).await;
    let b = start(config(dir.path(), 2)).await.unwrap();
    wait_until("node 2 registered", || {
        a.node()
            .metadata()
            .views()
            .load()
            .state
            .nodes
            .contains_key(&2)
    })
    .await;

    let mut admin = Client::connect(a.kafka_addr().unwrap()).await;
    create(&mut admin, "shared", 1).await;
    let nodes = [&a, &b];
    let leader = leader_among(&nodes, "shared", 1).await;
    let follower = nodes[1 - leader];

    let mut follower = Client::connect(follower.kafka_addr().unwrap()).await;
    let produced: ProduceResponse = follower
        .call(
            ApiKey::Produce,
            7,
            ProduceRequest::default()
                .with_acks(1)
                .with_timeout_ms(5_000)
                .with_topic_data(vec![
                    TopicProduceData::default()
                        .with_name(topic("shared"))
                        .with_partition_data(vec![
                            PartitionProduceData::default()
                                .with_index(0)
                                .with_records(Some(encode_records(&[record(0, "k", "v")]))),
                        ]),
                ]),
        )
        .await;
    let code = produced.responses[0].partition_responses[0].error_code;
    assert!(
        code == ResponseError::NotLeaderOrFollower.code()
            || code == ResponseError::LeaderNotAvailable.code(),
        "expected a leadership error, got {code}"
    );

    b.shutdown().await;
    a.shutdown().await;
}
