use std::time::Duration;

use anyhow::{Context, ensure};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{
    BrokerId, FetchRequest, ListOffsetsRequest, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{
    Decodable, HeaderVersion, Request, StrBytes, encode_request_header_into_buffer,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const FETCH_VERSION: i16 = 12;
const LIST_OFFSETS_VERSION: i16 = 7;
const READ_COMMITTED: i8 = 1;
const LATEST: i64 = -1;
const NO_REPLICA: BrokerId = BrokerId(-1);
const NO_LEADER_EPOCH: i32 = -1;
const FINAL_SESSION_EPOCH: i32 = -1;
const MAX_WAIT: Duration = Duration::from_millis(500);
const MAX_BYTES: i32 = 8 * 1024 * 1024;

pub struct Broker {
    stream: TcpStream,
    correlation: i32,
}

impl Broker {
    pub async fn connect(address: &str) -> anyhow::Result<Broker> {
        Ok(Broker {
            stream: TcpStream::connect(address).await?,
            correlation: 0,
        })
    }

    pub async fn last_stable_offset(&mut self, topic: &str) -> anyhow::Result<i64> {
        let request = ListOffsetsRequest::default()
            .with_replica_id(NO_REPLICA)
            .with_isolation_level(READ_COMMITTED)
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(name(topic))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_current_leader_epoch(NO_LEADER_EPOCH)
                            .with_timestamp(LATEST),
                    ]),
            ]);

        let mut response = self.call(&request, LIST_OFFSETS_VERSION).await?;

        let partition = response
            .topics
            .pop()
            .and_then(|mut topic| topic.partitions.pop())
            .context("list offsets response has no partition")?;
        ensure!(
            partition.error_code == 0,
            "list offsets error {}",
            partition.error_code
        );
        Ok(partition.offset)
    }

    pub async fn fetch(&mut self, topic: &str, offset: i64) -> anyhow::Result<PartitionData> {
        let request = FetchRequest::default()
            .with_replica_id(NO_REPLICA)
            .with_max_wait_ms(i32::try_from(MAX_WAIT.as_millis())?)
            .with_min_bytes(1)
            .with_max_bytes(MAX_BYTES)
            .with_isolation_level(READ_COMMITTED)
            .with_session_epoch(FINAL_SESSION_EPOCH)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(name(topic))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_current_leader_epoch(NO_LEADER_EPOCH)
                            .with_fetch_offset(offset)
                            .with_last_fetched_epoch(NO_LEADER_EPOCH)
                            .with_log_start_offset(-1)
                            .with_partition_max_bytes(MAX_BYTES),
                    ]),
            ]);

        let mut response = self.call(&request, FETCH_VERSION).await?;

        let partition = response
            .responses
            .pop()
            .and_then(|mut topic| topic.partitions.pop())
            .context("fetch response has no partition")?;
        ensure!(
            partition.error_code == 0,
            "fetch error {}",
            partition.error_code
        );
        Ok(partition)
    }

    async fn call<R: Request>(&mut self, request: &R, version: i16) -> anyhow::Result<R::Response> {
        let correlation = self.correlation;
        self.correlation += 1;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation)
            .with_client_id(Some(StrBytes::from_static_str("mink-conformance")));
        let mut frame = BytesMut::new();
        encode_request_header_into_buffer(&mut frame, &header)?;
        request.encode(&mut frame, version)?;

        self.stream.write_i32(i32::try_from(frame.len())?).await?;
        self.stream.write_all(&frame).await?;
        let length = usize::try_from(self.stream.read_i32().await?)?;
        let mut body = vec![0; length];
        self.stream.read_exact(&mut body).await?;

        let mut body = Bytes::from(body);
        let header_version = <R::Response as HeaderVersion>::header_version(version);
        let header = ResponseHeader::decode(&mut body, header_version)?;
        ensure!(
            header.correlation_id == correlation,
            "response correlation id {} does not match request {correlation}",
            header.correlation_id
        );
        R::Response::decode(&mut body, version)
    }
}

fn name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_owned()))
}
