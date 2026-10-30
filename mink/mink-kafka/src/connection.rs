use std::ops::RangeInclusive;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, ListOffsetsRequest,
    MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, TopicName,
};
use kafka_protocol::protocol::{
    Decodable, HeaderVersion, Request, StrBytes, encode_request_header_into_buffer,
};
use mink_core::Partition;
use mink_metrics::{Metrics, Outcome};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::Error;
use crate::codes::{NONE, UNSUPPORTED_SASL_MECHANISM, check};
use crate::sasl::Mechanism;

const CLIENT_ID: &str = "mink";
pub(crate) const MAX_FRAME: i32 = 256 * 1024 * 1024;

const API_VERSIONS: i16 = 3;
const METADATA: RangeInclusive<i16> = 10..=13;
const LIST_OFFSETS: RangeInclusive<i16> = 2..=10;
const FETCH: RangeInclusive<i16> = 4..=12;
const SASL_HANDSHAKE: RangeInclusive<i16> = 1..=1;
const SASL_AUTHENTICATE: RangeInclusive<i16> = 0..=2;

const NO_REPLICA: BrokerId = BrokerId(-1);
const NO_LEADER_EPOCH: i32 = -1;
const NO_LOG_START_OFFSET: i64 = -1;
const NO_FETCH_SESSION: i32 = 0;
const FINAL_SESSION_EPOCH: i32 = -1;
const READ_COMMITTED: i8 = 1;
const MIN_BYTES: i32 = 1;

pub(crate) const EARLIEST: i64 = -2;
pub(crate) const LATEST: i64 = -1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ApiVersions {
    metadata: i16,
    list_offsets: i16,
    fetch: i16,
    sasl_handshake: Option<i16>,
    sasl_authenticate: Option<i16>,
}

pub(crate) trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}

pub(crate) struct Connection {
    stream: Box<dyn Transport>,
    correlation: i32,
    versions: ApiVersions,
    timeout: Duration,
    closed: bool,
    expires: Option<Instant>,
    metrics: Metrics,
}

impl ApiVersions {
    fn negotiate(response: &ApiVersionsResponse) -> Result<ApiVersions, Error> {
        check::<ApiVersionsRequest>(response.error_code)?;

        let highest = |key: ApiKey, ours: RangeInclusive<i16>| {
            response
                .api_keys
                .iter()
                .find(|advertised| advertised.api_key == key as i16)
                .and_then(|advertised| {
                    let version = (*ours.end()).min(advertised.max_version);

                    (ours.contains(&version) && version >= advertised.min_version)
                        .then_some(version)
                })
                .ok_or(Error::UnsupportedVersion { key: key as i16 })
        };

        Ok(ApiVersions {
            metadata: highest(ApiKey::Metadata, METADATA)?,
            list_offsets: highest(ApiKey::ListOffsets, LIST_OFFSETS)?,
            fetch: highest(ApiKey::Fetch, FETCH)?,
            sasl_handshake: highest(ApiKey::SaslHandshake, SASL_HANDSHAKE).ok(),
            sasl_authenticate: highest(ApiKey::SaslAuthenticate, SASL_AUTHENTICATE).ok(),
        })
    }
}

impl Connection {
    pub(crate) async fn open(
        mut stream: Box<dyn Transport>,
        timeout: Duration,
        metrics: Metrics,
    ) -> Result<Connection, Error> {
        let request = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str(CLIENT_ID))
            .with_client_software_version(StrBytes::from_static_str(env!("CARGO_PKG_VERSION")));

        let exchange = exchange(&mut *stream, 0, &request, API_VERSIONS);
        let response = tokio::time::timeout(timeout, exchange)
            .await
            .map_err(|_| Error::Timeout)??;
        let versions = ApiVersions::negotiate(&response)?;

        Ok(Connection {
            stream,
            correlation: 1,
            versions,
            timeout,
            closed: false,
            expires: None,
            metrics,
        })
    }

    pub(crate) fn expire_at(&mut self, expires: Instant) {
        self.expires = Some(expires);
    }

    pub(crate) fn closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn expired(&self) -> bool {
        self.expires
            .is_some_and(|expires| Instant::now() >= expires)
    }

    pub(crate) async fn sasl_handshake(&mut self, mechanism: Mechanism) -> Result<(), Error> {
        let version = self
            .versions
            .sasl_handshake
            .ok_or(Error::UnsupportedVersion {
                key: ApiKey::SaslHandshake as i16,
            })?;
        let request = SaslHandshakeRequest::default()
            .with_mechanism(StrBytes::from_static_str(mechanism.name()));

        let response = self.call(&request, version).await?;

        match response.error_code {
            UNSUPPORTED_SASL_MECHANISM => {
                let enabled: Vec<_> = response
                    .mechanisms
                    .iter()
                    .map(|name| name.as_str())
                    .collect();

                Err(Error::Authentication(format!(
                    "the broker does not enable {}, only {}",
                    mechanism.name(),
                    enabled.join(", ")
                )))
            }
            code => check::<SaslHandshakeRequest>(code),
        }
    }

    pub(crate) async fn sasl_authenticate(
        &mut self,
        bytes: Vec<u8>,
    ) -> Result<SaslAuthenticateResponse, Error> {
        let version = self
            .versions
            .sasl_authenticate
            .ok_or(Error::UnsupportedVersion {
                key: ApiKey::SaslAuthenticate as i16,
            })?;
        let request = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(bytes));

        let response = self.call(&request, version).await?;

        match response.error_code {
            NONE => Ok(response),
            code => Err(Error::Authentication(
                response.error_message.as_ref().map_or_else(
                    || format!("broker error code {code}"),
                    |message| message.to_string(),
                ),
            )),
        }
    }

    pub(crate) async fn metadata(&mut self) -> Result<MetadataResponse, Error> {
        let request = MetadataRequest::default()
            .with_topics(None)
            .with_allow_auto_topic_creation(false);

        let response = self.call(&request, self.versions.metadata).await;

        self.counted(mink_metrics::Request::Metadata, response)
    }

    pub(crate) async fn offset(
        &mut self,
        partition: &Partition,
        timestamp: i64,
    ) -> Result<i64, Error> {
        let offset = self.list_offset(partition, timestamp).await;

        self.counted(mink_metrics::Request::Offsets, offset)
    }

    pub(crate) async fn fetch(
        &mut self,
        partition: &Partition,
        offset: i64,
        max_wait_ms: i32,
        max_bytes: i32,
    ) -> Result<PartitionData, Error> {
        let fetched = self
            .fetch_partition(partition, offset, max_wait_ms, max_bytes)
            .await;

        if let Ok(data) = &fetched {
            let bytes = data.records.as_ref().map_or(0, |records| records.len());
            self.metrics.fetched(bytes as u64);
        }
        self.counted(mink_metrics::Request::Fetch, fetched)
    }

    fn counted<T>(
        &self,
        request: mink_metrics::Request,
        result: Result<T, Error>,
    ) -> Result<T, Error> {
        self.metrics.kafka_request(request, Outcome::of(&result));

        result
    }

    async fn list_offset(&mut self, partition: &Partition, timestamp: i64) -> Result<i64, Error> {
        let request = ListOffsetsRequest::default()
            .with_replica_id(NO_REPLICA)
            .with_isolation_level(READ_COMMITTED)
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(topic_name(partition))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(partition.index())
                            .with_current_leader_epoch(NO_LEADER_EPOCH)
                            .with_timestamp(timestamp),
                    ]),
            ]);

        let mut response = self.call(&request, self.versions.list_offsets).await?;

        let data = response
            .topics
            .pop()
            .and_then(|mut topic| topic.partitions.pop())
            .ok_or_else(|| Error::MissingPartition(partition.clone()))?;
        check::<ListOffsetsRequest>(data.error_code)?;

        Ok(data.offset)
    }

    async fn fetch_partition(
        &mut self,
        partition: &Partition,
        offset: i64,
        max_wait_ms: i32,
        max_bytes: i32,
    ) -> Result<PartitionData, Error> {
        let request = FetchRequest::default()
            .with_replica_id(NO_REPLICA)
            .with_max_wait_ms(max_wait_ms)
            .with_min_bytes(MIN_BYTES)
            .with_max_bytes(max_bytes)
            .with_isolation_level(READ_COMMITTED)
            .with_session_id(NO_FETCH_SESSION)
            .with_session_epoch(FINAL_SESSION_EPOCH)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(topic_name(partition))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(partition.index())
                            .with_current_leader_epoch(NO_LEADER_EPOCH)
                            .with_fetch_offset(offset)
                            .with_last_fetched_epoch(NO_LEADER_EPOCH)
                            .with_log_start_offset(NO_LOG_START_OFFSET)
                            .with_partition_max_bytes(max_bytes),
                    ]),
            ]);

        let mut response = self.call(&request, self.versions.fetch).await?;

        let data = response
            .responses
            .pop()
            .and_then(|mut topic| topic.partitions.pop())
            .ok_or_else(|| Error::MissingPartition(partition.clone()))?;
        check::<FetchRequest>(data.error_code)?;

        Ok(data)
    }

    async fn call<R: Request>(&mut self, request: &R, version: i16) -> Result<R::Response, Error> {
        if self.closed {
            return Err(Error::Closed);
        }

        let correlation = self.correlation;
        self.correlation = self.correlation.wrapping_add(1);

        self.closed = true;
        let exchange = exchange(&mut *self.stream, correlation, request, version);
        let response = match tokio::time::timeout(self.timeout, exchange).await {
            Ok(response) => response,
            Err(_) => Err(Error::Timeout),
        };
        self.closed = response.is_err();
        if self.closed {
            let _ = self.stream.shutdown().await;
        }

        response
    }
}

async fn exchange<R: Request>(
    stream: &mut dyn Transport,
    correlation: i32,
    request: &R,
    version: i16,
) -> Result<R::Response, Error> {
    let header = RequestHeader::default()
        .with_request_api_key(R::KEY)
        .with_request_api_version(version)
        .with_correlation_id(correlation)
        .with_client_id(Some(StrBytes::from_static_str(CLIENT_ID)));
    let mut frame = BytesMut::new();
    encode_request_header_into_buffer(&mut frame, &header).map_err(Error::Encode)?;
    request.encode(&mut frame, version).map_err(Error::Encode)?;
    let length = i32::try_from(frame.len())
        .ok()
        .filter(|length| *length <= MAX_FRAME)
        .ok_or(Error::RequestTooLarge {
            length: frame.len(),
        })?;

    stream.write_i32(length).await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let length = stream.read_i32().await?;
    if !(0..=MAX_FRAME).contains(&length) {
        return Err(Error::FrameSize {
            length: i64::from(length),
        });
    }

    let mut body = vec![0; length as usize];
    stream.read_exact(&mut body).await?;
    let mut body = Bytes::from(body);

    let header_version = <R::Response as HeaderVersion>::header_version(version);
    let response = ResponseHeader::decode(&mut body, header_version).map_err(Error::Decode)?;
    if response.correlation_id != correlation {
        return Err(Error::Correlation {
            expected: correlation,
            actual: response.correlation_id,
        });
    }

    R::Response::decode(&mut body, version).map_err(Error::Decode)
}

fn topic_name(partition: &Partition) -> TopicName {
    TopicName(StrBytes::from_string(partition.topic().to_owned()))
}

#[cfg(test)]
pub(crate) mod tests {
    use kafka_protocol::messages::api_versions_response::ApiVersion;
    use kafka_protocol::protocol::Encodable;
    use tokio::net::TcpListener;

    use super::*;
    use crate::connector::tests::plaintext;

    pub(crate) fn advertised(ranges: &[(ApiKey, i16, i16)]) -> ApiVersionsResponse {
        ApiVersionsResponse::default().with_api_keys(
            ranges
                .iter()
                .map(|(key, min, max)| {
                    ApiVersion::default()
                        .with_api_key(*key as i16)
                        .with_min_version(*min)
                        .with_max_version(*max)
                })
                .collect(),
        )
    }

    pub(crate) fn supported() -> ApiVersionsResponse {
        advertised(&[
            (ApiKey::Metadata, 0, 13),
            (ApiKey::ListOffsets, 1, 10),
            (ApiKey::Fetch, 4, 18),
            (ApiKey::SaslHandshake, 0, 1),
            (ApiKey::SaslAuthenticate, 0, 2),
        ])
    }

    pub(crate) fn frame(
        correlation: i32,
        key: ApiKey,
        version: i16,
        response: &impl Encodable,
    ) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.extend_from_slice(&correlation.to_be_bytes());
        if key.response_header_version(version) >= 1 {
            body.extend_from_slice(&[0]);
        }

        response.encode(&mut body, version).unwrap();

        body.to_vec()
    }

    pub(crate) const TIMEOUT: Duration = Duration::from_secs(1);

    pub(crate) async fn broker(
        reply: impl Fn(ApiKey, i16, i32, Bytes) -> Option<Vec<u8>> + Send + 'static,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                while let Ok(length) = stream.read_i32().await {
                    let mut request = vec![0; length as usize];
                    if stream.read_exact(&mut request).await.is_err() {
                        break;
                    }

                    let key =
                        ApiKey::try_from(i16::from_be_bytes([request[0], request[1]])).unwrap();
                    let version = i16::from_be_bytes([request[2], request[3]]);
                    let correlation = i32::from_be_bytes(request[4..8].try_into().unwrap());

                    let body = match key {
                        ApiKey::ApiVersions => frame(correlation, key, version, &supported()),
                        key => match reply(key, version, correlation, Bytes::from(request)) {
                            Some(body) => body,
                            None => continue,
                        },
                    };

                    if stream.write_i32(body.len() as i32).await.is_err() {
                        break;
                    }

                    let _ = stream.write_all(&body).await;
                }
            }
        });

        address
    }

    #[test]
    fn versions_are_the_highest_both_sides_support() {
        let older = advertised(&[
            (ApiKey::Metadata, 0, 11),
            (ApiKey::ListOffsets, 1, 6),
            (ApiKey::Fetch, 4, 11),
        ]);

        let newer = ApiVersions::negotiate(&supported()).unwrap();
        let older = ApiVersions::negotiate(&older).unwrap();

        assert_eq!(
            (newer.metadata, newer.list_offsets, newer.fetch),
            (13, 10, 12)
        );
        assert_eq!(
            (older.metadata, older.list_offsets, older.fetch),
            (11, 6, 11)
        );
    }

    #[test]
    fn brokers_without_a_supported_version_are_rejected() {
        let no_topic_ids = advertised(&[
            (ApiKey::Metadata, 0, 9),
            (ApiKey::ListOffsets, 1, 10),
            (ApiKey::Fetch, 4, 18),
        ]);
        let no_fetch = advertised(&[(ApiKey::Metadata, 0, 13), (ApiKey::ListOffsets, 1, 10)]);
        let fetch_too_new = advertised(&[
            (ApiKey::Metadata, 0, 13),
            (ApiKey::ListOffsets, 1, 10),
            (ApiKey::Fetch, 13, 18),
        ]);

        for response in [no_topic_ids, no_fetch, fetch_too_new] {
            let negotiated = ApiVersions::negotiate(&response);

            assert!(
                matches!(negotiated, Err(Error::UnsupportedVersion { .. })),
                "{negotiated:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_mismatched_correlation_id_closes_the_connection() {
        let address = broker(|key, version, correlation, _| {
            Some(frame(
                correlation + 1,
                key,
                version,
                &MetadataResponse::default(),
            ))
        })
        .await;
        let mut connection = plaintext(TIMEOUT)
            .connect(&address, Metrics::new())
            .await
            .unwrap();

        let first = connection.metadata().await;
        let second = connection.metadata().await;

        assert!(
            matches!(
                first,
                Err(Error::Correlation {
                    expected: 1,
                    actual: 2
                })
            ),
            "{first:?}"
        );
        assert!(connection.closed());
        assert!(matches!(second, Err(Error::Closed)), "{second:?}");
    }

    #[tokio::test]
    async fn a_call_cancelled_mid_exchange_closes_the_connection() {
        let address = broker(|_, _, _, _| None).await;
        let mut connection = plaintext(TIMEOUT)
            .connect(&address, Metrics::new())
            .await
            .unwrap();

        let cancelled =
            tokio::time::timeout(Duration::from_millis(50), connection.metadata()).await;
        let next = connection.metadata().await;

        assert!(cancelled.is_err());
        assert!(connection.closed());
        assert!(matches!(next, Err(Error::Closed)), "{next:?}");
    }

    #[tokio::test]
    async fn a_broker_that_never_answers_times_out_and_closes_the_connection() {
        let address = broker(|_, _, _, _| None).await;
        let mut connection = plaintext(Duration::from_millis(100))
            .connect(&address, Metrics::new())
            .await
            .unwrap();

        let metadata = connection.metadata().await;

        assert!(matches!(metadata, Err(Error::Timeout)), "{metadata:?}");
        assert!(connection.closed());
    }
}
