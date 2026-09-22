//! Decodes a request frame, checks the api key and version against what is implemented, runs the
//! handler and encodes the response with the right header version.

use bytes::{Bytes, BytesMut};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, CreateTopicsRequest, DeleteGroupsRequest, DeleteTopicsRequest,
    DescribeClusterRequest, DescribeConfigsRequest, DescribeGroupsRequest, FetchRequest,
    FindCoordinatorRequest, HeartbeatRequest, InitProducerIdRequest, JoinGroupRequest,
    LeaveGroupRequest, ListGroupsRequest, ListOffsetsRequest, MetadataRequest, OffsetCommitRequest,
    OffsetFetchRequest, ProduceRequest, RequestHeader, ResponseHeader, SyncGroupRequest,
};
use kafka_protocol::protocol::{
    Decodable, Encodable, HeaderVersion, decode_request_header_from_buffer,
};

use crate::Kafka;
use crate::error::Error;

const APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 3, 9),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 7),
    (ApiKey::Metadata, 1, 12),
    (ApiKey::OffsetCommit, 2, 8),
    (ApiKey::OffsetFetch, 1, 8),
    (ApiKey::FindCoordinator, 0, 4),
    (ApiKey::JoinGroup, 0, 9),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 5),
    (ApiKey::SyncGroup, 0, 5),
    (ApiKey::DescribeGroups, 0, 5),
    (ApiKey::ListGroups, 0, 4),
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::CreateTopics, 2, 7),
    (ApiKey::DeleteTopics, 1, 6),
    (ApiKey::InitProducerId, 0, 4),
    (ApiKey::DeleteGroups, 0, 2),
    (ApiKey::DescribeConfigs, 0, 4),
    (ApiKey::DescribeCluster, 0, 1),
];

pub(crate) fn versions() -> Vec<ApiVersion> {
    APIS.iter()
        .map(|(key, min, max)| {
            let valid = key.valid_versions();
            ApiVersion::default()
                .with_api_key(*key as i16)
                .with_min_version((*min).max(valid.min))
                .with_max_version((*max).min(valid.max))
        })
        .collect()
}

fn supported(key: ApiKey, version: i16) -> bool {
    versions()
        .iter()
        .any(|v| v.api_key == key as i16 && (v.min_version..=v.max_version).contains(&version))
}

pub(crate) struct Request {
    pub version: i16,
    pub client_id: Option<String>,
}

pub(crate) async fn handle(kafka: &Kafka, frame: Bytes) -> Result<Option<Bytes>, Error> {
    let mut body = frame;
    let header = decode_request_header_from_buffer(&mut body).map_err(protocol)?;
    let key = ApiKey::try_from(header.request_api_key)
        .map_err(|_| Error::Protocol(format!("unknown api key {}", header.request_api_key)))?;
    let version = header.request_api_version;

    if key == ApiKey::ApiVersions && !supported(key, version) {
        let response = ApiVersionsResponse::default()
            .with_error_code(ResponseError::UnsupportedVersion.code())
            .with_api_keys(versions());
        return Ok(Some(encode(key, &header, 0, &response)?));
    }
    if !supported(key, version) {
        return Err(Error::UnsupportedVersion {
            key: key as i16,
            version,
        });
    }

    let request = Request {
        version,
        client_id: header.client_id.as_ref().map(|id| id.to_string()),
    };

    macro_rules! call {
        ($ty:ty, $method:ident) => {{
            let decoded = <$ty>::decode(&mut body, version).map_err(protocol)?;
            let response = kafka.$method(&request, decoded).await?;
            Some(encode(key, &header, version, &response)?)
        }};
    }

    Ok(match key {
        ApiKey::ApiVersions => Some(encode(
            key,
            &header,
            version,
            &ApiVersionsResponse::default().with_api_keys(versions()),
        )?),
        ApiKey::Metadata => call!(MetadataRequest, metadata),
        ApiKey::DescribeCluster => call!(DescribeClusterRequest, describe_cluster),
        ApiKey::FindCoordinator => call!(FindCoordinatorRequest, find_coordinator),
        ApiKey::CreateTopics => call!(CreateTopicsRequest, create_topics),
        ApiKey::DeleteTopics => call!(DeleteTopicsRequest, delete_topics),
        ApiKey::DescribeConfigs => call!(DescribeConfigsRequest, describe_configs),
        ApiKey::InitProducerId => call!(InitProducerIdRequest, init_producer_id),
        ApiKey::Produce => {
            let decoded = ProduceRequest::decode(&mut body, version).map_err(protocol)?;
            let acks = decoded.acks;
            let response = kafka.produce(&request, decoded).await?;
            (acks != 0)
                .then(|| encode(key, &header, version, &response))
                .transpose()?
        }
        ApiKey::Fetch => call!(FetchRequest, fetch),
        ApiKey::ListOffsets => call!(ListOffsetsRequest, list_offsets),
        ApiKey::JoinGroup => call!(JoinGroupRequest, join_group),
        ApiKey::SyncGroup => call!(SyncGroupRequest, sync_group),
        ApiKey::Heartbeat => call!(HeartbeatRequest, heartbeat),
        ApiKey::LeaveGroup => call!(LeaveGroupRequest, leave_group),
        ApiKey::DescribeGroups => call!(DescribeGroupsRequest, describe_groups),
        ApiKey::ListGroups => call!(ListGroupsRequest, list_groups),
        ApiKey::DeleteGroups => call!(DeleteGroupsRequest, delete_groups),
        ApiKey::OffsetCommit => call!(OffsetCommitRequest, offset_commit),
        ApiKey::OffsetFetch => call!(OffsetFetchRequest, offset_fetch),
        other => {
            return Err(Error::UnsupportedVersion {
                key: other as i16,
                version,
            });
        }
    })
}

fn encode<T: Encodable + HeaderVersion>(
    key: ApiKey,
    header: &RequestHeader,
    version: i16,
    body: &T,
) -> Result<Bytes, Error> {
    let mut buf = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(header.correlation_id)
        .encode(&mut buf, key.response_header_version(version))
        .map_err(protocol)?;
    body.encode(&mut buf, version).map_err(protocol)?;

    Ok(buf.freeze())
}

fn protocol(error: anyhow::Error) -> Error {
    Error::Protocol(error.to_string())
}
