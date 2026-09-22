//! Every failure the frontend can report, and the Kafka error code each one is sent as.

use std::io;

use kafka_protocol::ResponseError;
use mink_metadata::Error as MetadataError;
use mink_table::Bucket;
use tonic::Code;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("api key {key} version {version} is not supported")]
    UnsupportedVersion { key: i16, version: i16 },
    #[error("invalid topic name `{0}`")]
    InvalidTopic(String),
    #[error("topic `{0}` does not exist")]
    UnknownTopic(String),
    #[error("topic id {0} does not exist")]
    UnknownTopicId(uuid::Uuid),
    #[error("partition {1} of topic `{0}` does not exist")]
    UnknownPartition(String, i32),
    #[error("{0}")]
    InvalidRequest(String),
    #[error("{0}")]
    InvalidConfig(String),
    #[error("{0}")]
    InvalidPartitions(String),
    #[error("this node is not the group coordinator")]
    NotCoordinator,
    #[error("{bucket:?}: producer {producer_id} sent sequence {sequence}, expected {expected}")]
    OutOfOrderSequence {
        bucket: Bucket,
        producer_id: i64,
        sequence: i32,
        expected: i32,
    },
    #[error("producer {producer_id} epoch {given} is older than the current {current}")]
    ProducerFenced {
        producer_id: i64,
        given: i16,
        current: i16,
    },
    #[error("group `{0}` does not exist")]
    GroupNotFound(String),
    #[error("group `{0}` still has members")]
    NonEmptyGroup(String),
    #[error("member `{0}` is not in the group")]
    UnknownMember(String),
    #[error("generation {given} is not the current {current}")]
    IllegalGeneration { given: i32, current: i32 },
    #[error("the group is rebalancing")]
    RebalanceInProgress,
    #[error("member protocols do not intersect the group's")]
    InconsistentProtocol,
    #[error("a member id is required")]
    MemberIdRequired(String),
    #[error("session timeout {0}ms is outside the allowed range")]
    InvalidSessionTimeout(i32),
    #[error(transparent)]
    Server(#[from] mink_server::Error),
    #[error(transparent)]
    Client(#[from] mink_client::Error),
    #[error(transparent)]
    Coordinator(#[from] mink_coordinator::Error),
    #[error(transparent)]
    Record(#[from] mink_record::Error),
    #[error(transparent)]
    Table(#[from] mink_table::Error),
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl Error {
    pub fn code(&self) -> i16 {
        self.kind().code()
    }

    pub fn kind(&self) -> ResponseError {
        use ResponseError as K;

        match self {
            Error::Protocol(_) | Error::InvalidRequest(_) => K::InvalidRequest,
            Error::UnsupportedVersion { .. } => K::UnsupportedVersion,
            Error::InvalidTopic(_) => K::InvalidTopicException,
            Error::UnknownTopic(_) | Error::UnknownPartition(..) => K::UnknownTopicOrPartition,
            Error::UnknownTopicId(_) => K::UnknownTopicId,
            Error::InvalidConfig(_) => K::InvalidConfig,
            Error::InvalidPartitions(_) => K::InvalidPartitions,
            Error::NotCoordinator => K::NotCoordinator,
            Error::OutOfOrderSequence { .. } => K::OutOfOrderSequenceNumber,
            Error::ProducerFenced { .. } => K::InvalidProducerEpoch,
            Error::GroupNotFound(_) => K::GroupIdNotFound,
            Error::NonEmptyGroup(_) => K::NonEmptyGroup,
            Error::UnknownMember(_) => K::UnknownMemberId,
            Error::IllegalGeneration { .. } => K::IllegalGeneration,
            Error::RebalanceInProgress => K::RebalanceInProgress,
            Error::InconsistentProtocol => K::InconsistentGroupProtocol,
            Error::MemberIdRequired(_) => K::MemberIdRequired,
            Error::InvalidSessionTimeout(_) => K::InvalidSessionTimeout,
            Error::Server(e) => server(e),
            Error::Client(e) => client(e),
            Error::Coordinator(e) => coordinator(e),
            Error::Record(_) => K::CorruptMessage,
            Error::Table(_) => K::InvalidRequest,
            Error::Arrow(_) | Error::Io(_) => K::UnknownServerError,
        }
    }
}

fn server(error: &mink_server::Error) -> ResponseError {
    use ResponseError as K;
    use mink_server::Error as E;

    match error {
        E::NotLeader { .. } => K::NotLeaderOrFollower,
        E::TableNotExist(_) | E::BucketNotExist(_) => K::UnknownTopicOrPartition,
        E::NotLogTable(_) | E::NotKvTable(_) => K::InvalidTopicException,
        E::Unavailable(_) | E::HeldBy { .. } => K::LeaderNotAvailable,
        E::Log(mink_log::Error::OutOfRange { .. }) => K::OffsetOutOfRange,
        E::Log(mink_log::Error::Corrupt(_) | mink_log::Error::EmptyBatch) => K::CorruptMessage,
        E::Log(mink_log::Error::OutOfOrderSequence { .. }) => K::OutOfOrderSequenceNumber,
        E::Log(mink_log::Error::InvalidTimestamp { .. }) => K::InvalidTimestamp,
        E::Metadata(e) => metadata(e),
        _ => K::UnknownServerError,
    }
}

fn coordinator(error: &mink_coordinator::Error) -> ResponseError {
    use ResponseError as K;
    use mink_coordinator::Error as E;

    match error {
        E::Metadata(e) => metadata(e),
        E::Table(_) => K::InvalidRequest,
        E::NoCoordinator => K::NotController,
        E::NoLiveNodes => K::LeaderNotAvailable,
        _ => K::UnknownServerError,
    }
}

fn client(error: &mink_client::Error) -> ResponseError {
    use ResponseError as K;

    match error {
        mink_client::Error::Status(status) => match status.code() {
            Code::AlreadyExists => K::TopicAlreadyExists,
            Code::NotFound => K::UnknownTopicOrPartition,
            Code::InvalidArgument => K::InvalidRequest,
            Code::Unavailable | Code::FailedPrecondition => K::LeaderNotAvailable,
            _ => K::UnknownServerError,
        },
        mink_client::Error::Table(_) => K::InvalidRequest,
        mink_client::Error::NoCoordinator | mink_client::Error::NoLeader(_) => {
            K::LeaderNotAvailable
        }
        _ => K::UnknownServerError,
    }
}

fn metadata(error: &MetadataError) -> ResponseError {
    use ResponseError as K;

    match error {
        MetadataError::TableExists { .. } => K::TopicAlreadyExists,
        MetadataError::TableNotExist { .. } | MetadataError::BucketNotExist { .. } => {
            K::UnknownTopicOrPartition
        }
        MetadataError::InvalidArgument { .. } => K::InvalidRequest,
        _ => K::UnknownServerError,
    }
}
