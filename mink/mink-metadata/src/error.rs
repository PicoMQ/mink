//! Every failure of applying a command, with a stable numeric code and a mapping to stream engine errors.

use mink_table::Bucket;
use s3stream::Error as StreamError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("stream {stream_id} not found")]
    StreamNotExist { stream_id: u64 },

    #[error("stream {stream_id} is not closed")]
    StreamNotClosed { stream_id: u64 },

    #[error("stream {stream_id} fenced: {message}")]
    StreamFenced {
        stream_id: u64,
        epoch: i64,
        message: String,
    },

    #[error("stream {stream_id} expired epoch: {message}")]
    ExpiredEpoch {
        stream_id: u64,
        epoch: i64,
        message: String,
    },

    #[error("node {node_id} epoch mismatch: {message}")]
    NodeEpochMismatch { node_id: i32, message: String },

    #[error("redundant operation: {message}")]
    Redundant { message: String },

    #[error("unexpected: {message}")]
    Unexpected { message: String },

    #[error("database {name} already exists")]
    DatabaseExists { name: String },

    #[error("database {name} does not exist")]
    DatabaseNotExist { name: String },

    #[error("database {name} is not empty")]
    DatabaseNotEmpty { name: String },

    #[error("table {path} already exists")]
    TableExists { path: String },

    #[error("table {path} does not exist")]
    TableNotExist { path: String },

    #[error("partition {name} of {path} already exists")]
    PartitionExists { path: String, name: String },

    #[error("partition {name} of {path} does not exist")]
    PartitionNotExist { path: String, name: String },

    #[error("bucket {bucket:?} does not exist")]
    BucketNotExist { bucket: Bucket },

    #[error("leader epoch {given} of {bucket:?} is not the current {current}")]
    LeaderFenced {
        bucket: Bucket,
        current: i32,
        given: i32,
    },

    #[error("coordinator epoch {given} is older than the current {current}")]
    CoordinatorFenced { current: i32, given: i32 },

    #[error("invalid argument: {message}")]
    InvalidArgument { message: String },
}

impl Error {
    pub fn code(&self) -> u8 {
        match self {
            Error::StreamNotExist { .. } => 1,
            Error::StreamNotClosed { .. } => 2,
            Error::StreamFenced { .. } => 3,
            Error::ExpiredEpoch { .. } => 4,
            Error::NodeEpochMismatch { .. } => 5,
            Error::Redundant { .. } => 6,
            Error::Unexpected { .. } => 99,
            Error::DatabaseExists { .. } => 20,
            Error::DatabaseNotExist { .. } => 21,
            Error::DatabaseNotEmpty { .. } => 22,
            Error::TableExists { .. } => 23,
            Error::TableNotExist { .. } => 24,
            Error::PartitionExists { .. } => 25,
            Error::PartitionNotExist { .. } => 26,
            Error::BucketNotExist { .. } => 27,
            Error::LeaderFenced { .. } => 28,
            Error::CoordinatorFenced { .. } => 29,
            Error::InvalidArgument { .. } => 30,
        }
    }

    pub fn is_redundant(&self) -> bool {
        matches!(self, Error::Redundant { .. })
    }

    pub fn to_stream_error(&self) -> StreamError {
        match self {
            Error::StreamNotExist { stream_id } => StreamError::NotExist {
                stream_id: *stream_id,
            },
            Error::StreamFenced {
                stream_id, epoch, ..
            }
            | Error::ExpiredEpoch {
                stream_id, epoch, ..
            } => StreamError::Fenced {
                stream_id: *stream_id,
                epoch: (*epoch).max(0) as u64,
            },
            other => StreamError::Unexpected(other.to_string()),
        }
    }
}
