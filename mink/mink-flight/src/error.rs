//! Every failure of a Flight request and its mapping to a gRPC status, with redirect details for leadership.

use std::fmt;
use std::time::Duration;

use arrow_schema::ArrowError;
use mink_table::Bucket;
use tonic::{Code, Status};

use crate::proto::{NodeInfo, Redirect};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Server(#[from] mink_server::Error),
    #[error(transparent)]
    Coordinator(#[from] mink_coordinator::Error),
    #[error("record: {0}")]
    Record(#[from] mink_record::Error),
    #[error("tablet: {0}")]
    Tablet(#[from] mink_tablet::Error),
    #[error("union read: {0}")]
    Read(#[from] mink_read::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] ArrowError),
    #[error("malformed request: {0}")]
    Request(String),
    #[error("unknown action {0:?}")]
    UnknownAction(String),
    #[error("this node is not the coordinator")]
    NotCoordinator { coordinator: Option<NodeInfo> },
    #[error("this node does not lead {bucket:?}")]
    NotLeader {
        bucket: Bucket,
        to: Option<NodeInfo>,
    },
    #[error("schema {0} does not exist")]
    SchemaNotExist(u32),
    #[error("table {0} does not exist")]
    TableNotExist(String),
    #[error("internal: {0}")]
    Internal(String),
    #[error("forwarded: {0}")]
    Remote(Status),
    #[error("no leader for {0:?} within {1:?}")]
    NoLeader(Bucket, Duration),
}

impl Error {
    pub fn request(error: impl fmt::Display) -> Self {
        Error::Request(error.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Request(error.to_string())
    }
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        let message = error.to_string();
        match error {
            Error::Server(error) => server(error, message),
            Error::Coordinator(error) => match error {
                mink_coordinator::Error::Metadata(error) => metadata(error, message),
                mink_coordinator::Error::Table(_) | mink_coordinator::Error::NotPartitioned(_) => {
                    Status::invalid_argument(message)
                }
                mink_coordinator::Error::NoCoordinator | mink_coordinator::Error::NoLiveNodes => {
                    Status::unavailable(message)
                }
                mink_coordinator::Error::Lake(error) => match error {
                    mink_coordinator::lake::Error::TableNotFound(_) => Status::not_found(message),
                    mink_coordinator::lake::Error::TableExists(_) => {
                        Status::already_exists(message)
                    }
                    mink_coordinator::lake::Error::NotConfigured(_)
                    | mink_coordinator::lake::Error::Invalid { .. } => {
                        Status::invalid_argument(message)
                    }
                    mink_coordinator::lake::Error::Backend(_) => Status::internal(message),
                },
                _ => Status::internal(message),
            },
            Error::NotLeader { bucket, to } => redirect(
                Status::new(Code::FailedPrecondition, message),
                Redirect {
                    bucket: Some(bucket),
                    to,
                },
            ),
            Error::NotCoordinator { coordinator } => redirect(
                Status::new(Code::FailedPrecondition, message),
                Redirect {
                    bucket: None,
                    to: coordinator,
                },
            ),
            Error::Record(_) | Error::Arrow(_) | Error::Request(_) | Error::UnknownAction(_) => {
                Status::invalid_argument(message)
            }
            Error::Tablet(error) => tablet(error, message),
            Error::SchemaNotExist(_) | Error::TableNotExist(_) => Status::not_found(message),
            Error::Internal(_) | Error::Read(_) => Status::internal(message),
            Error::Remote(status) => status,
            Error::NoLeader(..) => Status::unavailable(message),
        }
    }
}

fn server(error: mink_server::Error, message: String) -> Status {
    match error {
        mink_server::Error::NotLeader { bucket, leader } => redirect(
            Status::new(Code::FailedPrecondition, message),
            Redirect {
                bucket: Some(bucket),
                to: leader.map(|node_id| NodeInfo {
                    node_id,
                    address: String::new(),
                }),
            },
        ),
        mink_server::Error::TableNotExist(_) | mink_server::Error::BucketNotExist(_) => {
            Status::not_found(message)
        }
        mink_server::Error::NotKvTable(_) | mink_server::Error::NotLogTable(_) => {
            Status::invalid_argument(message)
        }
        mink_server::Error::Unavailable(_) | mink_server::Error::HeldBy { .. } => {
            Status::unavailable(message)
        }
        mink_server::Error::Log(log) => match log {
            mink_log::Error::OutOfRange { .. } => Status::out_of_range(message),
            mink_log::Error::InvalidTimestamp { .. } | mink_log::Error::Corrupt(_) => {
                Status::invalid_argument(message)
            }
            _ => Status::internal(message),
        },
        mink_server::Error::Tablet(error) => tablet(error, message),
        mink_server::Error::Metadata(error) => metadata(error, message),
        _ => Status::internal(message),
    }
}

fn tablet(error: mink_tablet::Error, message: String) -> Status {
    match error {
        mink_tablet::Error::Closed => Status::unavailable(message),
        mink_tablet::Error::Log(log) => server(mink_server::Error::Log(log), message),
        mink_tablet::Error::Kv(_) | mink_tablet::Error::Io(_) | mink_tablet::Error::Storage(_) => {
            Status::internal(message)
        }
        _ => Status::invalid_argument(message),
    }
}

fn metadata(error: mink_metadata::Error, message: String) -> Status {
    match error {
        mink_metadata::Error::DatabaseExists { .. }
        | mink_metadata::Error::TableExists { .. }
        | mink_metadata::Error::PartitionExists { .. } => Status::already_exists(message),
        mink_metadata::Error::DatabaseNotExist { .. }
        | mink_metadata::Error::TableNotExist { .. }
        | mink_metadata::Error::PartitionNotExist { .. }
        | mink_metadata::Error::BucketNotExist { .. } => Status::not_found(message),
        mink_metadata::Error::DatabaseNotEmpty { .. } => Status::failed_precondition(message),
        mink_metadata::Error::InvalidArgument { .. } => Status::invalid_argument(message),
        _ => Status::internal(message),
    }
}

fn redirect(status: Status, redirect: Redirect) -> Status {
    let details = serde_json::to_vec(&redirect).unwrap_or_default();
    Status::with_details(status.code(), status.message().to_owned(), details.into())
}
