//! Every client failure, with redirect extraction from a status and the retriable classification.

use arrow_flight::error::FlightError;
use arrow_schema::ArrowError;
use mink_table::{Bucket, Path};
use tonic::{Code, Status};

use crate::proto;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Status(#[from] Status),
    #[error("flight: {0}")]
    Flight(#[from] FlightError),
    #[error("arrow: {0}")]
    Arrow(#[from] ArrowError),
    #[error("message: {0}")]
    Json(#[from] serde_json::Error),
    #[error("record: {0}")]
    Record(#[from] mink_record::Error),
    #[error("table: {0}")]
    Table(#[from] mink_table::Error),
    #[error("bad node address {0:?}: {1}")]
    Address(String, String),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("{0:?} has no leader")]
    NoLeader(Bucket),
    #[error("no coordinator is registered")]
    NoCoordinator,
    #[error("table {0} is not a primary-key table")]
    NotPrimaryKey(Path),
    #[error("table {0} is a primary-key table; use the upsert writer")]
    PrimaryKey(Path),
    #[error("{0:?} is not a bucket of table {1}")]
    UnknownBucket(Bucket, Path),
    #[error("gave up after {attempts} redirects: {last}")]
    Redirects { attempts: usize, last: Status },
}

impl Error {
    pub fn redirect(&self) -> Option<proto::Redirect> {
        let Error::Status(status) = self else {
            return None;
        };
        if status.code() != Code::FailedPrecondition || status.details().is_empty() {
            return None;
        }

        serde_json::from_slice(status.details()).ok()
    }

    pub fn is_retriable(&self) -> bool {
        match self {
            Error::Status(status) => self.redirect().is_some() || transient(status),
            Error::Flight(FlightError::Tonic(status)) => transient(status),
            Error::NoLeader(_) | Error::NoCoordinator => true,
            _ => false,
        }
    }
}

fn transient(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable => true,
        Code::Unknown | Code::Cancelled | Code::Internal => {
            let message = status.message();
            message.contains("transport error")
                || message.contains("connection")
                || message.contains("h2 protocol error")
                || message.contains("broken pipe")
                || message.contains("stream closed")
                || message.contains("reset")
        }
        _ => false,
    }
}
