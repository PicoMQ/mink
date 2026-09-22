//! Every failure of lake catalog, write, commit and read operations.

use std::fmt;
use std::result;

use arrow_schema::ArrowError;
use mink_coordinator::lake;
use mink_table::Path;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("lake table {0} already exists")]
    TableExists(Path),
    #[error("lake table {0} does not exist")]
    TableNotFound(Path),
    #[error("lake snapshot {0} is missing property {1}")]
    SnapshotProperty(i64, &'static str),
    #[error("arrow: {0}")]
    Arrow(#[from] ArrowError),
    #[error("record: {0}")]
    Record(#[from] mink_record::Error),
    #[error("lake commit conflict: {0}")]
    CommitConflict(String),
    #[error("coordinator: {0}")]
    Coordinator(#[from] mink_coordinator::Error),
    #[cfg(feature = "iceberg")]
    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),
    #[cfg(feature = "iceberg")]
    #[error("iceberg rest: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn invalid(message: impl Into<String>) -> Self {
        Error::Invalid(message.into())
    }

    pub fn other(error: impl fmt::Display) -> Self {
        Error::Other(error.to_string())
    }

    pub fn is_fenced(&self) -> bool {
        matches!(
            self,
            Error::Coordinator(mink_coordinator::Error::TieringFenced { .. })
        )
    }

    pub(crate) fn into_catalog(self, path: &Path) -> lake::Error {
        match self {
            Error::TableExists(path) => lake::Error::TableExists(path),
            Error::TableNotFound(path) => lake::Error::TableNotFound(path),
            Error::Invalid(reason) => lake::Error::Invalid {
                path: path.clone(),
                reason,
            },
            other => lake::Error::Backend(other.to_string()),
        }
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;
