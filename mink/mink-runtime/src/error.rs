//! Every failure of starting a node.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::config::ConfigError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("metadata store: {0}")]
    Store(#[from] mink_sql::StoreError),
    #[error("metadata log: {0}")]
    MetadataLog(#[from] mink_sql::Error),
    #[error("metadata: {0}")]
    Metadata(#[from] mink_metadata::Error),
    #[error("object storage: {0}")]
    Storage(#[from] s3stream::ObjectError),
    #[error("write-ahead log: {0}")]
    Wal(#[from] s3stream::WalError),
    #[error("engine: {0}")]
    Engine(#[from] s3stream::Error),
    #[error("lake: {0}")]
    Lake(#[from] mink_lake::Error),
    #[error("bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("kafka: {0}")]
    Kafka(String),
    #[error("create directory {path}: {source}")]
    DataDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}
