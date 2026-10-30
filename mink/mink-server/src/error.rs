use std::io;

use mink_schema::SchemaKey;
use nestor::{NestorError, OriginError};

use crate::config::ConfigError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] mink_store::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Kafka(#[from] mink_kafka::Error),
    #[error(transparent)]
    Iceberg(#[from] mink_iceberg::Error),
    #[error(transparent)]
    Schema(#[from] mink_schema::Error),
    #[error(transparent)]
    Core(#[from] mink_core::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("warehouse {0:?} is not an s3:// URI with a bucket")]
    InvalidWarehouse(String),
    #[error("{0} is outside the warehouse bucket")]
    OutsideBucket(String),
    #[error("no object {0}")]
    NoSuchKey(String),
    #[error("{path} built to {built} bytes, but its stored stats say {stored}")]
    BuiltLengthMismatch {
        path: String,
        built: u64,
        stored: u64,
    },
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("table {0} is not indexing")]
    NotIndexing(String),
    #[error("Kafka has no topic {0}")]
    NoSuchTopic(String),
    #[error("the schema registry is unavailable: {0}")]
    RegistryUnavailable(String),
    #[error("the schema registry answered unexpectedly: {0}")]
    RegistryResponse(String),
    #[error("schema {0} belongs to another registry")]
    ForeignSchemaKey(SchemaKey),
    #[error("registry schema {0} is not stored")]
    UnstoredRegistrySchema(SchemaKey),
    #[error("topic {topic} has no table schema {id}")]
    UnknownTableSchema { topic: String, id: i32 },
    #[error("no namespace {0}")]
    NoSuchNamespace(String),
    #[error("no table {0}")]
    NoSuchTable(String),
    #[error("forwarding to the owning node failed: {0}")]
    Forward(reqwest::Error),
    #[error("the HTTP client cannot be built: {0}")]
    HttpClient(reqwest::Error),
    #[error("node {0} has no address")]
    NoAddress(uuid::Uuid),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Cache(NestorError),
    #[error("the range is outside the object of {0} bytes")]
    Unsatisfiable(u64),
}

impl Error {
    pub(crate) fn store_unavailable(&self) -> bool {
        match self {
            Error::Store(mink_store::Error::Unavailable(_)) => true,
            Error::Cache(error) => origin(error).is_some_and(Error::store_unavailable),
            _ => false,
        }
    }
}

fn origin(error: &NestorError) -> Option<&Error> {
    match error {
        NestorError::Origin(OriginError::Io(inner)) => inner.downcast_ref::<Error>(),
        NestorError::Shared(inner) => origin(inner),
        _ => None,
    }
}
