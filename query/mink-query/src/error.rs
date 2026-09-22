//! Every failure of connecting, cataloguing, planning or reading, and its mapping into DataFusion.

use std::result;

use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("client: {0}")]
    Client(#[from] mink_client::Error),
    #[error("lake: {0}")]
    Lake(#[from] mink_lake::Error),
    #[error("read: {0}")]
    Read(#[from] mink_read::Error),
    #[error("record: {0}")]
    Record(#[from] mink_record::Error),
    #[error("types: {0}")]
    Types(#[from] mink_types::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] ArrowError),
    #[error("{0}")]
    DataFusion(#[from] DataFusionError),
    #[error("{0}")]
    Config(String),
}

pub type Result<T, E = Error> = result::Result<T, E>;

impl From<Error> for DataFusionError {
    fn from(error: Error) -> Self {
        match error {
            Error::DataFusion(inner) => inner,
            Error::Arrow(inner) => DataFusionError::ArrowError(Box::new(inner), None),
            other => DataFusionError::External(Box::new(other)),
        }
    }
}
