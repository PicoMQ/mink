//! Every failure of planning or running a union read.

use std::result;

use arrow_schema::ArrowError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("lake: {0}")]
    Lake(#[from] mink_lake::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] ArrowError),
    #[error("record: {0}")]
    Record(#[from] mink_record::Error),
    #[error("{0}")]
    Schema(String),
    #[error("the plan reads the lake but no lake reader is configured")]
    NoLake,
}

pub(crate) type Result<T, E = Error> = result::Result<T, E>;
