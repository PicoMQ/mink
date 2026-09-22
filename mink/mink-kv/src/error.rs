//! Failures of the key-value store: closed store, corrupt checkpoint, I/O and engine errors.

use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("kv store is closed")]
    Closed,
    #[error("kv store is not empty")]
    NotEmpty,
    #[error("checkpoint is corrupt: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("kv engine: {0}")]
    Engine(String),
}
