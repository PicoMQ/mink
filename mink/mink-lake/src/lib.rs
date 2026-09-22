//! Tiering of table logs into lake formats: format-agnostic writer, committer and source interfaces,
//! the tiering worker that drives them, and the Iceberg implementation.

mod committer;
pub mod config;
mod error;
mod predicate;
mod source;
pub mod tiering;
mod worker;
mod writer;

#[cfg(feature = "iceberg")]
pub mod iceberg;

pub use committer::{
    BucketOffset, CommitResult, CommittedSnapshot, Committer, CommitterContext, ReadableSnapshot,
    SNAPSHOT_OFFSETS_PROPERTY,
};
pub use config::{CatalogKind, Config};
pub use error::{Error, Result};
pub use predicate::{Compare, Predicate};
pub use source::{LakeSplit, Reader, ScanOptions, Source, Split, Tasks, take_rows};
pub use tiering::{BucketSource, Coordinator, LogSource, SnapshotRead, TableInfo};
pub use worker::{RoundReport, Worker};
pub use writer::{COMMIT_USER, Factory, TieredBatch, Writer, WriterContext};
