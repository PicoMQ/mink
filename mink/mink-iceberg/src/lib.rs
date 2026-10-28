mod arrays;
mod assembly;
mod file;
#[cfg(test)]
mod payload;
mod ranges;
mod rows;
mod schema;
mod table;
#[cfg(test)]
mod testing;

use arrow_schema::ArrowError;
use mink_core::{File, Partition};

pub use assembly::assemble;
pub use file::{build_file, file_contents};
pub use ranges::{NewFiles, tail_files};
pub use rows::{Decoded, decode};
pub use schema::{FileSchema, TypedSchema};
pub use table::{Change, FIRST_TAIL_SNAPSHOT_ID, FormatVersion, ObjectReader, Table};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("{0:?} has no committed records, so it has no bytes")]
    NoRecords(File),
    #[error("the record at offset {offset} is outside its file")]
    RecordOutsideFile { offset: i64 },
    #[error("the record at offset {offset} is not after the record before it")]
    RecordOutOfOrder { offset: i64 },
    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("Iceberg format version {0} is not supported")]
    FormatVersion(u8),
    #[error("a snapshot of topic {snapshot} cannot be committed to the table of topic {table}")]
    Topic { table: String, snapshot: String },
    #[error("snapshot {given} conflicts: the next snapshot of the table is {expected}")]
    Conflict { expected: i64, given: i64 },
    #[error("snapshot {id} is the latest snapshot or later, so it cannot expire")]
    LatestSnapshot { id: i64 },
    #[error("the tail does not extend {0:?} of the latest snapshot")]
    TailOffsets(Partition),
    #[error("reading {path}: {message}")]
    ObjectRead { path: String, message: String },
    #[error(transparent)]
    Core(#[from] mink_core::Error),
    #[error("corrupt table metadata: {0}")]
    Corrupt(String),
    #[error("{0:?} lies outside the offsets of the snapshot that adds it")]
    FileOutsideSnapshot(File),
    #[error("stats name field {0}, which is not a primitive field of the binary schema")]
    StatsField(i32),
    #[error("timestamp of the record at offset {offset} overflows microseconds")]
    TimestampOverflow { offset: i64 },
    #[error(transparent)]
    Schema(#[from] mink_schema::Error),
    #[error("time spec {0} is not a spec of the table")]
    UnknownTimeSpec(i32),
    #[error("assembling files: {0}")]
    Assembly(String),
    #[error("{file:?} names its schema, not schema {schema_id}")]
    FileSchema { file: File, schema_id: i32 },
}
