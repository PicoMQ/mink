mod arrays;
mod assembly;
mod file;
mod framing;
#[cfg(test)]
mod payload;
mod ranges;
mod rows;
mod schema;
mod slice;
mod table;
#[cfg(test)]
mod testing;
mod tiered;
mod writer;

use arrow_schema::ArrowError;
use mink_core::{File, Partition};

pub use assembly::assemble;
pub use file::{build_file, file_contents};
pub use framing::unpadded;
pub use ranges::{NewFiles, tail_files};
pub use rows::{Decoded, decode, record_batch};
pub use schema::{FileSchema, TypedSchema};
pub use slice::{Extent, Slice, extent, extents, slices};
pub use table::{
    Change, ExternalSnapshot, FIRST_TAIL_SNAPSHOT_ID, FormatVersion, ObjectReader, Table,
    check_schema, iceberg_spec_id,
};
pub use tiered::{
    append, decode_data_files, encode_data_files, end_offsets, end_offsets_update,
    external_partition_spec,
};
pub use writer::write_data_files;

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
    #[error("the sort order names field {0}, which is not a top-level column")]
    SortField(i32),
    #[error("a data file cannot be built: {0}")]
    DataFile(String),
    #[error("table property {key} has an unusable value {value:?}")]
    TableProperty { key: String, value: String },
    #[error("partition spec {0} is not a spec of the external table")]
    UnknownPartitionSpec(i32),
    #[error("external spec id {0} is in the range of the table's time spec ids")]
    ExternalSpecId(i32),
    #[error("snapshots through {through_id} list another external table")]
    OtherGenerationListed { through_id: i64 },
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
    #[error("framing a file: {0}")]
    Framing(String),
    #[error("{file:?} built a body of {built} bytes where its span holds {stored}")]
    BuiltLengthMismatch { file: File, built: u64, stored: u64 },
    #[error("{file:?} names its schema, not schema {schema_id}")]
    FileSchema { file: File, schema_id: i32 },
    #[error("the external table's current schema is not the topic's schema")]
    ExternalSchema,
    #[error("partition field {0} of the external table differs from the time field with its id")]
    ExternalPartitionField(i32),
    #[error("the external table has equality deletes")]
    EqualityDeletes,
    #[error("the snapshot starts {0:?} before the external table's end offset")]
    ExternalOffsets(Partition),
    #[error("sequence number {given} is not above the external table's {listed}")]
    ExternalSequenceNumber { listed: i64, given: i64 },
}

impl Error {
    pub(crate) fn table_property(key: &str, value: &str) -> Error {
        Error::TableProperty {
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }
}
