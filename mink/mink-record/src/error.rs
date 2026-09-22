//! Every failure in encoding, decoding, keying and partitioning rows and batches.

use arrow_schema::DataType as ArrowType;
use mink_table::Bucketing;
use mink_types::DataType;
use thiserror::Error;

use crate::ChangeType;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    #[error("unknown column `{0}`")]
    UnknownColumn(String),
    #[error("a key needs at least one column")]
    EmptyKey,
    #[error("{format:?} bucketing cannot encode key column `{column}` of type {data_type}")]
    KeyType {
        format: Bucketing,
        column: String,
        data_type: DataType,
    },
    #[error("iceberg bucketing takes exactly one key column, got {0}")]
    IcebergKeyArity(usize),
    #[error("key column `{0}` is null")]
    NullKey(String),
    #[error("column `{column}` of type {data_type} cannot be a partition key")]
    PartitionType { column: String, data_type: DataType },
    #[error("value {value:?} of partition column `{column}` is not a valid partition name")]
    PartitionValue { column: String, value: String },
    #[error("batch has {found} columns, key encoder needs column {index}")]
    ColumnIndex { index: usize, found: usize },
    #[error("column `{column}` is {found} in the batch, expected {expected} for {data_type}")]
    ColumnType {
        column: String,
        data_type: DataType,
        expected: ArrowType,
        found: ArrowType,
    },
    #[error("row {row} is out of range for a batch of {rows} rows")]
    RowIndex { row: usize, rows: usize },
    #[error("unknown change type byte {0}")]
    ChangeType(u8),
    #[error("table descriptor has no bucket count")]
    NoBucketCount,
    #[error("bucketing: {0}")]
    Bucket(String),

    #[error("need {needed} bytes, found {found}")]
    Truncated { needed: usize, found: usize },
    #[error("unsupported batch magic {0}")]
    Magic(u8),
    #[error("invalid batch length {0}")]
    Length(i32),
    #[error("invalid record count {0}")]
    RecordCount(i32),
    #[error("schema id {0} does not fit the header")]
    SchemaId(i64),
    #[error("batch of {0} bytes exceeds the header's length field")]
    BatchTooLarge(usize),
    #[error("batch is corrupt (stored crc = {stored}, computed crc = {computed})")]
    Crc { stored: u32, computed: u32 },
    #[error("{changes} change types for {rows} rows")]
    ChangeCount { changes: usize, rows: usize },
    #[error("append-only batch cannot carry {0}")]
    NotAppendOnly(ChangeType),
    #[error("header declares {declared} records, body holds {found}")]
    RowCount { declared: usize, found: usize },
    #[error("arrow ipc: {0}")]
    Ipc(String),
    #[error("invalid projection: {0}")]
    Projection(String),

    #[error("row codec cannot encode type {0}")]
    RowType(DataType),
    #[error("column {index} is non-nullable but the value is null")]
    NullField { index: usize },
    #[error("row has {found} fields, codec expects {expected}")]
    FieldCount { expected: usize, found: usize },
    #[error("row has {0} trailing bytes")]
    TrailingBytes(usize),
    #[error("malformed varint")]
    Varint,
    #[error("string field is not utf-8")]
    Utf8,
    #[error("timestamp nanos field out of range")]
    Nanos,
    #[error("decimal unscaled value of {0} bytes")]
    Unscaled(usize),
    #[error("scalar {scalar} cannot be appended to a {data_type} column")]
    ScalarType { data_type: DataType, scalar: String },
}
