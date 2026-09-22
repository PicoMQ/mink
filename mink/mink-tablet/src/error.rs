//! Every failure of the tablet: schema, merge engine, auto-increment, snapshot and storage errors.

use std::io;

use mink_table::SchemaId;
use mink_types::DataType;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("kv tablet is closed")]
    Closed,
    #[error("schema {0} does not exist")]
    SchemaNotExist(SchemaId),
    #[error("target column {0} was dropped from the latest schema")]
    TargetColumnDropped(usize),
    #[error("table has no primary key")]
    NoPrimaryKey,
    #[error("batch has {ops} operations for {rows} rows")]
    OpCount { ops: usize, rows: usize },
    #[error("target columns {targets:?} must contain the primary key columns {keys:?}")]
    TargetsMissKey {
        targets: Vec<String>,
        keys: Vec<String>,
    },
    #[error("target column index {0} is out of range")]
    TargetIndex(usize),
    #[error("partial update requires non-key column `{0}` to be nullable")]
    TargetNotNullable(String),
    #[error("partial update is not supported for the {0} merge engine")]
    PartialUnsupported(&'static str),
    #[error("delete is not supported for the {0} merge engine")]
    DeleteUnsupported(&'static str),
    #[error("delete operations are disabled for this table")]
    DeleteDisabled,
    #[error("version column `{0}` does not exist in the schema")]
    VersionColumn(String),
    #[error(
        "version column `{column}` must be INT, BIGINT, TIMESTAMP or TIMESTAMP_LTZ, not {data_type}"
    )]
    VersionType { column: String, data_type: DataType },
    #[error("`versioned` merge engine needs a version column")]
    VersionColumnMissing,
    #[error("partial aggregate requires non-key column `{0}` to be nullable")]
    AggregateNotNullable(String),
    #[error("aggregate `{aggregate}` on column `{column}` got {found}")]
    AggregateOperand {
        aggregate: String,
        column: String,
        found: String,
    },
    #[error("roaring bitmap in column `{0}` is malformed")]
    AggregateBitmap(String),
    #[error("auto-increment column `{0}` is missing, has no id, or is not INT or BIGINT")]
    AutoIncrementColumn(String),
    #[error("auto-increment cache size must be positive")]
    AutoIncrementCache,
    #[error(
        "table has auto-increment column `{0}`: the put must name its target columns and leave it out"
    )]
    AutoIncrementTargets(String),
    #[error("auto-increment column `{0}` must not be a target column")]
    AutoIncrementTarget(String),
    #[error("no auto-increment ids reserved")]
    AutoIncrementExhausted,
    #[error("reached the maximum value of sequence `{column}` ({max})")]
    AutoIncrementOverflow { column: String, max: i64 },
    #[error("auto-increment range is for column {found}, tablet has column {expected}")]
    AutoIncrementRange { expected: u32, found: u32 },
    #[error("sequence counter: {0}")]
    Sequence(String),
    #[error("snapshot metadata is corrupt: {0}")]
    SnapshotCorrupt(String),
    #[error(transparent)]
    Storage(#[from] object_store::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("pre-write buffer sequence numbers must increase: have {current}, got {incoming}")]
    SequenceOrder { current: i64, incoming: i64 },
    #[error("changelog landed at offset {actual}, pre-write buffer expected {expected}")]
    OffsetMismatch { expected: i64, actual: i64 },
    #[error("kv value of {0} bytes has no schema id")]
    ValueTooShort(usize),
    #[error(transparent)]
    Record(#[from] mink_record::Error),
    #[error(transparent)]
    Log(#[from] mink_log::Error),
    #[error(transparent)]
    Kv(#[from] mink_kv::Error),
}
