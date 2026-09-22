//! Every validation failure a table definition or alteration can produce.

use mink_types::DataType;
use thiserror::Error;

use crate::{Aggregate, MergeEngine, name::MAX_NAME_LENGTH};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    #[error(transparent)]
    Type(#[from] mink_types::Error),

    #[error("name must not be empty")]
    EmptyName,
    #[error("name `{0}` is reserved")]
    ReservedName(String),
    #[error("name `{0}` is longer than {MAX_NAME_LENGTH} characters")]
    NameLength(String),
    #[error("name `{0}` contains a character other than ASCII letters, digits, `_` and `-`")]
    NameCharacter(String),
    #[error("table path `{0}` must have the form `database.table`")]
    Path(String),

    #[error("primary key must name at least one column")]
    EmptyPrimaryKey,
    #[error("primary key constraint name must not be blank")]
    BlankConstraintName,
    #[error("duplicate primary key column `{0}`")]
    DuplicatePrimaryKeyColumn(String),
    #[error("unknown column `{0}`")]
    UnknownColumn(String),
    #[error("column `{0}` has no field id")]
    MissingFieldId(String),
    #[error("duplicate field id {0}")]
    DuplicateFieldId(u32),
    #[error("field id {id} is not below the next field id {next}")]
    FieldIdRange { id: u32, next: u32 },
    #[error("auto-increment requires a primary key")]
    AutoIncrementWithoutPrimaryKey,
    #[error("auto-increment column `{0}` must not be part of the primary key")]
    AutoIncrementInPrimaryKey(String),
    #[error("auto-increment column `{0}` must be INT or BIGINT")]
    AutoIncrementType(String),
    #[error("primary key column `{0}` cannot carry an aggregate")]
    AggregateOnPrimaryKey(String),
    #[error("aggregate {aggregate} does not support column `{column}` of type {data_type}")]
    AggregateType {
        aggregate: Aggregate,
        column: String,
        data_type: DataType,
    },

    #[error("duplicate partition key `{0}`")]
    DuplicatePartitionKey(String),
    #[error("partition key `{0}` must be part of the primary key")]
    PartitionKeyNotInPrimaryKey(String),
    #[error("partition key `{column}` has unsupported type {data_type}")]
    PartitionKeyType { column: String, data_type: DataType },
    #[error(
        "primary key {primary_key:?} must have a column outside the partition keys {partition_keys:?}"
    )]
    PrimaryKeyIsPartitionKey {
        primary_key: Vec<String>,
        partition_keys: Vec<String>,
    },
    #[error("duplicate bucket key `{0}`")]
    DuplicateBucketKey(String),
    #[error("bucket key `{0}` must not be a partition key")]
    BucketKeyIsPartitionKey(String),
    #[error("bucket key `{0}` must be part of the primary key")]
    BucketKeyNotInPrimaryKey(String),
    #[error("bucket count must be between 1 and {max}", max = i32::MAX)]
    BucketCount,
    #[error("bucket key bytes must not be empty")]
    EmptyBucketKey,

    #[error("column `{0}` already exists")]
    ColumnExists(String),
    #[error("column `{0}` must be nullable")]
    ColumnNotNullable(String),
    #[error("{0} is not supported")]
    UnsupportedChange(String),
    #[error("column `{column}` is referenced by {referenced_by} and cannot be {change}")]
    ColumnReferenced {
        column: String,
        referenced_by: &'static str,
        change: &'static str,
    },
    #[error("column `{column}` cannot change from {from} to {to}")]
    TypeNotPromotable {
        column: String,
        from: DataType,
        to: DataType,
    },
    #[error("a table needs at least one column")]
    NoColumns,
    #[error("`table.datalake.attach` requires `table.datalake.enabled`")]
    AttachWithoutLake,
    #[error("the option `{0}` is not supported to alter yet")]
    NotAlterable(String),
    #[error("option `{key}` cannot take the value `{value}`")]
    OptionValue { key: String, value: String },
    #[error(
        "the option `table.datalake.enabled` cannot be altered on a cluster without a lake configured"
    )]
    LakeNotConfigured,
    #[error("property `{0}` is not supported to alter, it belongs to the lake table")]
    LakeProperty(String),

    #[error("merge engine requires a primary key")]
    MergeEngineWithoutPrimaryKey,
    #[error("auto-partitioning requires a partitioned table")]
    AutoPartitionWithoutPartitionKeys,
    #[error("auto-partitioning with several partition keys must name its key")]
    AutoPartitionKeyMissing,
    #[error("auto-partition key `{0}` is not a partition key")]
    AutoPartitionKeyUnknown(String),
    #[error("auto-partitioning with several partition keys cannot pre-create partitions")]
    AutoPartitionPrecreateWithSeveralKeys,
    #[error("auto-partition time zone `{0}` is unknown")]
    AutoPartitionTimeZone(String),
    #[error(
        "version column `{column}` must be INT, BIGINT, TIMESTAMP or TIMESTAMP_LTZ, not {data_type}"
    )]
    VersionColumnType { column: String, data_type: DataType },
    #[error("aggregation merge engine requires the full changelog image")]
    AggregationWithWalImage,
    #[error("delete behavior requires a primary key")]
    DeleteBehaviorWithoutPrimaryKey,
    #[error("{0} merge engine does not allow deletes")]
    DeleteNotAllowed(MergeEngine),

    #[error("partition spec keys {spec:?} do not match the partition keys {keys:?}")]
    PartitionSpecKeys {
        spec: Vec<String>,
        keys: Vec<String>,
    },
    #[error("partition name `{name}` has {found} values for {expected} partition keys")]
    PartitionValueCount {
        name: String,
        found: usize,
        expected: usize,
    },
}
