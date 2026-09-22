//! Table definitions: schemas, columns, keys, partitioning, bucketing and table options,
//! with validation of every rule a table must satisfy before it exists.

mod aggregate;
mod alter;
mod autopartition;
mod bucket;
mod bucketing;
mod changelog;
mod column;
mod delete;
mod descriptor;
mod error;
mod format;
mod id;
mod key;
mod lake;
mod merge;
mod name;
mod options;
mod partition;
mod path;
mod schema;

pub use aggregate::{Aggregate, DEFAULT_LIST_DELIMITER};
pub use alter::{Change, alter_table, apply_schema_changes};
pub use autopartition::{AutoPartition, TimeUnit};
pub use bucket::{Bucket, BucketId};
pub use bucketing::Bucketing;
pub use changelog::ChangelogImage;
pub use column::Column;
pub use delete::DeleteBehavior;
pub use descriptor::{Descriptor, DescriptorBuilder};
pub use error::Error;
pub use format::{KvFormat, LogFormat};
pub use id::{Id, PartitionId, SchemaId};
pub use key::PrimaryKey;
pub use lake::LakeFormat;
pub use merge::MergeEngine;
pub use name::{MAX_NAME_LENGTH, Name};
pub use options::{DEFAULT_LAKE_FRESHNESS, DEFAULT_LOG_TTL, Options};
pub use partition::{PartitionName, PartitionSpec};
pub use path::Path;
pub use schema::{Schema, SchemaBuilder};
