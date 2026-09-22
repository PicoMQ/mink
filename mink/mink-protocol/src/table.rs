//! Requests and replies for creating, dropping, altering and describing tables.

use mink_table::{Bucket, Change, Descriptor, Id, Path, Schema, SchemaId};
use serde::{Deserialize, Serialize};

use crate::metadata::NodeInfo;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTable {
    pub path: Path,
    pub descriptor: Descriptor,
    #[serde(default)]
    pub ignore_if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropTable {
    pub path: Path,
    #[serde(default)]
    pub ignore_if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRef {
    pub path: Path,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketLeader {
    pub bucket: Bucket,
    pub leader: Option<NodeInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableInfo {
    pub table_id: Id,
    pub path: Path,
    pub descriptor: Descriptor,
    pub schemas: Vec<Schema>,
    pub created_ms: i64,
    pub modified_ms: i64,
    pub buckets: Vec<BucketLeader>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Created {
    pub table_id: Option<Id>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterTable {
    pub path: Path,
    pub changes: Vec<Change>,
    #[serde(default)]
    pub ignore_if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Altered {
    pub schema_id: Option<SchemaId>,
}
