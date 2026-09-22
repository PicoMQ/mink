//! Write requests to a bucket or table, per-batch metadata, and what was written or routed.

use mink_table::{Bucket, PartitionName, Path, SchemaId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Write {
    Append {
        bucket: Bucket,
        schema_id: SchemaId,
        writer_id: Option<i64>,
    },
    Put {
        bucket: Bucket,
        schema_id: SchemaId,
        writer_id: Option<i64>,
        target_columns: Option<Vec<usize>>,
    },
    AppendTable {
        path: Path,
        schema_id: SchemaId,
    },
    PutTable {
        path: Path,
        schema_id: SchemaId,
        target_columns: Option<Vec<usize>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Routed {
    pub buckets: Vec<RoutedBucket>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutedBucket {
    pub bucket: Bucket,
    pub partition: Option<PartitionName>,
    pub rows: usize,
    pub first_offset: i64,
    pub last_offset: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteBatch {
    pub batch_sequence: Option<i32>,
    pub changes: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Written {
    pub first_offset: i64,
    pub last_offset: i64,
    pub duplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterId {
    pub writer_id: i64,
}
