//! Latest key-value snapshot and lake snapshot descriptions for a bucket or table.

use mink_table::Bucket;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketRef {
    pub bucket: Bucket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvSnapshot {
    pub snapshot_id: u64,
    pub log_offset: i64,
    pub row_count: i64,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestKvSnapshot {
    pub snapshot: Option<KvSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LakeSnapshot {
    pub snapshot_id: i64,
    pub bucket_log_end_offset: Vec<(Bucket, i64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LakeSnapshotResult {
    pub snapshot: Option<LakeSnapshot>,
}
