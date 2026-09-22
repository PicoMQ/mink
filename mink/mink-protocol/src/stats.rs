//! Per-node statistics: hosted buckets with offsets, key-value state and retention, and the tiering schedule.

use mink_table::{Bucket, Id, Path};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStats {
    pub node_id: i32,
    pub node_epoch: i64,
    pub coordinator: bool,
    pub buckets: Vec<BucketStats>,
    pub tiering: Vec<TieringStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketStats {
    pub bucket: Bucket,
    pub path: Path,
    pub leader_epoch: i32,
    pub log_start_offset: i64,
    pub high_watermark: i64,
    pub log_end_offset: i64,
    pub writers: usize,
    pub kv: Option<KvStats>,
    pub retention: Option<RetentionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvStats {
    pub row_count: i64,
    pub flushed_log_offset: i64,
    pub snapshot_log_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionInfo {
    pub offset: i64,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieringStatus {
    pub table_id: Id,
    pub path: Path,
    pub state: String,
    pub epoch: u64,
    pub last_tiered_ms: i64,
    pub due_ms: Option<i64>,
    pub heartbeat_ms: Option<i64>,
}
