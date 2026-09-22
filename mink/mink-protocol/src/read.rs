//! Read requests: bounded scans, snapshot reads and unions over a bucket, lake reads over a partition,
//! plus per-batch metadata.

use mink_table::{Bucket, PartitionName, Path, SchemaId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Read {
    Scan {
        bucket: Bucket,
        offset: i64,
        max_bytes: Option<usize>,
        columns: Option<Vec<usize>>,
    },
    LimitScan {
        bucket: Bucket,
        limit: usize,
        columns: Option<Vec<usize>>,
    },
    Snapshot {
        bucket: Bucket,
        columns: Option<Vec<usize>>,
        batch_rows: Option<usize>,
    },
    Union {
        bucket: Bucket,
        columns: Option<Vec<usize>>,
    },
    Lake {
        path: Path,
        partition: Option<PartitionName>,
        snapshot_id: i64,
        columns: Option<Vec<usize>>,
    },
}

impl Read {
    pub fn project(&mut self, projection: Option<Vec<usize>>) {
        match self {
            Read::Scan { columns, .. }
            | Read::LimitScan { columns, .. }
            | Read::Snapshot { columns, .. }
            | Read::Union { columns, .. }
            | Read::Lake { columns, .. } => *columns = projection,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBatch {
    pub log_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanBatch {
    pub base_offset: i64,
    pub last_offset: i64,
    pub commit_timestamp: i64,
    pub schema_id: SchemaId,
    pub changes: Option<Vec<u8>>,
    pub high_watermark: i64,
}
