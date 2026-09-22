//! Offset queries by position or timestamp and their reply.

use mink_table::Bucket;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OffsetSpec {
    Earliest,
    Latest,
    Timestamp { timestamp: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListOffsets {
    pub bucket: Bucket,
    pub spec: OffsetSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offset {
    pub offset: i64,
}
