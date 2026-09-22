//! Registration, retrieval and deletion of a sink's recorded start offsets.

use mink_table::Bucket;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterProducerOffsets {
    pub producer_id: String,
    pub offsets: Vec<BucketOffset>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketOffset {
    pub bucket: Bucket,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerOffsetsRegistered {
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerRef {
    pub producer_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerOffsets {
    pub producer_id: String,
    pub expires_ms: i64,
    pub offsets: Vec<BucketOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerOffsetsResult {
    pub snapshot: Option<ProducerOffsets>,
}
