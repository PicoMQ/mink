//! A long-poll read request that waits for new data in a bucket.

use mink_table::Bucket;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tail {
    pub bucket: Bucket,
    pub offset: i64,
    pub columns: Option<Vec<usize>>,
    pub max_wait_ms: Option<u64>,
    pub min_bytes: Option<usize>,
}
