//! The bucket leadership moves a rebalance produced.

use mink_table::Bucket;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rebalanced {
    pub moves: Vec<BucketMove>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketMove {
    pub bucket: Bucket,
    pub from: i32,
    pub to: i32,
}
