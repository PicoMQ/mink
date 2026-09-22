//! The liveness reply of a node.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Health {
    pub node_id: i32,
    pub node_epoch: i64,
    pub registered: bool,
    pub coordinator: bool,
    pub hosted_buckets: usize,
    pub uptime_ms: i64,
}
