//! Cluster-wide description: nodes with liveness and load, the coordinator, and object counts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub nodes: Vec<ClusterNode>,
    pub coordinator: Option<CoordinatorInfo>,
    pub databases: usize,
    pub tables: usize,
    pub partitions: usize,
    pub buckets: usize,
    pub unled_buckets: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: i32,
    pub address: String,
    pub epoch: i64,
    pub live: bool,
    pub leading: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub protocols: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatorInfo {
    pub node_id: i32,
    pub address: String,
    pub epoch: i32,
}
