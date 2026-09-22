//! Requests and replies for creating, dropping and listing partitions.

use mink_table::{PartitionId, PartitionName, PartitionSpec, Path};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRequest {
    pub path: Path,
    pub spec: PartitionSpec,
    #[serde(default)]
    pub ignore_if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCreated {
    pub partition_id: Option<PartitionId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_id: PartitionId,
    pub name: PartitionName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partitions {
    pub partitions: Vec<PartitionInfo>,
}
