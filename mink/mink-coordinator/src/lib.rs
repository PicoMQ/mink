//! The cluster coordinator: catalog operations, bucket leadership, snapshot retention, automatic
//! partitioning and the tiering schedule, all driven from the metadata views.

mod assign;
mod coordinator;
mod error;
pub mod lake;
mod membership;
mod partition;
mod rebalance;
mod snapshots;
pub mod tiering;

pub use assign::{assign, elect, leader_load, orphaned};
pub use coordinator::{Config, Coordinator, Report};
pub use error::Error;
pub use lake::{LakeCatalog, MemoryLakeCatalog, NoLakeCatalog};
pub use membership::{Membership, StaticMembership};
pub use partition::{Plan as PartitionPlan, partition_time, plan as plan_partitions};
pub use rebalance::{Move, limits, plan as plan_rebalance};
pub use snapshots::{NoopCleaner, SnapshotCleaner, excess};
pub use tiering::{
    DEFAULT_TIMEOUT as TIERING_TIMEOUT, State as TieringState, Status as TieringStatus,
};
