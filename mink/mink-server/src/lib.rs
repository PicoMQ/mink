//! The tablet server: hosts the buckets it leads, syncing with the catalog to open, close and destroy them,
//! and serves appends, reads, lookups, snapshots and retention on top of them.

mod cleaner;
mod error;
mod failover;
mod node;
mod registry;
mod retention;
mod schemas;
mod sequence;
mod service;
mod snapshot;
pub mod tiering;

pub use cleaner::Cleaner;
pub use error::Error;
pub use failover::{Failover, take_over};
pub use node::{Config, Node, Report};
pub use registry::{Hosted, Kv, Registry, RetentionFrontier};
pub use retention::Retention;
pub use schemas::Schemas;
pub use sequence::Sequence;
pub use service::{LimitScan, OffsetSpec, Owner, Service, TableInfo};
pub use tiering::{Source, table};
