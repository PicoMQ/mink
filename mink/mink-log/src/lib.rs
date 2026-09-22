//! The per-bucket log: appends with idempotent writer tracking, bounded reads, offset lookup by time,
//! and writer state snapshots, on top of a durable stream.

mod append;
mod error;
mod fetch;
mod offset;
mod snapshot;
mod tablet;
mod writer;

pub use append::AppendInfo;
pub use error::Error;
pub use fetch::{FetchInfo, FetchIsolation};
pub use offset::OffsetSnapshot;
pub use snapshot::{Entry, Kv, Snapshot, SnapshotStore};
pub use tablet::{Config, Tablet};
pub use writer::{BATCHES_TO_RETAIN, BatchMetadata, State, Writers};
