//! Cluster metadata as a replicated state machine: commands applied to an immutable state, with a
//! codec, snapshots, read views, and the managers the stream engine and servers call through.

mod apply;
mod catalog;
pub mod codec;
mod command;
mod error;
mod lifecycle;
mod manager;
mod object;
mod query;
mod sink;
pub mod snapshot;
mod state;
mod stream;
mod view;

pub use apply::apply;
pub use catalog::{
    BucketRow, Catalog, CoordinatorRow, Counter, DatabaseRow, KvSnapshotRow, LakeSnapshotRow,
    OffsetsRow, PartitionRow, TableRow,
};
pub use codec::CodecError;
pub use command::{Command, Outcome};
pub use error::Error;
pub use lifecycle::{Lifecycle, ObjectCleaner};
pub use manager::{Handle, Kv, Objects, Streams};
pub use sink::{CommandSink, LocalSink, Proposed, SinkStats, SnapshotStats};
pub use state::{NodeRow, State, StreamOffsetKey, StreamRow};
pub use view::{View, ViewPublisher};
