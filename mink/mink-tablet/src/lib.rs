//! The primary-key tablet: applies puts through the merge engine, writes the changelog to the log,
//! flushes rows to the key-value store, and snapshots and recovers that store.

mod aggregate;
mod autoinc;
mod changelog;
mod error;
mod merger;
mod partial;
mod prewrite;
mod put;
mod recover;
mod schema;
mod snapshot;
mod tablet;
mod targets;
mod value;

pub use autoinc::{AutoIncrement, IdRange, Sequence, Tracker};
pub use error::Error;
pub use merger::{Decoded, Merged, Merger, RowMerger};
pub use partial::Updater;
pub use prewrite::{Buffer, TruncateReason};
pub use put::{Op, Put};
pub use recover::{RecoverPoint, Replay, Replayed};
pub use schema::{Fixed, Schemas, Version, Versions};
pub use snapshot::{CompletedSnapshot, METADATA_FILE, SnapshotFile, Uploader, discard, download};
pub use tablet::{Checkpoint, Config, Scan, Tablet};
pub use value::{Value, values_to_arrow};
