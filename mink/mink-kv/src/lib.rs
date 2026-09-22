//! Embedded key-value storage behind one store interface, with an in-memory engine and a persistent
//! LSM engine, batched writes, and directory checkpoints.

mod batch;
mod checkpoint;
mod error;
mod memory;
mod options;
mod store;
#[cfg(feature = "surrealkv")]
mod surreal;
mod writer;

pub use batch::{Batch, Op};
pub use checkpoint::{Checkpoint, CheckpointFile, TABLE_FILE_SUFFIX};
pub use error::Error;
pub use memory::{MemoryEngine, MemoryStore};
pub use options::Options;
pub use store::{Engine, Snapshot, Store};
#[cfg(feature = "surrealkv")]
pub use surreal::{SurrealEngine, SurrealStore};
pub use writer::{BATCH_CAPACITY, Writer};
