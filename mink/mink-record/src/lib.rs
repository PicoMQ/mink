//! Row and batch encoding: the log batch wire format, Arrow bodies, key and partition extraction,
//! and the compacted row form used by the key-value store.

mod arrow;
mod batch;
mod builder;
mod change;
mod codec;
mod compacted;
mod compression;
mod error;
pub mod header;
mod iceberg;
mod key;
mod message;
mod paimon;
mod partition;
mod projection;
mod remap;
mod route;
mod row;
mod scalar;

pub use arrow::ArrowCodec;
pub use batch::{Batch, Spec, build};
pub use builder::{Builder, Rows};
pub use change::ChangeType;
pub use codec::{Changes, Codec, Records, codec};
pub use compression::Compression;
pub use error::Error;
pub use header::Header;
pub use key::{Bound, KeyColumn, KeyEncoder};
pub use partition::{BoundPartition, PartitionGetter};
pub use projection::Projection;
pub use remap::Remap;
pub use route::{Group, Router, take};
pub use row::{CompactedRow, Row, RowCodec, row_codec};
pub use scalar::{Reader, Scalar, Timestamp};
