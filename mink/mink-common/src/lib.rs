//! Domain-free utilities shared by every crate in the workspace.
//! Nothing here knows about tables, logs, or the wire protocol.

pub mod bigint;
pub mod codec;
pub mod json;
pub mod murmur;
pub mod net;
pub mod serde;
pub mod sync;
pub mod text;
pub mod time;
pub mod url;
pub mod varint;

pub use time::{Clock, ManualClock, SystemClock};
