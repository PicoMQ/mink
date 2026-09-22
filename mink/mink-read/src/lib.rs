//! Union reads over a bucket: the lake snapshot followed by the log tail, merged by primary key when the
//! table has one, with column projection.

mod error;
mod merge;
mod plan;
mod scan;

pub use error::Error;
pub use plan::{LakePosition, Plan, Read};
pub use scan::{Options, project};
