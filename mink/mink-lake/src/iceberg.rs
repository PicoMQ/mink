//! The Iceberg implementation of the lake interfaces.

mod arrow;
mod catalog;
mod commit;
mod committer;
mod compact;
mod memory;
mod predicate;
mod rest;
mod schema;
mod source;
mod write;
mod writer;

pub use catalog::Catalog;
pub use commit::{CommitTarget, Produced, RowDelta, commit, produce};
pub use committer::COMMIT_USER_PROPERTY;
pub use compact::RewriteResult;
pub use schema::{FORMAT_VERSION_OPTION, Spec, from_iceberg, table_schema};
pub use source::Source;
pub use write::{Committable, FileContext, WriteResult};
pub use writer::partition_key;

pub use crate::config::{CatalogKind, Iceberg as Config};
