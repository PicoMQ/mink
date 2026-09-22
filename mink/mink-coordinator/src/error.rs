//! Every failure of a coordinator operation, wrapping metadata, table and lake errors.

use mink_table::{Id, Path};

use crate::lake;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Metadata(#[from] mink_metadata::Error),
    #[error(transparent)]
    Table(#[from] mink_table::Error),
    #[error(transparent)]
    Lake(#[from] lake::Error),
    #[error("no coordinator is registered")]
    NoCoordinator,
    #[error("no live node can lead buckets")]
    NoLiveNodes,
    #[error("table {0} is not partitioned")]
    NotPartitioned(Path),
    #[error("table {0} is not tiered into a lake")]
    NotLakeTable(Id),
    #[error("table {0} is not being tiered")]
    NotTiering(Id),
    #[error("tiering epoch {given} for table {id} is not the current {current}")]
    TieringFenced { id: Id, current: u64, given: u64 },
    #[error("membership: {0}")]
    Membership(String),
    #[error("snapshot cleaner: {0}")]
    Cleaner(String),
}
