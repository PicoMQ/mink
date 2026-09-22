//! The per-bucket writer interface, the batches it receives, and the factory that opens writers and committers.

use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use mink_record::Changes;
use mink_table::{Bucket, Descriptor, PartitionName, Path};

use crate::committer::{Committer, CommitterContext};
use crate::error::Result;

pub const COMMIT_USER: &str = "__mink_lake_tiering";

#[derive(Debug, Clone)]
pub struct WriterContext {
    pub path: Path,
    pub bucket: Bucket,
    pub partition: Option<PartitionName>,
    pub descriptor: Arc<Descriptor>,
}

#[derive(Debug, Clone)]
pub struct TieredBatch {
    pub rows: RecordBatch,
    pub changes: Changes,
    pub base_offset: i64,
    pub timestamp_ms: i64,
}

impl TieredBatch {
    pub const NO_OFFSET: i64 = -1;

    pub fn snapshot(rows: RecordBatch) -> Self {
        TieredBatch {
            changes: Changes::AppendOnly(rows.num_rows()),
            rows,
            base_offset: Self::NO_OFFSET,
            timestamp_ms: Self::NO_OFFSET,
        }
    }

    pub fn last_offset(&self) -> i64 {
        if self.base_offset < 0 {
            return Self::NO_OFFSET;
        }
        self.base_offset + self.rows.num_rows() as i64 - 1
    }
}

#[async_trait]
pub trait Writer<R>: Send {
    async fn write(&mut self, batch: &TieredBatch) -> Result<()>;

    async fn complete(self: Box<Self>) -> Result<R>;
}

#[async_trait]
pub trait Factory: Send + Sync {
    type WriteResult: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Committable: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;

    async fn create_writer(
        &self,
        context: WriterContext,
    ) -> Result<Box<dyn Writer<Self::WriteResult>>>;

    async fn create_committer(
        &self,
        context: CommitterContext,
    ) -> Result<Box<dyn Committer<Self::WriteResult, Self::Committable>>>;
}
