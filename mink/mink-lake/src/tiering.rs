//! The coordinator, log-source and bucket-source interfaces the tiering worker and union reader consume,
//! with the coordinator adapter.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use mink_coordinator::tiering::Table;
use mink_metadata::LakeSnapshotRow;
use mink_table::{Bucket, Descriptor, Id, PartitionName, Path};

use crate::error::Result;
use crate::writer::TieredBatch;

#[derive(Debug, Clone)]
pub struct Config {
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            poll_interval: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
        }
    }
}

#[async_trait]
pub trait Coordinator: Send + Sync {
    async fn request_table(&self) -> Result<Option<Table>>;

    async fn heartbeat(&self, table_id: Id, epoch: u64) -> Result<()>;

    async fn finish(&self, table_id: Id, epoch: u64) -> Result<()>;

    async fn fail(&self, table_id: Id, epoch: u64) -> Result<()>;

    fn lake_snapshot(&self, table_id: Id) -> Option<LakeSnapshotRow>;

    async fn commit_lake_snapshot(&self, table_id: Id, snapshot: LakeSnapshotRow) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_id: Id,
    pub path: Path,
    pub descriptor: Arc<Descriptor>,
    pub buckets: Vec<(Bucket, Option<PartitionName>)>,
}

pub struct SnapshotRead {
    pub log_offset: i64,
    pub batches: BoxStream<'static, Result<TieredBatch>>,
}

/// Rows of the log between two offsets; with `columns`, each batch carries exactly those columns in
/// that order.
#[async_trait]
pub trait LogSource: Send + Sync {
    async fn offsets(&self, bucket: Bucket) -> Result<(i64, i64)>;

    fn log(
        &self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
    ) -> BoxStream<'static, Result<TieredBatch>>;
}

#[async_trait]
pub trait BucketSource: LogSource {
    fn table(&self, path: &Path) -> Result<Option<TableInfo>>;

    async fn snapshot(&self, bucket: Bucket) -> Result<SnapshotRead>;
}

#[async_trait]
impl Coordinator for mink_coordinator::Coordinator {
    async fn request_table(&self) -> Result<Option<Table>> {
        if !self.is_leader() {
            return Ok(None);
        }

        Ok(self.request_tiering())
    }

    async fn heartbeat(&self, table_id: Id, epoch: u64) -> Result<()> {
        Ok(self.tiering_heartbeat(table_id, epoch)?)
    }

    async fn finish(&self, table_id: Id, epoch: u64) -> Result<()> {
        Ok(self.finish_tiering(table_id, epoch, false)?)
    }

    async fn fail(&self, table_id: Id, epoch: u64) -> Result<()> {
        Ok(self.fail_tiering(table_id, epoch)?)
    }

    fn lake_snapshot(&self, table_id: Id) -> Option<LakeSnapshotRow> {
        mink_coordinator::Coordinator::lake_snapshot(self, table_id)
    }

    async fn commit_lake_snapshot(&self, table_id: Id, snapshot: LakeSnapshotRow) -> Result<()> {
        Ok(mink_coordinator::Coordinator::commit_lake_snapshot(self, table_id, snapshot).await?)
    }
}
