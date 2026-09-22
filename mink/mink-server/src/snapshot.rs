//! Checkpoints a key-value tablet, uploads it and records it in the catalog under the current epochs.

use std::sync::Arc;

use mink_metadata::{Counter, KvSnapshotRow};
use mink_table::Bucket;

use crate::error::Error;
use crate::node::Node;
use crate::registry::Hosted;

impl Node {
    pub async fn snapshot_all(&self) -> Vec<(Bucket, Result<Option<u64>, Error>)> {
        let mut results = Vec::new();
        for hosted in self.registry().all() {
            if hosted.kv.is_some() {
                let bucket = hosted.bucket;
                let result = self.snapshot(hosted).await;
                results.push((bucket, result));
            }
        }

        results
    }

    pub async fn snapshot(&self, hosted: Arc<Hosted>) -> Result<Option<u64>, Error> {
        let kv = hosted.kv.as_ref().ok_or(Error::NotKvTable(hosted.bucket))?;
        let mut state = kv.snapshots.lock().await;
        if kv.tablet.flushed_log_offset().await <= state.log_offset {
            return Ok(None);
        }

        let snapshot_id = self
            .metadata()
            .allocate(Counter::SnapshotId(hosted.bucket), 1)
            .await?;
        let checkpoint_dir = kv.dir.join(format!("chk-{snapshot_id}"));
        let checkpoint = kv.tablet.checkpoint(&checkpoint_dir).await?;
        let uploaded = state
            .uploader
            .upload(snapshot_id, hosted.bucket, &checkpoint)
            .await;
        let _ = tokio::fs::remove_dir_all(&checkpoint_dir).await;
        let snapshot = uploaded?;

        let coordinator_epoch = self
            .metadata()
            .views()
            .load()
            .state
            .catalog
            .buckets
            .get(&hosted.bucket)
            .map(|row| row.coordinator_epoch)
            .ok_or(Error::BucketNotExist(hosted.bucket))?;
        let row = KvSnapshotRow {
            snapshot_id,
            log_offset: snapshot.log_offset,
            row_count: snapshot.row_count,
            path: snapshot.location.clone(),
        };

        match self
            .metadata()
            .commit_kv_snapshot(hosted.bucket, row, hosted.leader_epoch, coordinator_epoch)
            .await
        {
            Ok(()) => {
                state.uploader.completed(snapshot_id);
                state.log_offset = snapshot.log_offset;

                Ok(Some(snapshot_id))
            }
            Err(e) => {
                state.uploader.aborted(&snapshot).await?;

                Err(e.into())
            }
        }
    }
}
