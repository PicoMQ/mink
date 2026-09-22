//! Deletes a discarded key-value snapshot from object storage, keeping files still shared by retained ones.

use std::sync::Arc;

use async_trait::async_trait;
use mink_coordinator::{Error, SnapshotCleaner};
use mink_metadata::KvSnapshotRow;
use mink_table::Bucket;
use mink_tablet::{CompletedSnapshot, discard};
use object_store::ObjectStore;

pub struct Cleaner {
    storage: Arc<dyn ObjectStore>,
}

impl Cleaner {
    pub fn new(storage: Arc<dyn ObjectStore>) -> Self {
        Cleaner { storage }
    }
}

#[async_trait]
impl SnapshotCleaner for Cleaner {
    async fn discard(
        &self,
        _bucket: Bucket,
        snapshot: &KvSnapshotRow,
        retained: &[KvSnapshotRow],
    ) -> Result<(), Error> {
        let err = |e: mink_tablet::Error| Error::Cleaner(e.to_string());
        let load = |row: &KvSnapshotRow| {
            let path = row.path.clone();
            async move { CompletedSnapshot::load(self.storage.as_ref(), &path).await }
        };

        let snapshot = match load(snapshot).await {
            Ok(snapshot) => snapshot,
            Err(mink_tablet::Error::Storage(object_store::Error::NotFound { .. })) => {
                return Ok(());
            }
            Err(e) => return Err(err(e)),
        };
        let mut kept = Vec::with_capacity(retained.len());
        for row in retained {
            kept.push(load(row).await.map_err(err)?);
        }

        discard(self.storage.as_ref(), &snapshot, &kept)
            .await
            .map_err(err)
    }
}
