use std::collections::HashMap;

use mink_core::{Offsets, Snapshot, millis, now_ms};

use super::Writer;
use crate::Error;

impl Writer {
    pub(super) async fn expire_through_id(&self) -> Result<Option<i64>, Error> {
        let kept = self
            .services
            .store
            .snapshots()
            .list(self.topic.name())
            .await?;

        let mut cluster = self.services.kafka.cluster().await?;
        let mut earliest = HashMap::new();
        for partition in kept
            .last()
            .iter()
            .flat_map(|latest| latest.offsets())
            .map(Offsets::partition)
        {
            earliest.insert(partition.clone(), cluster.earliest(partition).await?);
        }

        let before_ms = now_ms() - millis(self.services.config.snapshot_retention);

        Ok(Snapshot::expired_through(&kept, before_ms, &earliest))
    }
}
