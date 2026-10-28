use mink_core::{TableLocation, millis, now_ms};

use super::Writer;
use crate::Error;

impl Writer {
    pub(super) async fn collect(&mut self) -> Result<(), Error> {
        let store = &self.services.store;
        let warehouse = &self.services.warehouse;
        let grace = self.services.config.collection_grace;

        let collectable = store
            .objects()
            .collectable(self.topic.name(), grace)
            .await?;
        for object in &collectable {
            warehouse.delete(object.path()).await?;
            self.services.metrics.collected(1);
        }

        store.objects().remove(&self.lease, &collectable).await?;

        let recorded = store.objects().list(self.topic.name()).await?;
        let before_ms = now_ms() - millis(grace);
        let prefix = TableLocation::new(warehouse.root(), self.topic.name()).metadata_directory();
        for listed in warehouse.list(&prefix).await? {
            let known = recorded.iter().any(|object| object.path() == listed.path());
            if !known && listed.modified_ms() <= before_ms {
                warehouse.delete(listed.path()).await?;
                self.services.metrics.collected(1);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::TTL;
    use crate::writer::tests::{WRITER, setup};

    #[tokio::test]
    async fn collection_deletes_unreferenced_and_unrecorded_objects() {
        let Some(setup) = setup(1, |config| {
            config.snapshot_retention = Duration::from_millis(1);
            config.collection_grace = Duration::from_millis(1);
        })
        .await
        else {
            return;
        };
        let mut writer = setup.writer(WRITER, TTL).await;
        for end in [10, 20, 30] {
            setup.index(0, end - 10, end).await;
            writer.commit().await.unwrap().unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let unrecorded = format!(
            "{}unrecorded.avro",
            TableLocation::new(setup.services.warehouse.root(), setup.topic.name())
                .metadata_directory()
        );
        setup
            .services
            .warehouse
            .write(&unrecorded, bytes::Bytes::from_static(b"x"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        writer.collect().await.unwrap();

        let warehouse = setup.in_warehouse().await;
        let recorded = setup.recorded().await;
        assert_eq!(warehouse, recorded);
        assert!(!warehouse.contains(&unrecorded));
        assert!(
            !warehouse
                .iter()
                .any(|path| path.ends_with("/v1-e1.metadata.json"))
        );
        assert!(
            warehouse
                .iter()
                .any(|path| path.ends_with("/v3-e1.metadata.json"))
        );
    }
}
