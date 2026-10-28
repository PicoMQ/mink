use mink_core::{Offsets, Partition, millis};
use mink_kafka::Cluster;
use mink_metrics::Source;

use super::{Indexer, file_groups};
use crate::Error;
use crate::config::History;

impl Indexer {
    pub(super) async fn backfill(&mut self) -> Result<(), Error> {
        let mut cluster = self.services.kafka.cluster().await?;
        let earliest = cluster.earliest(&self.partition).await?;
        let Some(target) = self.backfill_target.map(|target| target.max(earliest)) else {
            return Ok(());
        };
        if self.indexed.start_offset() <= target {
            self.backfill_target = None;
            return Ok(());
        }

        let chunk = self.services.config.backfill_offsets;
        let end = self.indexed.start_offset();
        let mut start = (end - chunk).max(target);
        let mut records = cluster
            .read(&Offsets::new(self.partition.clone(), start, end)?)
            .await?;

        while records.is_empty() && start > target {
            start = (start - chunk).max(target);
            records = cluster
                .read(&Offsets::new(self.partition.clone(), start, end)?)
                .await?;
        }
        drop(cluster);

        if records.is_empty() {
            self.backfill_target = None;
            return Ok(());
        }

        let config = &self.services.config;
        let spec = self.current_spec().await?;
        let groups = file_groups(
            &records,
            config.file_bytes,
            millis(config.file_window),
            spec.granularity(),
        );
        let new_files = self
            .write_files(start, end, &records, &groups, spec)
            .await?;
        self.indexed = self.add(&new_files, Source::Backfill).await?;
        Ok(())
    }
}

pub(crate) async fn target(
    cluster: &mut Cluster,
    history: &History,
    partition: &Partition,
    indexed: &Offsets,
    earliest: i64,
) -> Result<Option<i64>, Error> {
    let target = match history {
        History::None => return Ok(None),
        History::All => earliest,
        History::Since(timestamp_ms) => cluster
            .offset_at(partition, *timestamp_ms)
            .await?
            .unwrap_or(indexed.start_offset()),
    };

    Ok((target < indexed.start_offset()).then_some(target))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rdkafka::producer::{BaseRecord, Producer};

    use super::*;
    use crate::indexer::tests::{indexed_files, produce, start};
    use crate::testing::{self, kafka, registered};

    #[tokio::test]
    async fn backfill_prepends_history_down_to_the_target() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let Some(services) = testing::services(|config| {
            config.backfill_offsets = 4;
            config.history = History::Since(5_000);
        })
        .await
        else {
            return;
        };
        let topic = registered(&services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
        let producer = mink_conformance::producer(&bootstrap);
        for index in 0..10_i64 {
            let record = BaseRecord::<(), str>::to(topic.name())
                .partition(0)
                .timestamp((index + 1) * 1_000)
                .payload("v");
            producer.send(record).map_err(|(error, _)| error).unwrap();
        }
        producer.flush(Duration::from_secs(30)).unwrap();
        let lease = testing::partition_lease(&services, &topic, 0).await;
        let mut since = start(services.clone(), lease, &topic).await;

        for _ in 0..5 {
            since.backfill().await.unwrap();
        }

        assert_eq!(indexed_files(&since).await, [(4, 6, 2), (6, 10, 4)]);
        assert_eq!(since.backfill_target, None);
    }

    #[tokio::test]
    async fn chunks_without_committed_records_merge_with_the_one_below() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let Some(services) = testing::services(|config| config.backfill_offsets = 3).await else {
            return;
        };
        let topic = registered(&services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
        produce(&bootstrap, &topic, 2);
        mink_conformance::produce_aborted(
            &bootstrap,
            topic.name(),
            0,
            &["x", "x", "x", "x", "x", "x"],
        );
        let lease = testing::partition_lease(&services, &topic, 0).await;
        let mut indexer = start(services, lease, &topic).await;
        let end = indexer.indexed.end_offset();

        indexer.backfill().await.unwrap();
        indexer.backfill().await.unwrap();

        assert_eq!(indexed_files(&indexer).await, [(0, end, 2)]);
        assert_eq!(indexer.backfill_target, None);
    }
}
