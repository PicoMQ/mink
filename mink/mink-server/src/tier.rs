use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use mink_core::{
    ExternalTable, Offsets, Partition, TierFlush, TieredRange, millis, now_ms, oldest_untiered_ms,
    tier_flushes,
};
use mink_store::Lease;

use crate::Error;
use crate::external;
use crate::services::Services;

pub(crate) struct Tier {
    services: Arc<Services>,
    lease: Lease,
    partition: Partition,
    external: ExternalTable,
    catalog: Arc<external::Catalog>,
}

#[derive(Default)]
pub(crate) struct TierLags {
    oldest_untiered_ms: Mutex<HashMap<Partition, i64>>,
}

impl Tier {
    pub(crate) async fn start(
        services: Arc<Services>,
        lease: Lease,
        partition: Partition,
    ) -> Result<Tier, Error> {
        let external = services
            .store
            .external_tables()
            .require(partition.topic())
            .await?;
        let catalog = services.external_catalog(external.table().catalog())?;

        Ok(Tier {
            services,
            lease,
            partition,
            external,
            catalog,
        })
    }

    pub(crate) fn interval(&self) -> Duration {
        self.external.schedule().interval()
    }

    pub(crate) async fn tick(&mut self) -> Result<(), Error> {
        let store = &self.services.store;
        let lags = &self.services.tier_lags;

        let Some(snapshot) = store.snapshots().latest(self.partition.topic()).await? else {
            return Ok(());
        };
        let Some(indexed) = snapshot.offsets_of(&self.partition) else {
            return Ok(());
        };

        let tiered_end = store.tiered_ranges().end(&self.partition).await?;
        let start = tiered_end.map_or(indexed.start_offset(), |end| {
            end.max(indexed.start_offset())
        });
        if start >= indexed.end_offset() {
            lags.set(&self.partition, None);
            return Ok(());
        }

        let pending = Offsets::new(self.partition.clone(), start, indexed.end_offset())?;
        let files = store.files().list(&pending).await?;
        lags.set(&self.partition, oldest_untiered_ms(&files, start));

        let now = now_ms();
        let schedule = self.external.schedule();
        let flushes = tier_flushes(
            &files,
            start,
            now - millis(schedule.after()),
            now - millis(schedule.after() + schedule.interval()),
            self.services.config.tier_bytes,
        )?;
        if flushes.is_empty() {
            return Ok(());
        }

        let earliest = self
            .services
            .kafka
            .cluster()
            .await?
            .earliest(&self.partition)
            .await?;
        if start < earliest {
            return Ok(());
        }

        let table = self.catalog.load(&self.external).await?;
        for (index, flush) in flushes.iter().enumerate() {
            self.flush(&table, flush).await?;

            if let Some(end) = tiered_end.filter(|end| index == 0 && *end < start) {
                tracing::warn!(
                    partition = ?self.partition,
                    from = end,
                    to = start,
                    "Kafka deleted these offsets before they were tiered; tiering continues after them"
                );
                self.services.metrics.tier_gap((start - end).unsigned_abs());
            }
        }

        Ok(())
    }

    async fn flush(&self, table: &Table, flush: &TierFlush) -> Result<(), Error> {
        let _permit = self.services.tier_flushes.acquire().await;

        let mut written = Vec::new();
        let recorded = self.write_and_record(table, flush, &mut written).await;
        if recorded.is_err() {
            external::delete_data_files(table, &written).await;
        }

        recorded
    }

    async fn write_and_record(
        &self,
        table: &Table,
        flush: &TierFlush,
        written: &mut Vec<DataFile>,
    ) -> Result<(), Error> {
        for read in flush.reads() {
            let offsets = Offsets::new(self.partition.clone(), read.start(), read.end())?;
            let rows = self.record_batch(table, &offsets).await?;
            let prefix = format!("{}-{}-{}", self.partition.index(), read.start(), read.end());

            written.extend(mink_iceberg::write_data_files(table, vec![rows], &prefix).await?);
        }

        let range = flush.range();
        let offsets = Offsets::new(self.partition.clone(), range.start(), range.end())?;
        let spec_id = table.metadata().default_partition_spec().spec_id();
        let encoded = mink_iceberg::encoded_data_files(table, written.clone())?;
        self.services
            .store
            .tiered_ranges()
            .add(
                &self.lease,
                &TieredRange::new(offsets, self.external.generation(), spec_id, encoded),
            )
            .await?;

        Ok(())
    }

    async fn record_batch(&self, table: &Table, offsets: &Offsets) -> Result<RecordBatch, Error> {
        let records = self.services.kafka.cluster().await?.read(offsets).await?;
        let schema = self
            .services
            .schemas
            .current_file_schema(self.partition.topic())
            .await?;
        mink_iceberg::check_schema(schema.schema(), table.metadata().current_schema())?;

        Ok(mink_iceberg::record_batch(
            &self.partition,
            &records,
            &schema,
        )?)
    }
}

impl Drop for Tier {
    fn drop(&mut self) {
        self.services.tier_lags.set(&self.partition, None);
    }
}

impl TierLags {
    fn set(&self, partition: &Partition, oldest_untiered_ms: Option<i64>) {
        let mut oldest = self
            .oldest_untiered_ms
            .lock()
            .expect("the tier lags are never poisoned");

        match oldest_untiered_ms {
            Some(ms) => oldest.insert(partition.clone(), ms),
            None => oldest.remove(partition),
        };
    }

    pub(crate) fn lag(&self, now_ms: i64) -> Duration {
        let oldest = self
            .oldest_untiered_ms
            .lock()
            .expect("the tier lags are never poisoned")
            .values()
            .min()
            .copied();

        let lag_ms = oldest.map_or(0, |oldest| now_ms.saturating_sub(oldest).max(0));
        Duration::from_millis(lag_ms.unsigned_abs())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_array::Int64Array;
    use futures::TryStreamExt;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog as _, TableUpdate};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use uuid::Uuid;

    use crate::Error;
    use crate::buckets::bucket_and_key;
    use crate::testing::{TTL, Tiering, partition_lease, tiering};

    async fn stored_under(tiering: &Tiering, location: &str) -> usize {
        let (bucket, key) = bucket_and_key(location).unwrap();
        let store = tiering.services.buckets.client(bucket).unwrap();
        let listed: Vec<_> = store.list(Some(&key)).try_collect().await.unwrap();

        listed.len()
    }

    #[tokio::test]
    async fn tiered_files_follow_the_external_table() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiering.index_and_commit(&[3, 2]).await;

        tiering.tier().await.tick().await.unwrap();

        let ranges = tiering
            .services
            .store
            .tiered_ranges()
            .list(tiering.topic.name())
            .await
            .unwrap();
        let table = tiering
            .catalog
            .load_table(&tiering.identifier)
            .await
            .unwrap();
        let metadata = table.metadata();
        assert_eq!(
            metadata.current_schema().as_struct(),
            mink_schema::binary_schema().as_struct()
        );
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].offsets().start_offset(), 0);
        assert_eq!(ranges[0].offsets().end_offset(), 6);
        let files =
            mink_iceberg::data_files(&table, ranges[0].spec_id(), ranges[0].encoded_data_files())
                .unwrap();
        assert_eq!(files.len(), 2);

        for file in files {
            let bytes = table
                .file_io()
                .new_input(file.file_path())
                .unwrap()
                .read()
                .await
                .unwrap();
            let rows = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let offsets = rows
                .column_by_name("_kafka_offset")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec();

            let name = file.file_path().rsplit('/').next().unwrap();
            assert!(file.file_path().starts_with(metadata.location()));
            assert!(name.starts_with("0-0-6-"), "{name}");
            assert_eq!(file.record_count(), 3);
            assert_eq!(file.sort_order_id(), Some(1));
            assert!(
                offsets.windows(2).all(|pair| pair[0] > pair[1]),
                "{offsets:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_second_tick_continues_from_the_last_range() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiering.index_and_commit(&[3]).await;
        let mut tier = tiering.tier().await;
        tier.tick().await.unwrap();

        tiering.index_and_commit(&[2]).await;
        tier.tick().await.unwrap();

        let ranges = tiering.services.store.tiered_ranges();
        let pending = ranges.list(tiering.topic.name()).await.unwrap();
        let spans: Vec<_> = pending
            .iter()
            .map(|range| (range.offsets().start_offset(), range.offsets().end_offset()))
            .collect();
        assert_eq!(spans, [(3, 6)]);
        assert_eq!(
            ranges.committed(tiering.topic.name()).await.unwrap(),
            std::collections::BTreeMap::from([(0, 3)])
        );
    }

    #[tokio::test]
    async fn a_small_range_waits_until_it_is_overdue() {
        let Some(tiering) = tiering(Duration::from_secs(30 * 86_400)).await else {
            return;
        };
        tiering.index_and_commit(&[3]).await;

        tiering.tier().await.tick().await.unwrap();

        let ranges = tiering
            .services
            .store
            .tiered_ranges()
            .list(tiering.topic.name())
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn a_fenced_tier_writes_nothing() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiering.index_and_commit(&[3]).await;
        let mut tier = tiering.tier().await;
        let table = tiering
            .catalog
            .load_table(&tiering.identifier)
            .await
            .unwrap();
        let data = format!("{}/data", table.metadata().location());

        let leases = tiering.services.store.leases();
        leases.release(&tier.lease).await.unwrap();
        leases
            .acquire(&tier.lease.scope().clone(), Uuid::from_u128(4), TTL)
            .await
            .unwrap()
            .unwrap();
        let ran = tier.tick().await;

        let ranges = tiering
            .services
            .store
            .tiered_ranges()
            .list(tiering.topic.name())
            .await
            .unwrap();
        assert!(matches!(ran, Err(Error::Store(_))), "{ran:?}");
        assert!(ranges.is_empty());
        assert_eq!(stored_under(&tiering, &data).await, 0);
    }

    #[tokio::test]
    async fn a_schema_the_topic_does_not_have_stops_tiering() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiering.index_and_commit(&[3]).await;
        let binary = mink_schema::binary_schema();
        let mut fields: Vec<_> = binary.as_struct().fields().to_vec();
        fields.push(
            NestedField::optional(10, "extra", Type::Primitive(PrimitiveType::String)).into(),
        );
        let schema = Schema::builder().with_fields(fields).build().unwrap();
        tiering
            .catalog
            .commit(
                &tiering.identifier,
                Vec::new(),
                vec![
                    TableUpdate::AddSchema { schema },
                    TableUpdate::SetCurrentSchema { schema_id: -1 },
                ],
            )
            .await
            .unwrap();

        let ran = tiering.tier().await.tick().await;

        assert!(
            matches!(
                ran,
                Err(Error::Iceberg(mink_iceberg::Error::ExternalSchema))
            ),
            "{ran:?}"
        );
    }

    #[tokio::test]
    async fn tiering_waits_while_kafka_is_past_the_indexed_start() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiering.index_and_commit(&[3]).await;
        mink_conformance::delete_records(&tiering.bootstrap, tiering.topic.name(), 0, 1).await;

        tiering.tier().await.tick().await.unwrap();

        let ranges = tiering.services.store.tiered_ranges();
        assert!(ranges.list(tiering.topic.name()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn tiering_continues_after_offsets_kafka_deleted_and_counts_the_gap() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        let topic = tiering.topic.name();
        tiering.index_and_commit(&[3]).await;
        tiering.tier().await.tick().await.unwrap();
        tiering.writer().await.tick().await.unwrap();
        tiering.index_and_commit(&[2]).await;
        tiering.index_and_commit(&[1]).await;
        mink_conformance::delete_records(&tiering.bootstrap, topic, 0, 6).await;
        let indexer = partition_lease(&tiering.services, &tiering.topic, 0).await;
        tiering
            .services
            .store
            .indexed_ranges()
            .trim(&indexer, 6)
            .await
            .unwrap();
        tiering.writer().await.tick().await.unwrap();

        tiering.tier().await.tick().await.unwrap();

        let spans: Vec<_> = tiering
            .services
            .store
            .tiered_ranges()
            .list(topic)
            .await
            .unwrap()
            .iter()
            .map(|range| (range.offsets().start_offset(), range.offsets().end_offset()))
            .collect();
        tiering.writer().await.tick().await.unwrap();
        let table = tiering
            .catalog
            .load_table(&tiering.identifier)
            .await
            .unwrap();
        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        let metrics = tiering.services.metrics.render();
        assert_eq!(spans, [(6, 9)]);
        assert_eq!(summary[&mink_iceberg::start_offset_key(0)], "6");
        assert_eq!(summary[&mink_iceberg::end_offset_key(0)], "9");
        assert!(metrics.contains("mink_tier_gaps_total 1"), "{metrics}");
        assert!(
            metrics.contains("mink_tier_skipped_offsets_total 3"),
            "{metrics}"
        );
    }

    #[tokio::test]
    async fn the_lag_is_the_age_of_the_oldest_untiered_record() {
        let Some(tiering) = tiering(Duration::from_secs(30 * 86_400)).await else {
            return;
        };
        tiering.index_and_commit(&[3]).await;
        let mut tier = tiering.tier().await;

        tier.tick().await.unwrap();
        let lag = tiering.services.tier_lags.lag(mink_core::now_ms());
        drop(tier);

        assert!(lag >= Duration::from_secs(3 * 86_400 - 60), "{lag:?}");
        assert_eq!(
            tiering.services.tier_lags.lag(mink_core::now_ms()),
            Duration::ZERO
        );
    }
}
