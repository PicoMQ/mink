use std::sync::Arc;
use std::time::Duration;

use mink_core::{Granularity, Offsets, Partition, Record, TimeSpec, millis};
use mink_iceberg::NewFiles;
use mink_metrics::Source;
use mink_store::Lease;
use tokio::time::Instant;

use crate::Error;
use crate::services::Services;

pub(crate) mod backfill;
mod retention;

const POLL: Duration = Duration::from_millis(500);

pub(crate) struct Indexer {
    services: Arc<Services>,
    lease: Lease,
    partition: Partition,
    indexed: Offsets,
    read_end_offset: i64,
    unwritten: Vec<Record>,
    unwritten_since: Option<Instant>,
    backfill_target: Option<i64>,
    last_retention_check: Instant,
}

struct FileGroup {
    start: usize,
    end: usize,
    complete: bool,
}

impl Indexer {
    pub(crate) async fn start(
        services: Arc<Services>,
        lease: Lease,
        partition: Partition,
    ) -> Result<Indexer, Error> {
        let indexed = match services.store.indexed_ranges().get(&partition).await? {
            Some(indexed) => indexed,
            None => {
                let stable = services
                    .kafka
                    .cluster()
                    .await?
                    .last_stable_offset(&partition)
                    .await?;
                services
                    .store
                    .indexed_ranges()
                    .start(&lease, stable)
                    .await?
            }
        };
        let mut cluster = services.kafka.cluster().await?;
        let earliest = cluster.earliest(&partition).await?;
        let history = &services.config.history;
        let backfill_target =
            backfill::target(&mut cluster, history, &partition, &indexed, earliest).await?;
        drop(cluster);

        Ok(Indexer {
            services,
            lease,
            partition,
            read_end_offset: indexed.end_offset(),
            indexed,
            unwritten: Vec::new(),
            unwritten_since: None,
            backfill_target,
            last_retention_check: Instant::now(),
        })
    }

    pub(crate) fn interval(&self) -> Duration {
        POLL
    }

    pub(crate) async fn tick(&mut self) -> Result<(), Error> {
        self.index().await?;

        if self.backfill_target.is_some() {
            self.backfill().await?;
        }

        if self.last_retention_check.elapsed() >= self.services.config.retention_check {
            self.trim_to_retention().await?;
            self.last_retention_check = Instant::now();
        }
        Ok(())
    }

    async fn index(&mut self) -> Result<(), Error> {
        let mut cluster = self.services.kafka.cluster().await?;
        let stable = cluster.last_stable_offset(&self.partition).await?;

        if stable > self.read_end_offset {
            let unread = Offsets::new(self.partition.clone(), self.read_end_offset, stable)?;
            let records = cluster.read(&unread).await?;

            if self.unwritten.is_empty() && !records.is_empty() {
                self.unwritten_since = Some(Instant::now());
            }
            self.unwritten.extend(records);
            self.read_end_offset = stable;
        }
        drop(cluster);

        let config = &self.services.config;
        let window_elapsed = self
            .unwritten_since
            .is_some_and(|since| since.elapsed() >= config.file_window);
        let spec = self.current_spec().await?;
        let mut groups = file_groups(
            &self.unwritten,
            config.file_bytes,
            millis(config.file_window),
            spec.granularity(),
        );
        if !window_elapsed && groups.last().is_some_and(|group| !group.complete) {
            groups.pop();
        }
        let Some(last) = groups.last() else {
            return Ok(());
        };

        let written = last.end;
        let end = self
            .unwritten
            .get(written)
            .map_or(self.read_end_offset, Record::offset);
        let new_files = self
            .write_files(
                self.indexed.end_offset(),
                end,
                &self.unwritten[..written],
                &groups,
                spec,
            )
            .await?;
        self.indexed = self.add(&new_files, Source::Live).await?;

        self.unwritten.drain(..written);
        self.unwritten_since = (!self.unwritten.is_empty()).then(Instant::now);
        Ok(())
    }

    async fn current_spec(&self) -> Result<TimeSpec, Error> {
        let specs = self
            .services
            .store
            .time_specs()
            .list(self.partition.topic())
            .await?;

        Ok(specs.current())
    }

    async fn write_files(
        &self,
        start: i64,
        end: i64,
        records: &[Record],
        groups: &[FileGroup],
        spec: TimeSpec,
    ) -> Result<NewFiles, Error> {
        let topic = self.partition.topic();
        let schema = self
            .services
            .schemas
            .latest_file_schema(topic, records)
            .await?;

        let mut new_files = NewFiles::default();
        let mut file_start = start;

        for (index, group) in groups.iter().enumerate() {
            let file_end = groups
                .get(index + 1)
                .map_or(end, |next| records[next.start].offset());
            let records = &records[group.start..group.end];
            let offsets = Offsets::new(self.partition.clone(), file_start, file_end)?;

            new_files.add_range(&offsets, records, spec, &schema)?;
            file_start = file_end;
        }

        Ok(new_files)
    }

    async fn add(&self, new_files: &NewFiles, source: Source) -> Result<Offsets, Error> {
        let files = self.services.store.files();

        let indexed = files
            .add(&self.lease, new_files.files(), new_files.sketches())
            .await?;

        let count = new_files.files().len() as u64;
        self.services
            .metrics
            .indexed(source, count, new_files.records(), new_files.bytes());
        Ok(indexed)
    }
}

fn file_groups(
    records: &[Record],
    file_bytes: u64,
    window_ms: i64,
    granularity: Granularity,
) -> Vec<FileGroup> {
    let mut groups = Vec::new();
    let mut start = 0;
    let mut bytes = 0;
    let mut latest = None;

    for (index, record) in records.iter().enumerate() {
        let bucket = granularity.bucket(record.timestamp_ms());
        if index > start && bucket > latest {
            groups.push(FileGroup {
                start,
                end: index,
                complete: true,
            });
            start = index;
            bytes = 0;
            latest = None;
        }

        latest = latest.max(bucket);
        bytes += record.size();
        let span = record.timestamp_ms() - records[start].timestamp_ms();

        if bytes >= file_bytes || span >= window_ms {
            groups.push(FileGroup {
                start,
                end: index + 1,
                complete: true,
            });
            start = index + 1;
            bytes = 0;
            latest = None;
        }
    }

    if start < records.len() {
        groups.push(FileGroup {
            start,
            end: records.len(),
            complete: false,
        });
    }
    groups
}

#[cfg(test)]
mod tests {
    use mink_core::{Header, Part, Topic};
    use mink_store::Lease;
    use serde_json::json;

    use super::*;
    use crate::config::History;
    use crate::testing::{self, kafka, registered};
    use crate::{Config, SchemaRegistry};

    pub(super) async fn indexer(
        change: impl FnOnce(&mut Config),
    ) -> Option<(Indexer, Topic, String)> {
        let bootstrap = kafka()?;
        let services = testing::services(change).await?;
        let topic = registered(&services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;

        let lease = testing::partition_lease(&services, &topic, 0).await;
        let indexer = start(services, lease, &topic).await;
        Some((indexer, topic, bootstrap))
    }

    pub(super) async fn start(services: Arc<Services>, lease: Lease, topic: &Topic) -> Indexer {
        let partition = Partition::new(topic.name(), 0).unwrap();

        Indexer::start(services, lease, partition).await.unwrap()
    }

    pub(super) async fn indexed_files(indexer: &Indexer) -> Vec<(i64, i64, u64)> {
        let files = indexer
            .services
            .store
            .files()
            .list(&indexer.indexed)
            .await
            .unwrap();

        files
            .iter()
            .map(|indexed| {
                let file = indexed.file();
                (
                    file.start_offset(),
                    file.end_offset(),
                    indexed.stats().records(),
                )
            })
            .collect()
    }

    pub(super) fn bounds(indexer: &Indexer) -> (i64, i64) {
        (indexer.indexed.start_offset(), indexer.indexed.end_offset())
    }

    pub(super) fn produce(bootstrap: &str, topic: &Topic, count: usize) {
        let values: Vec<String> = (0..count).map(|index| format!("v{index}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();

        mink_conformance::produce_values(bootstrap, topic.name(), 0, &values);
    }

    fn record(offset: i64, timestamp_ms: i64, value: &'static str) -> Record {
        Record::new(
            offset,
            timestamp_ms,
            None,
            Some(bytes::Bytes::from_static(value.as_bytes())),
            vec![Header::new("h", None)],
        )
        .unwrap()
    }

    #[test]
    fn groups_end_by_size_and_by_timestamp_span() {
        let records = [
            record(0, 0, "aa"),
            record(1, 10, "aa"),
            record(2, 20, "a"),
            record(3, 1_000, "a"),
            record(4, 1_010, "a"),
        ];

        let groups = file_groups(&records, 5, 500, Granularity::None);

        let ranges: Vec<_> = groups
            .iter()
            .map(|group| (group.start, group.end, group.complete))
            .collect();
        assert_eq!(ranges, [(0, 2, true), (2, 4, true), (4, 5, false)]);
    }

    #[test]
    fn ranges_end_where_a_later_bucket_begins() {
        let hour = 3_600_000;
        let records = [
            record(0, 5 * hour, "a"),
            record(1, 5 * hour + 1, "a"),
            record(2, 6 * hour, "a"),
            record(3, 5 * hour + 2, "a"),
            record(4, 7 * hour, "a"),
        ];

        let groups = file_groups(&records, 1_000, 2 * hour, Granularity::Hour);

        let ranges: Vec<_> = groups
            .iter()
            .map(|group| (group.start, group.end, group.complete))
            .collect();
        assert_eq!(ranges, [(0, 2, true), (2, 4, true), (4, 5, false)]);
    }

    #[tokio::test]
    async fn files_are_written_by_size_and_by_window() {
        let Some((mut indexer, topic, bootstrap)) = indexer(|config| {
            config.history = History::None;
            config.file_bytes = 4;
            config.file_window = Duration::from_millis(300);
        })
        .await
        else {
            return;
        };

        produce(&bootstrap, &topic, 5);
        indexer.index().await.unwrap();
        let by_size = indexed_files(&indexer).await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        indexer.index().await.unwrap();
        let by_window = indexed_files(&indexer).await;

        assert_eq!(by_size, [(0, 2, 2), (2, 4, 2)]);
        assert_eq!(by_window, [(0, 2, 2), (2, 4, 2), (4, 5, 1)]);
    }

    #[tokio::test]
    async fn a_new_partition_starts_at_its_last_stable_offset() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let Some(services) = testing::services(|config| config.history = History::None).await
        else {
            return;
        };
        let topic = registered(&services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
        produce(&bootstrap, &topic, 3);
        let lease = testing::partition_lease(&services, &topic, 0).await;

        let indexer = start(services, lease, &topic).await;

        assert_eq!(bounds(&indexer), (3, 3));
    }

    #[tokio::test]
    async fn a_stopped_indexer_resumes_without_losing_records() {
        let Some((mut first, topic, bootstrap)) = indexer(|config| {
            config.history = History::None;
            config.file_window = Duration::from_millis(300);
        })
        .await
        else {
            return;
        };
        produce(&bootstrap, &topic, 3);
        first.index().await.unwrap();
        let (services, lease) = (first.services.clone(), first.lease.clone());
        drop(first);

        let mut second = start(services, lease, &topic).await;
        second.index().await.unwrap();
        tokio::time::sleep(Duration::from_millis(350)).await;
        second.index().await.unwrap();

        assert_eq!(indexed_files(&second).await, [(0, 3, 3)]);
    }

    #[tokio::test]
    async fn files_are_written_with_a_schema_that_holds_their_records() {
        let Some(registry) = mink_conformance::service("TEST_SCHEMA_REGISTRY") else {
            return;
        };
        let settings = SchemaRegistry {
            url: registry.clone(),
            username: None,
            password: None,
        };
        let Some((mut indexer, topic, bootstrap)) = indexer(|config| {
            config.history = History::None;
            config.file_window = Duration::from_millis(300);
            config.schema_registry = Some(settings);
        })
        .await
        else {
            return;
        };
        let subject = mink_schema::subject(topic.name(), Part::Value);
        let first =
            json!({"type": "record", "name": "V", "fields": [{"name": "a", "type": "long"}]});
        let second = json!({"type": "record", "name": "V", "fields": [
            {"name": "a", "type": "long"},
            {"name": "b", "type": ["null", "string"], "default": null}]});
        let first = mink_conformance::register_schema(
            &registry,
            &subject,
            &json!({ "schema": first.to_string() }),
        )
        .await;
        let second = mink_conformance::register_schema(
            &registry,
            &subject,
            &json!({ "schema": second.to_string() }),
        )
        .await;

        let records = [
            mink_conformance::prefixed(first, &[10]),
            mink_conformance::prefixed(second, &[12, 2, 2, b'x']),
        ];
        let records: Vec<&[u8]> = records.iter().map(Vec::as_slice).collect();
        mink_conformance::produce_bytes(&bootstrap, topic.name(), 0, &records);
        indexer.index().await.unwrap();
        tokio::time::sleep(Duration::from_millis(350)).await;
        indexer.index().await.unwrap();

        let files = indexer
            .services
            .store
            .files()
            .list(&indexer.indexed)
            .await
            .unwrap();
        let [file] = files.as_slice() else {
            panic!("one file, not {}", files.len());
        };
        assert_eq!(file.file().schema_id(), 2);
        assert_eq!(file.stats().records(), 2);
        assert_eq!(file.stats().nulls()[&9], 2);
        assert_eq!(file.stats().nulls()[&11], 1);
    }
}
