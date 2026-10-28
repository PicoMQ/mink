use std::sync::Arc;
use std::time::Duration;

use mink_core::{FieldSketches, File, IndexedFile, Snapshot, Topic, now_ms};
use mink_iceberg::{FormatVersion, Table};
use mink_metrics::Outcome;
use mink_store::Lease;
use tokio::time::Instant;

use crate::Error;
use crate::services::Services;

mod assembly;
mod collection;
mod expiry;

pub(crate) struct Writer {
    services: Arc<Services>,
    lease: Lease,
    topic: Topic,
    table: Table,
    last_collection: Instant,
    last_statistics: Option<Instant>,
}

impl Writer {
    pub(crate) async fn start(
        services: Arc<Services>,
        lease: Lease,
        name: String,
    ) -> Result<Writer, Error> {
        let topic = services
            .store
            .topics()
            .get(&name)
            .await?
            .ok_or(Error::NotIndexing(name))?;

        let table = match services.store.snapshots().latest(topic.name()).await? {
            Some(latest) => services.warehouse.load_table(&topic, &latest).await?,
            None => Table::new(FormatVersion::V2, services.warehouse.root(), &topic),
        };

        Ok(Writer {
            services,
            lease,
            topic,
            table,
            last_collection: Instant::now(),
            last_statistics: None,
        })
    }

    pub(crate) fn interval(&self) -> Duration {
        self.services.config.snapshot_interval
    }

    pub(crate) async fn tick(&mut self) -> Result<(), Error> {
        let started = Instant::now();

        let committed = self.commit().await;

        if !matches!(committed, Ok(None)) {
            let outcome = Outcome::of(&committed);
            self.services.metrics.commit(outcome, started.elapsed());
        }
        committed?;

        if self.last_collection.elapsed() >= self.services.config.collection_interval {
            self.collect().await?;
            self.last_collection = Instant::now();
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<Option<Snapshot>, Error> {
        let store = &self.services.store;
        let name = self.topic.name();

        let offsets = store.indexed_ranges().list(name).await?;
        if offsets.is_empty() {
            return Ok(None);
        }

        let latest = store.snapshots().latest(name).await?;
        let snapshot = Snapshot::new(
            Snapshot::next_id(latest.as_ref().map(Snapshot::id)),
            now_ms(),
            self.lease.epoch(),
            offsets,
        )?;
        let assemblies = match &latest {
            Some(latest) => self.assemblies(latest, &snapshot).await?,
            None => Vec::new(),
        };
        let statistics_due = self
            .last_statistics
            .is_none_or(|last| last.elapsed() >= self.services.config.statistics_interval);
        let sketches = if statistics_due {
            self.sketches().await?
        } else {
            None
        };

        let unchanged = latest
            .as_ref()
            .is_some_and(|latest| latest.offsets() == snapshot.offsets());
        if unchanged && assemblies.is_empty() && sketches.is_none() {
            return Ok(None);
        }

        let expire_through_id = self.expire_through_id().await?;
        let table = match expire_through_id {
            Some(through_id) => self.table.expire(through_id)?,
            None => self.table.clone(),
        };

        let mut added = self.new_files(&snapshot, latest.as_ref()).await?;
        added.extend(
            assemblies
                .iter()
                .map(|assembly| assembly.built().indexed().clone()),
        );
        let replaced: Vec<File> = assemblies
            .iter()
            .flat_map(|assembly| assembly.replaced().iter().cloned())
            .collect();

        let specs = store.time_specs().list(name).await?;
        let schemas = store.schemas().table_schemas(name).await?;
        let change = table
            .apply(
                &snapshot,
                &added,
                &replaced,
                sketches.as_ref(),
                &specs,
                &schemas,
            )
            .await?;
        let objects = self.services.warehouse.write_all(&change).await?;
        store
            .snapshots()
            .commit(
                &self.lease,
                &snapshot,
                &objects,
                change.released(),
                &assemblies,
            )
            .await?;
        self.services.metrics.assembled(assemblies.len() as u64);

        if let Some(through_id) = expire_through_id {
            let expired = store.snapshots().expire(&self.lease, through_id).await?;
            self.services.metrics.expired(expired);
        }
        if sketches.is_some() {
            store.sketches().trim(&self.lease).await?;
            self.last_statistics = Some(Instant::now());
        }
        self.table = change.into_table();
        Ok(Some(snapshot))
    }

    async fn sketches(&self) -> Result<Option<FieldSketches>, Error> {
        let stored = self
            .services
            .store
            .sketches()
            .list(self.topic.name())
            .await?;
        if stored.is_empty() {
            return Ok(None);
        }

        Ok(Some(stored.by_field()?))
    }

    async fn new_files(
        &self,
        snapshot: &Snapshot,
        latest: Option<&Snapshot>,
    ) -> Result<Vec<IndexedFile>, Error> {
        let mut new_files = Vec::new();

        for offsets in snapshot.offsets() {
            let files = self.services.store.files().list(offsets).await?;
            new_files.extend(
                files
                    .into_iter()
                    .filter(|indexed| !latest.is_some_and(|latest| latest.covers(indexed.file()))),
            );
        }

        Ok(new_files)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mink_core::{DaySketches, Offsets, Partition, Record, Scope, TableLocation};
    use mink_iceberg::{FileSchema, NewFiles};
    use uuid::Uuid;

    use super::*;
    use crate::Config;
    use crate::testing::{self, TTL, kafka, registered};

    pub(super) const WRITER: Uuid = Uuid::from_u128(1);
    const NEXT_WRITER: Uuid = Uuid::from_u128(2);

    pub(super) struct Setup {
        pub(super) services: Arc<Services>,
        pub(super) topic: Topic,
        indexers: Vec<Lease>,
    }

    pub(super) async fn setup(partitions: i32, change: impl FnOnce(&mut Config)) -> Option<Setup> {
        let bootstrap = kafka()?;
        let services = testing::services(change).await?;
        let topic = registered(&services, partitions).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), partitions).await;

        let mut indexers = Vec::new();
        for index in 0..partitions {
            let lease = testing::partition_lease(&services, &topic, index).await;
            services
                .store
                .indexed_ranges()
                .start(&lease, 0)
                .await
                .unwrap();
            indexers.push(lease);
        }

        Some(Setup {
            services,
            topic,
            indexers,
        })
    }

    impl Setup {
        pub(super) async fn index(&self, partition: i32, start: i64, end: i64) {
            let file = mink_conformance::file(self.topic.name(), partition, start, end);

            self.services
                .store
                .files()
                .add(
                    &self.indexers[partition as usize],
                    &mink_conformance::built(&[file]),
                    &DaySketches::default(),
                )
                .await
                .unwrap();
        }

        pub(super) async fn index_hour(&self, start: i64, end: i64, hour: i64) {
            let records: Vec<Record> = (start..end)
                .map(|offset| {
                    let timestamp = hour * 3_600_000 + offset;
                    let value = Some(bytes::Bytes::from(format!("v{offset}")));

                    Record::new(offset, timestamp, None, value, Vec::new()).unwrap()
                })
                .collect();
            let partition = Partition::new(self.topic.name(), 0).unwrap();
            let offsets = Offsets::new(partition, start, end).unwrap();
            let spec = self
                .services
                .store
                .time_specs()
                .list(self.topic.name())
                .await
                .unwrap()
                .current();
            let mut new_files = NewFiles::default();
            new_files
                .add_range(&offsets, &records, spec, &FileSchema::Binary)
                .unwrap();

            self.services
                .store
                .files()
                .add(&self.indexers[0], new_files.files(), new_files.sketches())
                .await
                .unwrap();
        }

        pub(super) async fn writer(&self, holder: Uuid, ttl: Duration) -> Writer {
            let scope = Scope::Topic(self.topic.name().to_owned());
            let lease = self
                .services
                .store
                .leases()
                .acquire(&scope, holder, ttl)
                .await
                .unwrap()
                .unwrap();

            Writer::start(self.services.clone(), lease, self.topic.name().to_owned())
                .await
                .unwrap()
        }

        pub(super) async fn in_warehouse(&self) -> Vec<String> {
            let metadata = TableLocation::new(self.services.warehouse.root(), self.topic.name())
                .metadata_directory();
            let mut paths: Vec<_> = self
                .services
                .warehouse
                .list(&metadata)
                .await
                .unwrap()
                .iter()
                .map(|listed| listed.path().to_owned())
                .collect();
            paths.sort();

            paths
        }

        pub(super) async fn recorded(&self) -> Vec<String> {
            let mut paths: Vec<_> = self
                .services
                .store
                .objects()
                .list(self.topic.name())
                .await
                .unwrap()
                .iter()
                .map(|object| object.path().to_owned())
                .collect();
            paths.sort();

            paths
        }
    }

    #[tokio::test]
    async fn a_snapshot_covers_the_indexed_ranges() {
        let Some(setup) = setup(2, |_| {}).await else {
            return;
        };
        setup.index(0, 0, 10).await;
        setup.index(0, 10, 20).await;
        let mut writer = setup.writer(WRITER, TTL).await;

        let snapshot = writer.commit().await.unwrap().unwrap();

        let offsets: Vec<_> = snapshot
            .offsets()
            .iter()
            .map(|offsets| {
                (
                    offsets.partition().index(),
                    offsets.start_offset(),
                    offsets.end_offset(),
                )
            })
            .collect();
        let files = setup
            .services
            .store
            .snapshots()
            .files(&snapshot)
            .await
            .unwrap();
        let loaded = setup
            .services
            .warehouse
            .load_table(&setup.topic, &snapshot)
            .await;
        assert_eq!(offsets, [(0, 0, 20), (1, 0, 0)]);
        assert_eq!(files.len(), 2);
        assert!(loaded.is_ok(), "{loaded:?}");
    }

    #[tokio::test]
    async fn nothing_is_committed_when_nothing_was_indexed() {
        let Some(setup) = setup(1, |_| {}).await else {
            return;
        };
        setup.index(0, 0, 10).await;
        let mut writer = setup.writer(WRITER, TTL).await;

        let first = writer.commit().await.unwrap();
        let second = writer.commit().await.unwrap();

        assert!(first.is_some());
        assert!(second.is_none());
        assert_eq!(
            setup
                .services
                .store
                .snapshots()
                .list(setup.topic.name())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn objects_reach_the_warehouse_before_the_commit() {
        let Some(setup) = setup(1, |_| {}).await else {
            return;
        };
        setup.index(0, 0, 10).await;
        let mut stale = setup.writer(WRITER, Duration::ZERO).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut current = setup.writer(NEXT_WRITER, TTL).await;

        let fenced = stale.commit().await;
        let unrecorded = setup.in_warehouse().await;
        current.commit().await.unwrap();

        assert!(
            matches!(fenced, Err(Error::Store(mink_store::Error::Fenced(_)))),
            "{fenced:?}"
        );
        assert_eq!(unrecorded.len(), 3);
        let recorded = setup.recorded().await;
        let warehouse = setup.in_warehouse().await;
        assert!(recorded.iter().all(|path| warehouse.contains(path)));
    }

    #[tokio::test]
    async fn a_fenced_writer_stops_and_its_successor_loads_the_table() {
        let Some(setup) = setup(1, |_| {}).await else {
            return;
        };
        setup.index(0, 0, 10).await;
        let mut first = setup.writer(WRITER, Duration::from_millis(200)).await;
        first.commit().await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        setup.index(0, 10, 20).await;

        let mut second = setup.writer(NEXT_WRITER, TTL).await;
        let committed = second.commit().await.unwrap().unwrap();
        setup.index(0, 20, 30).await;
        let fenced = first.commit().await;

        assert_eq!((committed.id(), committed.epoch()), (2, 2));
        assert!(
            matches!(
                fenced,
                Err(Error::Iceberg(mink_iceberg::Error::Conflict {
                    expected: 2,
                    given: 3
                }))
            ),
            "{fenced:?}"
        );
        let snapshots = setup
            .services
            .store
            .snapshots()
            .list(setup.topic.name())
            .await
            .unwrap();
        assert_eq!(snapshots.len(), 2);
        let table = setup
            .services
            .warehouse
            .load_table(&setup.topic, &committed)
            .await;
        assert!(table.is_ok(), "{table:?}");
    }
}
