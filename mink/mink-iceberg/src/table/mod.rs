mod commit;
mod manifests;
mod render;
mod specs;
mod statistics;
#[cfg(test)]
mod testing;

use std::future::Future;

use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::{
    ManifestEntry, ManifestFile, ManifestList, MetadataLog, Schema, Snapshot as IcebergSnapshot,
    StatisticsFile, TableMetadata,
};
use mink_core::{
    FieldSketches, File, IndexedFile, Object, ObjectKind, Offsets, Snapshot, TableLocation,
    TimeSpecs, Topic,
};
use uuid::Uuid;

use crate::Error;
use commit::{Commit, CommitOutput, Counts, summary};
use manifests::Manifest;

pub use commit::FIRST_TAIL_SNAPSHOT_ID;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatVersion {
    V2,
}

#[derive(Debug, Clone)]
pub struct Table {
    version: FormatVersion,
    uuid: Uuid,
    topic: String,
    warehouse: String,
    statistics: Option<StatisticsFile>,
    snapshots: Vec<IcebergSnapshot>,
    metadata_log: Vec<MetadataLog>,
    manifests: Vec<Manifest>,
}

#[derive(Debug)]
pub struct Change {
    table: Table,
    objects: Vec<(Object, Bytes)>,
    metadata: (Object, Bytes),
    released: Vec<Object>,
}

pub trait ObjectReader {
    fn read(&self, path: &str) -> impl Future<Output = Result<Bytes, Error>> + Send;
}

impl FormatVersion {
    fn number(self) -> u8 {
        match self {
            FormatVersion::V2 => 2,
        }
    }

    pub fn iceberg(self) -> iceberg::spec::FormatVersion {
        match self {
            FormatVersion::V2 => iceberg::spec::FormatVersion::V2,
        }
    }

    fn from_iceberg(version: iceberg::spec::FormatVersion) -> Result<FormatVersion, Error> {
        match version {
            iceberg::spec::FormatVersion::V2 => Ok(FormatVersion::V2),
            other => Err(Error::FormatVersion(other as u8)),
        }
    }
}

impl Change {
    pub fn objects(&self) -> impl Iterator<Item = &(Object, Bytes)> {
        self.objects.iter().chain([&self.metadata])
    }

    pub fn metadata(&self) -> &(Object, Bytes) {
        &self.metadata
    }

    pub fn released(&self) -> &[Object] {
        &self.released
    }

    pub fn into_table(self) -> Table {
        self.table
    }
}

impl Table {
    pub fn new(version: FormatVersion, warehouse: &str, topic: &Topic) -> Table {
        Table {
            version,
            uuid: topic.uuid(),
            topic: topic.name().to_owned(),
            warehouse: warehouse.to_owned(),
            statistics: None,
            snapshots: Vec::new(),
            metadata_log: Vec::new(),
            manifests: Vec::new(),
        }
    }

    pub async fn load(
        reader: &impl ObjectReader,
        warehouse: &str,
        topic: &Topic,
        snapshot: &Snapshot,
    ) -> Result<Table, Error> {
        let location = TableLocation::new(warehouse, topic.name()).metadata_file(&snapshot.key());
        let bytes = reader.read(&location).await?;
        let metadata: TableMetadata = serde_json::from_slice(&bytes)?;

        let version = FormatVersion::from_iceberg(metadata.format_version())?;
        let mut table = Table::new(version, warehouse, topic);
        if metadata.uuid() != table.uuid || metadata.location() != table.location().path() {
            return Err(Error::Corrupt(format!(
                "{location} is not the table of topic {}",
                topic.name()
            )));
        }

        let current = metadata
            .current_snapshot()
            .filter(|current| current.snapshot_id() == snapshot.id())
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "{location} does not end at snapshot {}",
                    snapshot.id()
                ))
            })?;

        let io = FileIO::new_with_memory();
        let list = reader.read(current.manifest_list()).await?;
        let files: Vec<ManifestFile> =
            ManifestList::parse_with_version(&list, metadata.format_version())?
                .consume_entries()
                .into_iter()
                .collect();

        for file in files {
            let bytes = reader.read(&file.manifest_path).await?;
            io.new_output(&file.manifest_path)?.write(bytes).await?;

            let manifest = file.load_manifest(&io).await?;
            let live: Vec<(File, ManifestEntry)> = manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive())
                .map(|entry| Ok((table.file_of(entry.data_file())?, (**entry).clone())))
                .collect::<Result<_, Error>>()?;
            table.manifests.push(Manifest { file, live });
        }

        table.statistics = metadata
            .statistics_iter()
            .max_by_key(|statistics| statistics.snapshot_id)
            .cloned();
        table.snapshots = metadata
            .snapshots()
            .map(|snapshot| (**snapshot).clone())
            .collect();
        table.snapshots.sort_by_key(IcebergSnapshot::snapshot_id);
        table.metadata_log = metadata.metadata_log().to_vec();
        table.metadata_log.push(MetadataLog {
            metadata_file: location,
            timestamp_ms: current.timestamp_ms(),
        });

        Ok(table)
    }

    fn location(&self) -> TableLocation {
        TableLocation::new(&self.warehouse, &self.topic)
    }

    pub fn metadata_location(&self, snapshot: &Snapshot) -> String {
        self.location().metadata_file(&snapshot.key())
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshots.last().map(IcebergSnapshot::snapshot_id)
    }

    pub async fn apply(
        &self,
        snapshot: &Snapshot,
        added: &[IndexedFile],
        replaced: &[File],
        sketches: Option<&FieldSketches>,
        specs: &TimeSpecs,
        schemas: &[Schema],
    ) -> Result<Change, Error> {
        self.validate(snapshot, added)?;

        let commit = Commit::committed(snapshot, specs, schemas);
        let mut change = self.write(&commit, added, replaced, sketches).await?;

        change.released.splice(0..0, self.released_by_next());
        Ok(change)
    }

    pub async fn tail(
        &self,
        committed: &Snapshot,
        offsets: Vec<Offsets>,
        added: &[IndexedFile],
        specs: &TimeSpecs,
        schemas: &[Schema],
    ) -> Result<Change, Error> {
        let latest = self.snapshot_id();
        if latest != Some(committed.id()) {
            return Err(Error::Conflict {
                expected: latest.unwrap_or(0),
                given: committed.id(),
            });
        }

        for before in committed.offsets() {
            let extends = offsets.iter().any(|after| {
                after.partition() == before.partition()
                    && after.start_offset() == before.start_offset()
                    && after.end_offset() >= before.end_offset()
            });

            if !extends {
                return Err(Error::TailOffsets(before.partition().clone()));
            }
        }

        let commit = Commit::tail(committed, offsets, specs, schemas);
        let before_tail = |file: &File| {
            committed
                .offsets_of(file.partition())
                .is_some_and(|offsets| file.start_offset() < offsets.end_offset())
        };
        if let Some(indexed) = added
            .iter()
            .find(|indexed| before_tail(indexed.file()) || !commit.covers(indexed.file()))
        {
            return Err(Error::FileOutsideSnapshot(indexed.file().clone()));
        }

        let change = self.write(&commit, added, &[], None).await?;

        Ok(Change {
            released: Vec::new(),
            ..change
        })
    }

    async fn write(
        &self,
        commit: &Commit<'_>,
        added: &[IndexedFile],
        replaced: &[File],
        sketches: Option<&FieldSketches>,
    ) -> Result<Change, Error> {
        let io = FileIO::new_with_memory();
        let mut output = CommitOutput::default();

        let statistics = self.statistics(&io, commit, sketches, &mut output).await?;

        let reused = self
            .write_manifests(&io, commit, added, replaced, &mut output)
            .await?;

        let CommitOutput {
            mut objects,
            mut manifests,
            released,
            additions,
            deletions,
        } = output;
        manifests.extend(reused);
        objects.push(self.write_list(&io, commit, &mut manifests).await?);

        let mut table = self.with_snapshot(commit, manifests, &additions, &deletions);
        table.statistics = statistics;

        let metadata = (
            Object::new(
                self.location().metadata_file(&commit.key),
                ObjectKind::TableMetadata,
            ),
            Bytes::from(table.render(commit)?),
        );

        Ok(Change {
            table,
            objects,
            metadata,
            released,
        })
    }

    fn released_by_next(&self) -> Vec<Object> {
        let mut released = Vec::new();

        if let Some(metadata) = self.metadata_log.last() {
            released.push(Object::new(
                &metadata.metadata_file,
                ObjectKind::TableMetadata,
            ));
        }

        if let Some(latest) = self.snapshots.last() {
            released.push(Object::new(
                latest.manifest_list(),
                ObjectKind::ManifestList,
            ));
        }

        released
    }

    fn with_snapshot(
        &self,
        commit: &Commit<'_>,
        manifests: Vec<Manifest>,
        additions: &Counts,
        deletions: &Counts,
    ) -> Table {
        let mut totals = Counts::default();
        for (_, entry) in manifests.iter().flat_map(|manifest| &manifest.live) {
            totals.add(entry.data_file());
        }

        let iceberg_snapshot = IcebergSnapshot::builder()
            .with_snapshot_id(commit.snapshot_id)
            .with_parent_snapshot_id(self.snapshot_id())
            .with_sequence_number(commit.sequence_number)
            .with_timestamp_ms(commit.timestamp_ms)
            .with_manifest_list(self.location().manifest_list(&commit.key))
            .with_summary(summary(&commit.offsets, additions, deletions, &totals))
            .with_schema_id(commit.current_schema().schema_id())
            .build();

        let mut table = self.clone();
        table.snapshots.push(iceberg_snapshot);
        table.metadata_log.push(MetadataLog {
            metadata_file: self.location().metadata_file(&commit.key),
            timestamp_ms: commit.timestamp_ms,
        });
        table.manifests = manifests;
        table
    }

    pub fn expire(&self, through_id: i64) -> Result<Table, Error> {
        let latest = self.snapshot_id().unwrap_or(0);
        if through_id >= latest {
            return Err(Error::LatestSnapshot { id: through_id });
        }

        let mut table = self.clone();
        let kept = table
            .snapshots
            .iter()
            .position(|snapshot| snapshot.snapshot_id() > through_id)
            .unwrap_or(0);

        table.snapshots.drain(..kept);
        table.metadata_log.drain(..kept);
        Ok(table)
    }

    fn validate(&self, snapshot: &Snapshot, added: &[IndexedFile]) -> Result<(), Error> {
        if snapshot.topic() != self.topic {
            return Err(Error::Topic {
                table: self.topic.clone(),
                snapshot: snapshot.topic().to_owned(),
            });
        }

        let expected = Snapshot::next_id(self.snapshot_id());
        if snapshot.id() != expected {
            return Err(Error::Conflict {
                expected,
                given: snapshot.id(),
            });
        }

        if let Some(indexed) = added
            .iter()
            .find(|indexed| !snapshot.covers(indexed.file()))
        {
            return Err(Error::FileOutsideSnapshot(indexed.file().clone()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iceberg::io::FileIO;
    use iceberg::spec::TableMetadata;
    use mink_conformance::{WAREHOUSE, metadata_fixture};
    use mink_core::{Partition, Stats, TimePartition};
    use serde_json::{Value, json};

    use super::testing::{applied, file, listed, snapshot, specs, store, topic, written};
    use super::*;

    #[tokio::test]
    async fn metadata_matches_the_fixtures() {
        let fixture = metadata_fixture();
        let specs = specs();
        let mut table = Table::new(FormatVersion::V2, fixture.warehouse(), fixture.topic());
        assert!(!fixture.steps().is_empty(), "no metadata fixtures");

        for step in fixture.steps() {
            let id = step.snapshot().id();
            if let Some(through_id) = step.expire_through() {
                table = table.expire(through_id).unwrap();
            }

            let change = table
                .apply(step.snapshot(), step.added(), &[], None, &specs, &[])
                .await
                .unwrap();
            let again = table
                .apply(step.snapshot(), step.added(), &[], None, &specs, &[])
                .await
                .unwrap();

            assert_eq!(written(&change), step.objects(), "snapshot {id}");
            for (object, bytes) in change.objects() {
                assert!(
                    fixture.as_json(object, bytes) == fixture.expected(object),
                    "snapshot {id}: {} differs from its fixture",
                    object.path()
                );
            }

            assert_eq!(change.released(), step.released(), "snapshot {id}");
            assert_eq!(
                Some(change.metadata()),
                Some(again.metadata()),
                "snapshot {id}"
            );
            table = change.into_table();
        }
    }

    #[tokio::test]
    async fn expiry_keeps_the_latest_snapshot() {
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());
        let table = applied(table, [(0, 10), (0, 0)], &[file(0, 0, 10)])
            .await
            .into_table();
        let table = applied(table, [(0, 20), (0, 0)], &[file(0, 10, 20)])
            .await
            .into_table();
        let table = applied(table, [(0, 30), (0, 0)], &[file(0, 20, 30)])
            .await
            .into_table();

        let latest = table.expire(3);
        let expired = table.expire(2).unwrap();
        let next = applied(expired.clone(), [(0, 30), (0, 0)], &[]).await;
        let metadata: Value = serde_json::from_slice(&next.metadata().1).unwrap();

        assert!(
            matches!(latest, Err(Error::LatestSnapshot { id: 3 })),
            "{latest:?}"
        );
        assert!(matches!(
            expired.expire(3),
            Err(Error::LatestSnapshot { id: 3 })
        ));
        let ids = |key: &str, field: &str| {
            metadata[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry[field].clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids("snapshots", "snapshot-id"), [json!(3), json!(4)]);
        assert_eq!(ids("snapshot-log", "snapshot-id"), [json!(3), json!(4)]);
        assert_eq!(
            ids("metadata-log", "metadata-file"),
            [json!("s3://warehouse/orders/metadata/v3-e1.metadata.json")]
        );
        assert_eq!(
            listed(&next),
            ["3-e1-m0.avro", "2-e1-m0.avro", "1-e1-m0.avro"]
        );
    }

    #[tokio::test]
    async fn the_metadata_loads_in_iceberg_rust() {
        let fixture = metadata_fixture();
        let io = FileIO::new_with_memory();
        let specs = specs();
        let mut table = Table::new(FormatVersion::V2, fixture.warehouse(), fixture.topic());
        let mut added = Vec::new();

        for step in fixture.steps() {
            if let Some(through_id) = step.expire_through() {
                table = table.expire(through_id).unwrap();
            }

            let change = table
                .apply(step.snapshot(), step.added(), &[], None, &specs, &[])
                .await
                .unwrap();
            store(&io, &change).await;
            added.extend(step.added().iter().map(|indexed| indexed.file().clone()));

            let location = TableLocation::new(fixture.warehouse(), fixture.topic().name());
            let bytes = io
                .new_input(location.metadata_file(&step.snapshot().key()))
                .unwrap()
                .read()
                .await
                .unwrap();
            let metadata: TableMetadata = serde_json::from_slice(&bytes).unwrap();
            let current = metadata.current_snapshot().unwrap();

            let list = io
                .new_input(current.manifest_list())
                .unwrap()
                .read()
                .await
                .unwrap();
            let list = ManifestList::parse_with_version(&list, metadata.format_version()).unwrap();

            let mut live = Vec::new();
            for manifest in list.entries() {
                let manifest = manifest.load_manifest(&io).await.unwrap();
                live.extend(
                    manifest
                        .entries()
                        .iter()
                        .filter(|entry| entry.is_alive())
                        .map(|entry| entry.file_path().to_owned()),
                );
            }

            live.sort();

            let mut expected: Vec<_> = added
                .iter()
                .filter(|file| step.snapshot().covers(file))
                .map(|file| TableLocation::data_file(fixture.warehouse(), file))
                .collect();
            expected.sort();
            assert_eq!(metadata.uuid(), fixture.topic().uuid());
            assert_eq!(current.snapshot_id(), step.snapshot().id());
            assert_eq!(live, expected, "snapshot {}", step.snapshot().id());
            table = change.into_table();
        }
    }

    #[tokio::test]
    async fn writers_with_different_epochs_never_share_a_path() {
        let specs = specs();
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());
        let added = [file(0, 0, 10)];

        let first = table
            .apply(
                &snapshot(1, 1, [(0, 10), (0, 0)]),
                &added,
                &[],
                None,
                &specs,
                &[],
            )
            .await
            .unwrap();
        let second = table
            .apply(
                &snapshot(1, 2, [(0, 10), (0, 0)]),
                &added,
                &[],
                None,
                &specs,
                &[],
            )
            .await
            .unwrap();

        let paths = |change: &Change| {
            change
                .objects()
                .map(|(object, _)| object.path().to_owned())
                .collect::<Vec<_>>()
        };
        assert!(
            paths(&first)
                .iter()
                .all(|path| !paths(&second).contains(path))
        );
        assert!(
            paths(&first)
                .iter()
                .chain(&paths(&second))
                .all(|path| path.starts_with("s3://warehouse/orders/metadata/"))
        );
    }

    #[tokio::test]
    async fn a_loaded_table_applies_like_the_table_it_was_loaded_from() {
        let fixture = metadata_fixture();
        let io = FileIO::new_with_memory();
        let specs = specs();
        let mut table = Table::new(FormatVersion::V2, fixture.warehouse(), fixture.topic());
        let mut loaded: Option<Table> = None;

        for step in fixture.steps() {
            if let Some(through_id) = step.expire_through() {
                table = table.expire(through_id).unwrap();
                loaded = loaded.map(|loaded| loaded.expire(through_id).unwrap());
            }

            let change = table
                .apply(step.snapshot(), step.added(), &[], None, &specs, &[])
                .await
                .unwrap();
            if let Some(loaded) = &loaded {
                let from_loaded = loaded
                    .apply(step.snapshot(), step.added(), &[], None, &specs, &[])
                    .await
                    .unwrap();
                assert_eq!(written(&from_loaded), written(&change));
                assert_eq!(from_loaded.released(), change.released());
                for ((object, expected), (_, bytes)) in change.objects().zip(from_loaded.objects())
                {
                    assert!(
                        fixture.as_json(object, bytes) == fixture.as_json(object, expected),
                        "{}",
                        object.path()
                    );
                }
            }

            store(&io, &change).await;
            loaded = Some(
                Table::load(&io, fixture.warehouse(), fixture.topic(), step.snapshot())
                    .await
                    .unwrap(),
            );
            table = change.into_table();
        }
    }

    #[tokio::test]
    async fn loading_rejects_metadata_that_is_not_the_snapshot_of_the_topic() {
        let io = FileIO::new_with_memory();
        let specs = specs();
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());
        let first = applied(table, [(0, 10), (0, 0)], &[file(0, 0, 10)]).await;
        store(&io, &first).await;

        let table = first.into_table();
        let latest = snapshot(2, 1, [(0, 20), (0, 0)]);
        let second = table
            .apply(&latest, &[file(0, 10, 20)], &[], None, &specs, &[])
            .await
            .unwrap();
        store(&io, &second).await;

        let earlier = io
            .new_input(table.metadata_log[0].metadata_file.clone())
            .unwrap();
        let ending_early = FileIO::new_with_memory();
        ending_early
            .new_output(table.location().metadata_file(&latest.key()))
            .unwrap()
            .write(earlier.read().await.unwrap())
            .await
            .unwrap();

        let foreign = File::v1(
            Partition::new("users", 0).unwrap(),
            0,
            10,
            TimePartition::new(0, None),
            0,
        )
        .unwrap();
        let commit = Commit::committed(&latest, &specs, &[]);
        let mut writer = table
            .manifest_writer(
                &io,
                &table.location().manifest(&commit.key, 0),
                &commit,
                TimePartition::new(0, None),
            )
            .unwrap();
        writer
            .add_file(
                table
                    .data_file(
                        &commit,
                        &IndexedFile::new(foreign.clone(), Stats::new(10, 100, 0)),
                    )
                    .unwrap(),
                2,
            )
            .unwrap();
        writer.write_manifest_file().await.unwrap();

        let other_topic = Topic::new("orders", Uuid::from_u128(2), 2).unwrap();

        let another_table = Table::load(&io, WAREHOUSE, &other_topic, &latest).await;
        let not_ending_there = Table::load(&ending_early, WAREHOUSE, &topic(), &latest).await;
        let foreign_file = Table::load(&io, WAREHOUSE, &topic(), &latest).await;

        assert!(
            matches!(another_table, Err(Error::Corrupt(_))),
            "{another_table:?}"
        );
        assert!(
            matches!(not_ending_there, Err(Error::Corrupt(_))),
            "{not_ending_there:?}"
        );
        assert!(
            matches!(foreign_file, Err(Error::Corrupt(_))),
            "{foreign_file:?}"
        );
    }

    #[tokio::test]
    async fn added_files_must_lie_inside_the_snapshot() {
        let specs = specs();
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());

        let outside = table
            .apply(
                &snapshot(1, 1, [(0, 10), (0, 0)]),
                &[file(0, 5, 15)],
                &[],
                None,
                &specs,
                &[],
            )
            .await;

        assert!(
            matches!(outside, Err(Error::FileOutsideSnapshot(_))),
            "{outside:?}"
        );
    }

    #[tokio::test]
    async fn a_tail_extends_the_latest_snapshot_without_committing() {
        let io = FileIO::new_with_memory();
        let specs = specs();
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());
        let first = applied(table, [(0, 10), (0, 5)], &[file(0, 0, 10), file(1, 0, 5)]).await;
        store(&io, &first).await;
        let table = first.into_table();
        let committed = snapshot(2, 1, [(0, 20), (0, 5)]);
        let second = table
            .apply(&committed, &[file(0, 10, 20)], &[], None, &specs, &[])
            .await
            .unwrap();
        store(&io, &second).await;
        let table = second.into_table();
        let extended = snapshot(2, 1, [(0, 25), (0, 5)]).offsets().to_vec();

        let tail = table
            .tail(
                &committed,
                extended.clone(),
                &[file(0, 20, 25)],
                &specs,
                &[],
            )
            .await
            .unwrap();
        let again = table
            .tail(
                &committed,
                extended.clone(),
                &[file(0, 20, 25)],
                &specs,
                &[],
            )
            .await
            .unwrap();
        let stale = table
            .tail(
                &snapshot(1, 1, [(0, 10), (0, 5)]),
                extended.clone(),
                &[],
                &specs,
                &[],
            )
            .await;
        let shorter = table
            .tail(
                &committed,
                snapshot(2, 1, [(0, 15), (0, 5)]).offsets().to_vec(),
                &[],
                &specs,
                &[],
            )
            .await;
        let inside = table
            .tail(&committed, extended, &[file(0, 15, 25)], &specs, &[])
            .await;

        let paths: Vec<_> = written(&tail)
            .iter()
            .map(|object| object.path().to_owned())
            .collect();
        assert!(
            paths
                .iter()
                .all(|path| path.contains("/metadata/") && path.contains("2-e1-t")),
            "{paths:?}"
        );
        assert!(tail.released().is_empty());
        assert_eq!(tail.metadata(), again.metadata());
        assert!(
            matches!(
                stale,
                Err(Error::Conflict {
                    expected: 2,
                    given: 1
                })
            ),
            "{stale:?}"
        );
        assert!(matches!(shorter, Err(Error::TailOffsets(_))), "{shorter:?}");
        assert!(
            matches!(inside, Err(Error::FileOutsideSnapshot(_))),
            "{inside:?}"
        );

        store(&io, &tail).await;
        let (metadata_object, _) = tail.metadata();
        let bytes = io
            .new_input(metadata_object.path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let metadata: TableMetadata = serde_json::from_slice(&bytes).unwrap();
        let current = metadata.current_snapshot().unwrap();
        let list = io
            .new_input(current.manifest_list())
            .unwrap()
            .read()
            .await
            .unwrap();
        let list = ManifestList::parse_with_version(&list, metadata.format_version()).unwrap();
        let mut live = Vec::new();
        for manifest in list.entries() {
            let manifest = manifest.load_manifest(&io).await.unwrap();
            live.extend(
                manifest
                    .entries()
                    .iter()
                    .filter(|entry| entry.is_alive())
                    .map(|entry| entry.file_path().rsplit('/').next().unwrap().to_owned()),
            );
        }
        live.sort();

        assert!(current.snapshot_id() >= FIRST_TAIL_SNAPSHOT_ID);
        assert_eq!(current.sequence_number(), 3);
        assert_eq!(current.parent_snapshot_id(), Some(2));
        assert_eq!(
            live,
            [
                "0-10.t0.s0.v1.parquet",
                "0-5.t0.s0.v1.parquet",
                "10-20.t0.s0.v1.parquet",
                "20-25.t0.s0.v1.parquet"
            ]
        );
    }
}
