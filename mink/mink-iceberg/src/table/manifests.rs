use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestEntry,
    ManifestFile, ManifestList, ManifestListWriter, ManifestStatus, ManifestWriter,
    ManifestWriterBuilder, PrimitiveType, Struct,
};
use mink_core::{File, Granularity, IndexedFile, Object, ObjectKind, TableLocation, TimePartition};

use super::commit::{Commit, CommitOutput};
use super::specs::partition_specs;
use super::{FormatVersion, Table};
use crate::Error;

const MANIFESTS_PER_BUCKET: usize = 16;

#[derive(Debug, Clone)]
pub(super) struct Manifest {
    pub(super) file: ManifestFile,
    pub(super) live: Vec<(File, ManifestEntry)>,
}

enum Entry<'a> {
    Added(&'a IndexedFile),
    Existing(File, ManifestEntry),
    Deleted(ManifestEntry),
}

impl Manifest {
    pub(super) fn key(&self) -> Option<TimePartition> {
        self.live.first().map(|(file, _)| file.time())
    }

    pub(super) fn object(&self) -> Object {
        Object::new(&self.file.manifest_path, ObjectKind::Manifest)
    }
}

impl Table {
    pub(super) async fn write_manifests(
        &self,
        io: &FileIO,
        commit: &Commit<'_>,
        added: &[IndexedFile],
        replaced: &[File],
        output: &mut CommitOutput,
    ) -> Result<Vec<Manifest>, Error> {
        let dropped = |file: &File| !commit.covers(file) || replaced.contains(file);
        let kept_or_deleted = |file: &'_ File, entry: &'_ ManifestEntry| {
            if dropped(file) {
                Entry::Deleted(entry.clone())
            } else {
                Entry::Existing(file.clone(), entry.clone())
            }
        };

        let mut added_by_key: BTreeMap<TimePartition, Vec<&IndexedFile>> = BTreeMap::new();
        for indexed in added {
            added_by_key
                .entry(indexed.file().time())
                .or_default()
                .push(indexed);
        }

        let mut existing_by_key: BTreeMap<TimePartition, Vec<&Manifest>> = BTreeMap::new();
        for manifest in &self.manifests {
            match manifest.key() {
                Some(key) => existing_by_key.entry(key).or_default().push(manifest),
                None => output.released.push(manifest.object()),
            }
        }

        let merged: BTreeSet<TimePartition> = existing_by_key
            .iter()
            .filter(|(key, manifests)| {
                let assembled = replaced.iter().any(|file| file.time() == **key);
                let after = manifests.len() + usize::from(added_by_key.contains_key(key));

                assembled || after > MANIFESTS_PER_BUCKET
            })
            .map(|(key, _)| *key)
            .collect();

        for (key, files) in added_by_key.iter().filter(|(key, _)| !merged.contains(key)) {
            let entries = files.iter().map(|indexed| Entry::Added(indexed)).collect();

            self.manifest(io, commit, *key, entries, output).await?;
        }

        for key in &merged {
            let mut entries = Vec::new();

            for manifest in &existing_by_key[key] {
                output.released.push(manifest.object());
                entries.extend(
                    manifest
                        .live
                        .iter()
                        .map(|(file, entry)| kept_or_deleted(file, entry)),
                );
            }
            entries.extend(
                added_by_key
                    .get(key)
                    .into_iter()
                    .flatten()
                    .map(|indexed| Entry::Added(indexed)),
            );

            self.manifest(io, commit, *key, entries, output).await?;
        }

        let changed: Vec<(TimePartition, &Manifest)> = existing_by_key
            .iter()
            .filter(|(key, _)| !merged.contains(key))
            .flat_map(|(key, manifests)| manifests.iter().map(move |manifest| (*key, *manifest)))
            .filter(|(_, manifest)| manifest.live.iter().any(|(file, _)| dropped(file)))
            .collect();
        let reused: Vec<Manifest> = self
            .manifests
            .iter()
            .filter(|manifest| manifest.key().is_some_and(|key| !merged.contains(&key)))
            .filter(|manifest| manifest.live.iter().all(|(file, _)| !dropped(file)))
            .cloned()
            .collect();

        for (key, manifest) in changed {
            output.released.push(manifest.object());

            let entries = manifest
                .live
                .iter()
                .map(|(file, entry)| kept_or_deleted(file, entry))
                .collect();
            self.manifest(io, commit, key, entries, output).await?;
        }

        Ok(reused)
    }

    async fn manifest(
        &self,
        io: &FileIO,
        commit: &Commit<'_>,
        key: TimePartition,
        entries: Vec<Entry<'_>>,
        output: &mut CommitOutput,
    ) -> Result<(), Error> {
        let location = self
            .location()
            .manifest(&commit.key, output.manifests.len());
        let mut writer = self.manifest_writer(io, &location, commit, key)?;

        let mut live = Vec::new();
        for entry in entries {
            match entry {
                Entry::Added(indexed) => {
                    let data_file = self.data_file(commit, indexed)?;

                    output.additions.add(&data_file);
                    writer.add_file(data_file.clone(), commit.sequence_number)?;
                    live.push((indexed.file().clone(), added_entry(commit, data_file)));
                }
                Entry::Existing(file, entry) => {
                    let (snapshot_id, sequence_number) = loaded(&entry);

                    writer.add_existing_file(
                        entry.data_file().clone(),
                        snapshot_id,
                        sequence_number,
                        entry.file_sequence_number,
                    )?;
                    live.push((file, entry));
                }
                Entry::Deleted(entry) => {
                    let (_, sequence_number) = loaded(&entry);

                    output.deletions.add(entry.data_file());
                    writer.add_delete_file(
                        entry.data_file().clone(),
                        sequence_number,
                        entry.file_sequence_number,
                    )?;
                }
            }
        }

        let file = writer.write_manifest_file().await?;
        let bytes = io.new_input(&location)?.read().await?;

        output
            .objects
            .push((Object::new(&location, ObjectKind::Manifest), bytes));
        output.manifests.push(Manifest { file, live });
        Ok(())
    }

    pub(super) async fn write_list(
        &self,
        io: &FileIO,
        commit: &Commit<'_>,
        manifests: &mut [Manifest],
    ) -> Result<(Object, Bytes), Error> {
        let location = &self.location().manifest_list(&commit.key);
        let parent = self.snapshot_id();
        let output = io.new_output(location)?.writer().await?;
        let mut list = match self.version {
            FormatVersion::V2 => {
                ManifestListWriter::v2(output, commit.snapshot_id, parent, commit.sequence_number)
            }
        };

        list.add_manifests(manifests.iter().map(|manifest| manifest.file.clone()))?;
        list.close().await?;

        let bytes = io.new_input(location)?.read().await?;
        let listed = ManifestList::parse_with_version(&bytes, self.version.iceberg())?;
        for (manifest, file) in manifests.iter_mut().zip(listed.consume_entries()) {
            manifest.file = file;
        }

        Ok((Object::new(location, ObjectKind::ManifestList), bytes))
    }

    pub(super) fn manifest_writer(
        &self,
        io: &FileIO,
        location: &str,
        commit: &Commit<'_>,
        key: TimePartition,
    ) -> Result<ManifestWriter, Error> {
        let spec = partition_specs(commit.specs)?
            .into_iter()
            .find(|spec| spec.spec_id() == key.spec_id())
            .ok_or(Error::UnknownTimeSpec(key.spec_id()))?;
        let builder = ManifestWriterBuilder::new(
            io.new_output(location)?,
            Some(commit.snapshot_id),
            Arc::new(commit.current_schema().clone()),
            spec,
        );

        Ok(match self.version {
            FormatVersion::V2 => builder.build_v2_data(),
        })
    }

    pub(super) fn data_file(
        &self,
        commit: &Commit<'_>,
        indexed: &IndexedFile,
    ) -> Result<DataFile, Error> {
        let (file, stats) = (indexed.file(), indexed.stats());
        let datums = |values: &BTreeMap<i32, Vec<u8>>| -> Result<HashMap<i32, Datum>, Error> {
            values
                .iter()
                .map(|(field, bytes)| {
                    Ok((
                        *field,
                        Datum::try_from_bytes(bytes, primitive(commit, *field)?)?,
                    ))
                })
                .collect()
        };
        let counts = |values: &BTreeMap<i32, u64>| {
            values
                .iter()
                .map(|(field, count)| (*field, *count))
                .collect()
        };

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(TableLocation::data_file(&self.warehouse, file))
            .file_format(DataFileFormat::Parquet)
            .partition(partition_value(commit, file)?)
            .partition_spec_id(file.time().spec_id())
            .record_count(stats.records())
            .file_size_in_bytes(stats.bytes())
            .lower_bounds(datums(stats.lower())?)
            .upper_bounds(datums(stats.upper())?)
            .null_value_counts(counts(stats.nulls()))
            .nan_value_counts(counts(stats.nans()))
            .build()
            .expect("every required data file field is set");

        Ok(data_file)
    }

    pub(super) fn file_of(&self, data_file: &DataFile) -> Result<File, Error> {
        TableLocation::parse_data_file(&self.warehouse, data_file.file_path())
            .filter(|file| file.partition().topic() == self.topic)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "{} is not a file of topic {}",
                    data_file.file_path(),
                    self.topic
                ))
            })
    }
}

fn partition_value(commit: &Commit<'_>, file: &File) -> Result<Struct, Error> {
    let spec_id = file.time().spec_id();
    let granularity = commit
        .specs
        .get(spec_id)
        .ok_or(Error::UnknownTimeSpec(spec_id))?
        .granularity();
    let partition = Some(Literal::int(file.partition().index()));

    let value = match (granularity, file.time().bucket()) {
        (Granularity::None, None) => Struct::from_iter([partition]),
        (Granularity::Day, Some(bucket)) => {
            Struct::from_iter([partition, Some(Literal::date(bucket))])
        }
        (Granularity::Hour | Granularity::Month, Some(bucket)) => {
            Struct::from_iter([partition, Some(Literal::int(bucket))])
        }
        _ => {
            return Err(Error::Corrupt(format!(
                "{file:?} does not fit spec {spec_id}"
            )));
        }
    };

    Ok(value)
}

fn primitive(commit: &Commit<'_>, field: i32) -> Result<PrimitiveType, Error> {
    commit
        .current_schema()
        .field_by_id(field)
        .and_then(|field| field.field_type.as_primitive_type().cloned())
        .ok_or(Error::StatsField(field))
}

fn added_entry(commit: &Commit<'_>, data_file: DataFile) -> ManifestEntry {
    ManifestEntry::builder()
        .status(ManifestStatus::Added)
        .snapshot_id(commit.snapshot_id)
        .sequence_number(commit.sequence_number)
        .file_sequence_number(commit.sequence_number)
        .data_file(data_file)
        .build()
}

fn loaded(entry: &ManifestEntry) -> (i64, i64) {
    let snapshot_id = entry
        .snapshot_id()
        .expect("loaded entries carry their snapshot");
    let sequence_number = entry
        .sequence_number()
        .expect("loaded entries carry their sequence number");

    (snapshot_id, sequence_number)
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{Manifest as IcebergManifest, ManifestStatus};
    use mink_conformance::WAREHOUSE;
    use mink_core::{Granularity, Partition, Stats, TimeSpecs};

    use super::super::Change;
    use super::super::testing::{applied, file, listed, names, snapshot, topic, written};
    use super::*;

    #[tokio::test]
    async fn dropping_files_rewrites_only_the_manifests_that_hold_them() {
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());
        let table = applied(table, [(0, 10), (0, 0)], &[file(0, 0, 10)])
            .await
            .into_table();
        let table = applied(table, [(0, 10), (0, 10)], &[file(1, 0, 10)])
            .await
            .into_table();
        let table = applied(table, [(0, 20), (0, 10)], &[file(0, 10, 20)])
            .await
            .into_table();

        let dropping = applied(table, [(10, 25), (0, 10)], &[file(0, 20, 25)]).await;

        assert_eq!(
            names(&written(&dropping)),
            [
                "4-e1-m0.avro",
                "4-e1-m1.avro",
                "snap-4-e1.avro",
                "v4-e1.metadata.json"
            ]
        );
        assert_eq!(
            names(dropping.released()),
            ["v3-e1.metadata.json", "snap-3-e1.avro", "1-e1-m0.avro"]
        );
        assert_eq!(
            listed(&dropping),
            [
                "4-e1-m0.avro",
                "4-e1-m1.avro",
                "3-e1-m0.avro",
                "2-e1-m0.avro"
            ]
        );
        let (_, rewrite) = dropping.objects().nth(1).unwrap();
        let entries = IcebergManifest::parse_avro(rewrite).unwrap();
        let statuses: Vec<_> = entries
            .entries()
            .iter()
            .map(|entry| {
                (
                    entry.file_path().rsplit('/').next().unwrap().to_owned(),
                    entry.status(),
                )
            })
            .collect();
        assert_eq!(
            statuses,
            [("0-10.t0.s0.v1.parquet".to_owned(), ManifestStatus::Deleted)]
        );
    }

    #[tokio::test]
    async fn manifests_with_no_live_files_are_not_listed_again() {
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());
        let table = applied(table, [(0, 10), (0, 0)], &[file(0, 0, 10)])
            .await
            .into_table();
        let table = applied(table, [(0, 20), (0, 0)], &[file(0, 10, 20)])
            .await
            .into_table();
        let dropping = applied(table, [(10, 20), (0, 0)], &[]).await;
        let dropping_list = listed(&dropping);
        let after = applied(dropping.into_table(), [(10, 20), (0, 0)], &[]).await;

        assert_eq!(dropping_list, ["3-e1-m0.avro", "2-e1-m0.avro"]);
        assert_eq!(listed(&after), ["2-e1-m0.avro"]);
        assert_eq!(
            names(after.released()),
            ["v3-e1.metadata.json", "snap-3-e1.avro", "3-e1-m0.avro"]
        );
    }

    fn hourly(start: i64, end: i64, bucket: i32) -> IndexedFile {
        let time = TimePartition::new(0, Some(bucket));
        let file = File::v1(Partition::new("orders", 0).unwrap(), start, end, time, 0).unwrap();
        let records = u64::try_from(end - start).unwrap();

        IndexedFile::new(file, Stats::new(records, records * 100, 0))
    }

    fn manifest_buckets(change: &Change) -> Vec<(String, Option<Vec<u8>>)> {
        let (_, bytes) = change
            .objects()
            .find(|(object, _)| object.kind() == ObjectKind::ManifestList)
            .unwrap();

        ManifestList::parse_with_version(bytes, FormatVersion::V2.iceberg())
            .unwrap()
            .consume_entries()
            .into_iter()
            .map(|manifest| {
                let name = manifest
                    .manifest_path
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .to_owned();
                let summary = manifest.partitions.unwrap_or_default();

                (
                    name,
                    summary
                        .get(1)
                        .and_then(|field| field.lower_bound.as_ref().map(|bound| bound.to_vec())),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn manifests_hold_one_bucket_each() {
        let specs = TimeSpecs::new(vec![Granularity::Hour]).unwrap();
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());
        let bucket = |hour: i32| Some(hour.to_le_bytes().to_vec());

        let first = table
            .apply(
                &snapshot(1, 1, [(0, 20), (0, 0)]),
                &[hourly(0, 10, 5), hourly(10, 20, 6)],
                &[],
                None,
                &specs,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            manifest_buckets(&first),
            [
                ("1-e1-m0.avro".to_owned(), bucket(5)),
                ("1-e1-m1.avro".to_owned(), bucket(6))
            ]
        );

        let mut table = first.into_table();
        for id in 2..=16 {
            let start = 10 + 10 * (id - 1);
            let change = table
                .apply(
                    &snapshot(id, 1, [(0, start + 10), (0, 0)]),
                    &[hourly(start, start + 10, 6)],
                    &[],
                    None,
                    &specs,
                    &[],
                )
                .await
                .unwrap();
            table = change.into_table();
        }
        let merged = table
            .apply(
                &snapshot(17, 1, [(0, 180), (0, 0)]),
                &[hourly(170, 180, 6)],
                &[],
                None,
                &specs,
                &[],
            )
            .await
            .unwrap();
        let buckets = manifest_buckets(&merged);
        assert_eq!(
            buckets
                .iter()
                .filter(|(_, lower)| *lower == bucket(6))
                .count(),
            1
        );
        assert_eq!(
            buckets
                .iter()
                .filter(|(_, lower)| *lower == bucket(5))
                .count(),
            1
        );

        let parts = [hourly(0, 10, 5).file().clone()];
        let assembled = IndexedFile::new(
            File::v1(
                Partition::new("orders", 0).unwrap(),
                0,
                10,
                TimePartition::new(0, Some(5)),
                1,
            )
            .unwrap(),
            Stats::new(10, 900, 0),
        );
        let reassembled = merged
            .into_table()
            .apply(
                &snapshot(18, 1, [(0, 180), (0, 0)]),
                &[assembled],
                &parts,
                None,
                &specs,
                &[],
            )
            .await
            .unwrap();
        let (_, manifest) = reassembled
            .objects()
            .find(|(object, _)| object.path().ends_with("18-e1-m0.avro"))
            .unwrap();
        let statuses: Vec<(String, ManifestStatus)> = IcebergManifest::parse_avro(manifest)
            .unwrap()
            .entries()
            .iter()
            .map(|entry| {
                (
                    entry
                        .data_file()
                        .file_path()
                        .rsplit('/')
                        .next()
                        .unwrap()
                        .to_owned(),
                    entry.status(),
                )
            })
            .collect();
        assert_eq!(
            statuses,
            [
                (
                    "0-10.t0_5.s0.v1.parquet".to_owned(),
                    ManifestStatus::Deleted
                ),
                ("0-10.t0_5.s1.v1.parquet".to_owned(), ManifestStatus::Added),
            ]
        );
    }
}
