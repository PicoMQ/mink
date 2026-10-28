use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::ManifestList;
use mink_core::{
    Granularity, IndexedFile, Object, ObjectKind, Offsets, Partition, Snapshot, TimeSpecs, Topic,
};
use uuid::Uuid;

use super::{Change, FormatVersion, ObjectReader, Table};
use crate::Error;

impl ObjectReader for FileIO {
    async fn read(&self, path: &str) -> Result<Bytes, Error> {
        Ok(self.new_input(path)?.read().await?)
    }
}

pub(super) fn specs() -> TimeSpecs {
    TimeSpecs::new(vec![Granularity::None]).unwrap()
}

pub(super) fn topic() -> Topic {
    Topic::new("orders", Uuid::from_u128(1), 2).unwrap()
}

pub(super) fn snapshot(id: i64, epoch: i64, ranges: [(i64, i64); 2]) -> Snapshot {
    let offsets = ranges
        .iter()
        .enumerate()
        .map(|(partition, (start, end))| {
            Offsets::new(
                Partition::new("orders", partition as i32).unwrap(),
                *start,
                *end,
            )
            .unwrap()
        })
        .collect();

    Snapshot::new(id, id * 1000, epoch, offsets).unwrap()
}

pub(super) fn file(partition: i32, start: i64, end: i64) -> IndexedFile {
    mink_conformance::file("orders", partition, start, end)
}

pub(super) async fn applied(
    table: Table,
    ranges: [(i64, i64); 2],
    added: &[IndexedFile],
) -> Change {
    let id = Snapshot::next_id(table.snapshot_id());
    let specs = specs();

    table
        .apply(&snapshot(id, 1, ranges), added, &[], None, &specs, &[])
        .await
        .unwrap()
}

pub(super) fn names(objects: &[Object]) -> Vec<&str> {
    objects
        .iter()
        .map(|object| object.path().rsplit('/').next().unwrap())
        .collect()
}

pub(super) fn written(change: &Change) -> Vec<Object> {
    change.objects().map(|(object, _)| object.clone()).collect()
}

pub(super) fn listed(change: &Change) -> Vec<String> {
    let (_, bytes) = change
        .objects()
        .find(|(object, _)| object.kind() == ObjectKind::ManifestList)
        .unwrap();

    ManifestList::parse_with_version(bytes, FormatVersion::V2.iceberg())
        .unwrap()
        .entries()
        .iter()
        .map(|manifest| {
            manifest
                .manifest_path
                .rsplit('/')
                .next()
                .unwrap()
                .to_owned()
        })
        .collect()
}

pub(super) async fn store(io: &FileIO, change: &Change) {
    for (object, bytes) in change.objects() {
        io.new_output(object.path())
            .unwrap()
            .write(bytes.clone())
            .await
            .unwrap();
    }
}
