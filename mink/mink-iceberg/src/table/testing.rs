use std::collections::HashMap;

use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::ManifestList;
use iceberg::table::Table as ExternalTable;
use mink_core::{
    Granularity, IndexedFile, Object, ObjectKind, Offsets, Partition, Record, Snapshot, TimeSpecs,
    Topic,
};
use uuid::Uuid;

use super::{Change, FormatVersion, ObjectReader, Table};
use crate::testing::{appended, external_table};
use crate::{Error, FileSchema, last_time_field_id, record_batch, write_data_files};

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

    Snapshot::new(id, id, id * 1000, epoch, offsets).unwrap()
}

pub(super) fn file(partition: i32, start: i64, end: i64) -> IndexedFile {
    mink_conformance::file("orders", partition, start, end)
}

pub(super) async fn applied(
    table: Table,
    ranges: [(i64, i64); 2],
    added: &[IndexedFile],
) -> Change {
    let id = Snapshot::next(table.snapshot_id());
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

pub(super) async fn tiered() -> ExternalTable {
    let table = external_table(last_time_field_id() + 1, HashMap::new());
    let records: Vec<Record> = (0..6)
        .map(|offset| {
            let value = Some(Bytes::from(format!("v{offset}")));

            Record::new(offset, offset, None, value, Vec::new()).unwrap()
        })
        .collect();
    let partition = Partition::new("orders", 0).unwrap();
    let rows = record_batch(&partition, &records, &FileSchema::Binary).unwrap();

    let files = write_data_files(&table, vec![rows], "0-0-6").await.unwrap();

    appended(&table, files, Vec::new(), &[(0, 6)]).await
}
