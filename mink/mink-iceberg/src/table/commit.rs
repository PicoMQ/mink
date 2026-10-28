use std::collections::HashMap;

use bytes::Bytes;
use iceberg::spec::{DataFile, Operation, Schema, Summary};
use mink_core::{File, Object, Offsets, Snapshot, TimeSpecs};
use mink_schema::binary_schema;
use sha2::{Digest, Sha256};

use super::manifests::Manifest;

pub const FIRST_TAIL_SNAPSHOT_ID: i64 = 1 << 62;

#[derive(Debug, Clone)]
pub(super) struct Commit<'a> {
    pub(super) snapshot_id: i64,
    pub(super) sequence_number: i64,
    pub(super) timestamp_ms: i64,
    pub(super) key: String,
    pub(super) offsets: Vec<Offsets>,
    pub(super) specs: &'a TimeSpecs,
    pub(super) schemas: &'a [Schema],
}

#[derive(Default)]
pub(super) struct CommitOutput {
    pub(super) objects: Vec<(Object, Bytes)>,
    pub(super) manifests: Vec<Manifest>,
    pub(super) released: Vec<Object>,
    pub(super) additions: Counts,
    pub(super) deletions: Counts,
}

#[derive(Default)]
pub(super) struct Counts {
    files: u64,
    records: u64,
    bytes: u64,
}

impl<'a> Commit<'a> {
    pub(super) fn committed(
        snapshot: &Snapshot,
        specs: &'a TimeSpecs,
        schemas: &'a [Schema],
    ) -> Commit<'a> {
        Commit {
            snapshot_id: snapshot.id(),
            sequence_number: snapshot.id(),
            timestamp_ms: snapshot.timestamp_ms(),
            key: snapshot.key(),
            offsets: snapshot.offsets().to_vec(),
            specs,
            schemas,
        }
    }

    pub(super) fn tail(
        committed: &Snapshot,
        offsets: Vec<Offsets>,
        specs: &'a TimeSpecs,
        schemas: &'a [Schema],
    ) -> Commit<'a> {
        let mut digest = Sha256::new();
        for offsets in &offsets {
            digest.update(format!(
                "{}:{}:{}\n",
                offsets.partition().index(),
                offsets.start_offset(),
                offsets.end_offset()
            ));
        }

        let digest = digest.finalize();
        let key = u64::from_be_bytes(
            digest[..8]
                .try_into()
                .expect("a SHA-256 digest has 8 bytes"),
        );
        let low = key & (FIRST_TAIL_SNAPSHOT_ID.unsigned_abs() - 1);

        Commit {
            snapshot_id: FIRST_TAIL_SNAPSHOT_ID
                | i64::try_from(low).expect("62 bits fit in an i64"),
            sequence_number: committed.id() + 1,
            timestamp_ms: committed.timestamp_ms(),
            key: format!("{}-t{key:016x}", committed.key()),
            offsets,
            specs,
            schemas,
        }
    }

    pub(super) fn covers(&self, file: &File) -> bool {
        self.offsets.iter().any(|offsets| offsets.covers(file))
    }

    pub(super) fn current_schema(&self) -> &Schema {
        self.schemas.last().unwrap_or(binary_schema())
    }
}

impl Counts {
    pub(super) fn add(&mut self, data_file: &DataFile) {
        self.files += 1;
        self.records += data_file.record_count();
        self.bytes += data_file.file_size_in_bytes();
    }
}

pub(super) fn summary(
    offsets: &[Offsets],
    additions: &Counts,
    deletions: &Counts,
    totals: &Counts,
) -> Summary {
    let operation = match (additions.files, deletions.files) {
        (_, 0) => Operation::Append,
        (0, _) => Operation::Delete,
        _ => Operation::Overwrite,
    };
    let mut properties = HashMap::new();
    let mut put = |key: &str, value: u64| {
        properties.insert(key.to_owned(), value.to_string());
    };

    if additions.files > 0 {
        put("added-data-files", additions.files);
        put("added-records", additions.records);
        put("added-files-size", additions.bytes);
    }

    if deletions.files > 0 {
        put("deleted-data-files", deletions.files);
        put("deleted-records", deletions.records);
        put("removed-files-size", deletions.bytes);
    }

    put("total-data-files", totals.files);
    put("total-records", totals.records);
    put("total-files-size", totals.bytes);
    put("total-delete-files", 0);
    put("total-position-deletes", 0);
    put("total-equality-deletes", 0);

    for offsets in offsets {
        let prefix = format!("mink.partition.{}", offsets.partition().index());

        properties.insert(
            format!("{prefix}.start-offset"),
            offsets.start_offset().to_string(),
        );
        properties.insert(
            format!("{prefix}.end-offset"),
            offsets.end_offset().to_string(),
        );
    }

    Summary {
        operation,
        additional_properties: properties,
    }
}
