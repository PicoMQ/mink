use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io;

use bytes::Bytes;
use iceberg::spec::{Datum, FormatVersion, Manifest, ManifestList, ManifestStatus};
use mink_core::{
    IndexedFile, Object, ObjectKind, Offsets, Partition, Snapshot, TableLocation, Topic,
};
use mink_store::{parse_stats, stats_json};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::fixtures;

pub struct MetadataFixture {
    warehouse: String,
    format_version: FormatVersion,
    topic: Topic,
    steps: Vec<MetadataStep>,
}

pub struct MetadataStep {
    expire_through: Option<i64>,
    snapshot: Snapshot,
    added: Vec<IndexedFile>,
    objects: Vec<Object>,
    released: Vec<Object>,
}

impl MetadataFixture {
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    pub fn topic(&self) -> &Topic {
        &self.topic
    }

    pub fn steps(&self) -> &[MetadataStep] {
        &self.steps
    }

    pub fn expected(&self, object: &Object) -> Vec<u8> {
        fs::read(fixtures("metadata").join(fixture_name(object))).unwrap()
    }

    pub fn as_json(&self, object: &Object, bytes: &[u8]) -> Vec<u8> {
        as_json(self.format_version, object, bytes)
    }
}

impl MetadataStep {
    pub fn new(
        expire_through: Option<i64>,
        snapshot: Snapshot,
        added: Vec<IndexedFile>,
        objects: Vec<Object>,
        released: Vec<Object>,
    ) -> MetadataStep {
        MetadataStep {
            expire_through,
            snapshot,
            added,
            objects,
            released,
        }
    }

    pub fn expire_through(&self) -> Option<i64> {
        self.expire_through
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn added(&self) -> &[IndexedFile] {
        &self.added
    }

    pub fn objects(&self) -> &[Object] {
        &self.objects
    }

    pub fn released(&self) -> &[Object] {
        &self.released
    }
}

pub fn metadata_fixture() -> MetadataFixture {
    let cases: Value =
        serde_json::from_str(&fs::read_to_string(fixtures("metadata").join("cases.json")).unwrap())
            .unwrap();
    let warehouse = cases["warehouse"].as_str().unwrap();
    let topic = &cases["topic"];
    let topic = Topic::new(
        topic["name"].as_str().unwrap(),
        Uuid::parse_str(topic["uuid"].as_str().unwrap()).unwrap(),
        topic["partition_count"].as_i64().unwrap() as i32,
    )
    .unwrap();

    let steps = cases["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| MetadataStep {
            expire_through: step["expire_through"].as_i64(),
            snapshot: parse_snapshot(topic.name(), &step["snapshot"]),
            added: step["added"]
                .as_array()
                .unwrap()
                .iter()
                .map(|added| {
                    let path = added["file"].as_str().unwrap();
                    IndexedFile::new(
                        TableLocation::parse_data_file(warehouse, path).unwrap(),
                        parse_stats(&added["stats"]).unwrap(),
                    )
                })
                .collect(),
            objects: objects(&step["objects"]),
            released: objects(&step["released"]),
        })
        .collect();

    MetadataFixture {
        warehouse: warehouse.to_owned(),
        format_version: serde_json::from_value(cases["format_version"].clone()).unwrap(),
        topic,
        steps,
    }
}

pub fn bless_metadata(
    warehouse: &str,
    topic: &Topic,
    format_version: FormatVersion,
    steps: &[MetadataStep],
    written: &[(Object, Bytes)],
) -> io::Result<()> {
    let directory = fixtures("metadata");
    if directory.exists() {
        fs::remove_dir_all(&directory)?;
    }

    fs::create_dir_all(&directory)?;

    for (object, bytes) in written {
        fs::write(
            directory.join(fixture_name(object)),
            as_json(format_version, object, bytes),
        )?;
    }

    let steps: Vec<_> = steps
        .iter()
        .map(|step| {
            json!({
                "expire_through": step.expire_through,
                "snapshot": snapshot(&step.snapshot),
                "added": step.added.iter().map(|indexed| json!({
                    "file": TableLocation::data_file(warehouse, indexed.file()),
                    "stats": stats_json(indexed.stats()),
                })).collect::<Vec<_>>(),
                "objects": objects_json(&step.objects),
                "released": objects_json(&step.released),
            })
        })
        .collect();
    let cases = json!({
        "warehouse": warehouse,
        "format_version": format_version,
        "topic": {
            "name": topic.name(),
            "uuid": topic.uuid(),
            "partition_count": topic.partition_count(),
        },
        "steps": steps,
    });

    fs::write(
        directory.join("cases.json"),
        serde_json::to_string_pretty(&cases)? + "\n",
    )
}

fn snapshot(snapshot: &Snapshot) -> Value {
    let offsets: Vec<_> = snapshot
        .offsets()
        .iter()
        .map(|offsets| {
            json!({
                "partition": offsets.partition().index(),
                "start_offset": offsets.start_offset(),
                "end_offset": offsets.end_offset(),
            })
        })
        .collect();

    json!({
        "id": snapshot.id(),
        "timestamp_ms": snapshot.timestamp_ms(),
        "epoch": snapshot.epoch(),
        "offsets": offsets,
    })
}

fn parse_snapshot(topic: &str, value: &Value) -> Snapshot {
    let offsets = value["offsets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|offsets| {
            Offsets::new(
                Partition::new(topic, offsets["partition"].as_i64().unwrap() as i32).unwrap(),
                offsets["start_offset"].as_i64().unwrap(),
                offsets["end_offset"].as_i64().unwrap(),
            )
            .unwrap()
        })
        .collect();

    Snapshot::new(
        value["id"].as_i64().unwrap(),
        value["timestamp_ms"].as_i64().unwrap(),
        value["epoch"].as_i64().unwrap(),
        offsets,
    )
    .unwrap()
}

fn objects_json(objects: &[Object]) -> Vec<Value> {
    objects
        .iter()
        .map(|object| json!({ "path": object.path(), "kind": object.kind().name() }))
        .collect()
}

fn objects(value: &Value) -> Vec<Object> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|object| {
            let kind = ObjectKind::from_name(object["kind"].as_str().unwrap()).unwrap();
            Object::new(object["path"].as_str().unwrap(), kind)
        })
        .collect()
}

fn as_json(format_version: FormatVersion, object: &Object, bytes: &[u8]) -> Vec<u8> {
    let contents = match object.kind() {
        ObjectKind::TableMetadata | ObjectKind::Statistics => return bytes.to_vec(),
        ObjectKind::ManifestList => manifest_list(format_version, bytes),
        ObjectKind::Manifest => manifest(bytes),
    };

    (serde_json::to_string_pretty(&contents).unwrap() + "\n").into_bytes()
}

fn fixture_name(object: &Object) -> String {
    let name = object.path().rsplit('/').next().unwrap();

    match object.kind() {
        ObjectKind::TableMetadata | ObjectKind::Statistics => name.to_owned(),
        ObjectKind::ManifestList | ObjectKind::Manifest => {
            format!("{}.json", name.strip_suffix(".avro").unwrap())
        }
    }
}

fn manifest_list(format_version: FormatVersion, bytes: &[u8]) -> Value {
    let list = ManifestList::parse_with_version(bytes, format_version).unwrap();

    let manifests: Vec<_> = list
        .entries()
        .iter()
        .map(|manifest| {
            let partitions: Vec<_> = manifest
                .partitions
                .iter()
                .flatten()
                .map(|summary| {
                    json!({
                        "contains_null": summary.contains_null,
                        "contains_nan": summary.contains_nan,
                        "lower_bound": summary.lower_bound.as_ref().map(hex::encode),
                        "upper_bound": summary.upper_bound.as_ref().map(hex::encode),
                    })
                })
                .collect();

            json!({
                "manifest_path": manifest.manifest_path,
                "manifest_length": manifest.manifest_length,
                "partition_spec_id": manifest.partition_spec_id,
                "content": manifest.content.to_string(),
                "sequence_number": manifest.sequence_number,
                "min_sequence_number": manifest.min_sequence_number,
                "added_snapshot_id": manifest.added_snapshot_id,
                "added_files_count": manifest.added_files_count,
                "existing_files_count": manifest.existing_files_count,
                "deleted_files_count": manifest.deleted_files_count,
                "added_rows_count": manifest.added_rows_count,
                "existing_rows_count": manifest.existing_rows_count,
                "deleted_rows_count": manifest.deleted_rows_count,
                "partitions": partitions,
            })
        })
        .collect();

    json!({ "manifests": manifests })
}

fn manifest(bytes: &[u8]) -> Value {
    let manifest = Manifest::parse_avro(bytes).unwrap();
    let metadata = manifest.metadata();
    let partition_type = metadata
        .partition_spec()
        .partition_type(metadata.schema())
        .unwrap();
    let bounds = |values: &HashMap<i32, Datum>| -> BTreeMap<i32, String> {
        values
            .iter()
            .map(|(field, datum)| (*field, hex::encode(datum.to_bytes().unwrap())))
            .collect()
    };
    let counts = |values: &HashMap<i32, u64>| -> BTreeMap<i32, u64> {
        values
            .iter()
            .map(|(field, count)| (*field, *count))
            .collect()
    };

    let entries: Vec<_> = manifest
        .entries()
        .iter()
        .map(|entry| {
            let file = entry.data_file();
            let partition: Map<String, Value> = partition_type
                .fields()
                .iter()
                .zip(file.partition().iter())
                .map(|(field, literal)| {
                    let value = literal
                        .map(|literal| literal.clone().try_into_json(&field.field_type).unwrap());
                    (field.name.clone(), value.unwrap_or(Value::Null))
                })
                .collect();

            json!({
                "status": status(entry.status()),
                "snapshot_id": entry.snapshot_id(),
                "sequence_number": entry.sequence_number(),
                "file_sequence_number": entry.file_sequence_number,
                "file_path": file.file_path(),
                "file_format": file.file_format().to_string(),
                "partition": partition,
                "record_count": file.record_count(),
                "file_size_in_bytes": file.file_size_in_bytes(),
                "lower_bounds": bounds(file.lower_bounds()),
                "upper_bounds": bounds(file.upper_bounds()),
                "null_value_counts": counts(file.null_value_counts()),
                "nan_value_counts": counts(file.nan_value_counts()),
            })
        })
        .collect();

    json!({
        "schema_id": metadata.schema_id(),
        "partition_spec_id": metadata.partition_spec().spec_id(),
        "format_version": *metadata.format_version() as u8,
        "content": metadata.content().to_string(),
        "entries": entries,
    })
}

fn status(status: ManifestStatus) -> &'static str {
    match status {
        ManifestStatus::Added => "added",
        ManifestStatus::Existing => "existing",
        ManifestStatus::Deleted => "deleted",
    }
}
