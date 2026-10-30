mod data;
mod kafka;
mod metadata;
mod payload;
mod postgres;
mod records;
mod registry;

use std::env;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use mink_core::{BuiltFile, File, IndexedFile, OffsetRange, Partition, Stats, TimePartition};

pub use data::{DataFixture, bless_data, data_fixtures};
pub use kafka::{
    create_topic, delete_records, produce_aborted, produce_at, produce_bytes, produce_values,
    produce_with_header, producer, transactional_producer,
};
pub use metadata::{MetadataFixture, MetadataStep, bless_metadata, metadata_fixture};
pub use payload::{
    ExpectedMerge, GlueFixture, GlueRecord, MergeStep, PayloadCase, PayloadFixture, PayloadRecord,
    glue_fixture, payload_fixture,
};
pub use postgres::fresh_schema;
pub use records::{RecordFixture, record_fixtures};
pub use registry::{prefixed, register_schema, schema_guid};

pub const WAREHOUSE: &str = "s3://warehouse";

pub fn fixtures(contract: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/fixtures")
        .join(contract)
}

pub fn service(variable: &str) -> Option<String> {
    match env::var(variable) {
        Ok(value) => Some(value),
        Err(_) if env::var_os("CI").is_some() => panic!("CI must set {variable}"),
        Err(_) => None,
    }
}

pub fn built(files: &[IndexedFile]) -> Vec<BuiltFile> {
    files
        .iter()
        .map(|indexed| BuiltFile::new(indexed.clone(), Bytes::new()))
        .collect()
}

pub fn ranges(parts: &[(i64, i64)]) -> Vec<OffsetRange> {
    parts
        .iter()
        .map(|(start, end)| OffsetRange::new(*start, *end).unwrap())
        .collect()
}

pub fn file(topic: &str, partition: i32, start_offset: i64, end_offset: i64) -> IndexedFile {
    let file = File::v1(
        Partition::new(topic, partition).unwrap(),
        start_offset,
        end_offset,
        TimePartition::new(0, None),
        0,
    )
    .unwrap();
    let records = u64::try_from(end_offset - start_offset).unwrap();
    let mut stats = Stats::new(records, records * 100, 0)
        .with_bounds(
            1,
            partition.to_le_bytes().to_vec(),
            partition.to_le_bytes().to_vec(),
        )
        .with_bounds(
            2,
            start_offset.to_le_bytes().to_vec(),
            (end_offset - 1).to_le_bytes().to_vec(),
        );
    for field in [1, 2, 3, 4, 7, 8, 9] {
        stats = stats.with_nulls(field, 0);
    }

    IndexedFile::new(file, stats)
}
