use std::collections::BTreeMap;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::Error;
use crate::granularity::TimePartition;
use crate::offsets::OffsetRange;
use crate::topic::Partition;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct File {
    partition: Partition,
    range: OffsetRange,
    time: TimePartition,
    schema_id: i32,
    assembly: Option<AssemblyHash>,
    version: FileVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AssemblyHash {
    value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedFile {
    file: File,
    stats: Stats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltFile {
    indexed: IndexedFile,
    footer: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assembly {
    built: BuiltFile,
    parts: Vec<OffsetRange>,
    replaced: Vec<File>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileVersion {
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    records: u64,
    bytes: u64,
    first_timestamp_ms: i64,
    lower: BTreeMap<i32, Vec<u8>>,
    upper: BTreeMap<i32, Vec<u8>>,
    nulls: BTreeMap<i32, u64>,
    nans: BTreeMap<i32, u64>,
}

impl FileVersion {
    pub fn number(self) -> u16 {
        match self {
            FileVersion::V1 => 1,
        }
    }

    pub fn from_number(number: u16) -> Option<FileVersion> {
        match number {
            1 => Some(FileVersion::V1),
            _ => None,
        }
    }
}

impl AssemblyHash {
    pub fn new(value: u64) -> AssemblyHash {
        AssemblyHash { value }
    }

    pub fn of(parts: &[OffsetRange]) -> AssemblyHash {
        let lines: String = parts
            .iter()
            .map(|part| format!("{}:{}\n", part.start(), part.end()))
            .collect();
        let digest = Sha256::digest(lines.as_bytes());

        AssemblyHash::new(u64::from_be_bytes(
            digest[..8].try_into().expect("a digest has 8 bytes"),
        ))
    }

    pub fn value(&self) -> u64 {
        self.value
    }
}

impl File {
    pub fn new(
        partition: Partition,
        start_offset: i64,
        end_offset: i64,
        time: TimePartition,
        schema_id: i32,
        version: FileVersion,
    ) -> Result<File, Error> {
        let range = OffsetRange::new(start_offset, end_offset)?;

        File::identified(partition, range, time, schema_id, None, version)
    }

    pub fn v1(
        partition: Partition,
        start_offset: i64,
        end_offset: i64,
        time: TimePartition,
        schema_id: i32,
    ) -> Result<File, Error> {
        File::new(
            partition,
            start_offset,
            end_offset,
            time,
            schema_id,
            FileVersion::V1,
        )
    }

    pub fn assembled(
        partition: Partition,
        time: TimePartition,
        schema_id: i32,
        version: FileVersion,
        parts: &[OffsetRange],
    ) -> Result<File, Error> {
        let [first, .., last] = parts else {
            return Err(Error::Parts);
        };
        let ordered = parts
            .windows(2)
            .all(|pair| pair[0].end() <= pair[1].start());
        let empty = parts.iter().any(OffsetRange::is_empty);

        if !ordered || empty {
            return Err(Error::Parts);
        }

        let range = OffsetRange::new(first.start(), last.end())?;

        File::identified(
            partition,
            range,
            time,
            schema_id,
            Some(AssemblyHash::of(parts)),
            version,
        )
    }

    pub fn identified(
        partition: Partition,
        range: OffsetRange,
        time: TimePartition,
        schema_id: i32,
        assembly: Option<AssemblyHash>,
        version: FileVersion,
    ) -> Result<File, Error> {
        if range.is_empty() {
            return Err(Error::EmptyFile(range.start()));
        }

        if schema_id < 0 {
            return Err(Error::SchemaId(schema_id));
        }

        if time.spec_id() < 0 {
            return Err(Error::SpecId(time.spec_id()));
        }

        Ok(File {
            partition,
            range,
            time,
            schema_id,
            assembly,
            version,
        })
    }

    pub fn partition(&self) -> &Partition {
        &self.partition
    }

    pub fn range(&self) -> OffsetRange {
        self.range
    }

    pub fn start_offset(&self) -> i64 {
        self.range.start()
    }

    pub fn end_offset(&self) -> i64 {
        self.range.end()
    }

    pub fn time(&self) -> TimePartition {
        self.time
    }

    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    pub fn assembly(&self) -> Option<AssemblyHash> {
        self.assembly
    }

    pub fn version(&self) -> FileVersion {
        self.version
    }

    pub fn assembles(&self, parts: &[OffsetRange]) -> bool {
        self.assembly == Some(AssemblyHash::of(parts))
    }
}

impl BuiltFile {
    pub fn new(indexed: IndexedFile, footer: Bytes) -> BuiltFile {
        BuiltFile { indexed, footer }
    }

    pub fn indexed(&self) -> &IndexedFile {
        &self.indexed
    }

    pub fn footer(&self) -> &Bytes {
        &self.footer
    }
}

impl Assembly {
    pub fn new(
        built: BuiltFile,
        parts: Vec<OffsetRange>,
        replaced: Vec<File>,
    ) -> Result<Assembly, Error> {
        let assembled = built.indexed().file();
        let matches = |file: &File| {
            file.partition() == assembled.partition()
                && file.time() == assembled.time()
                && file.schema_id() == assembled.schema_id()
                && file.version() == assembled.version()
        };

        if !assembled.assembles(&parts) {
            return Err(Error::Parts);
        }

        if replaced.len() < 2 || !replaced.iter().all(matches) {
            return Err(Error::AssemblyFiles(assembled.clone()));
        }

        Ok(Assembly {
            built,
            parts,
            replaced,
        })
    }

    pub fn built(&self) -> &BuiltFile {
        &self.built
    }

    pub fn parts(&self) -> &[OffsetRange] {
        &self.parts
    }

    pub fn replaced(&self) -> &[File] {
        &self.replaced
    }
}

impl IndexedFile {
    pub fn new(file: File, stats: Stats) -> IndexedFile {
        IndexedFile { file, stats }
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }
}

impl Stats {
    pub fn new(records: u64, bytes: u64, first_timestamp_ms: i64) -> Stats {
        Stats {
            records,
            bytes,
            first_timestamp_ms,
            lower: BTreeMap::new(),
            upper: BTreeMap::new(),
            nulls: BTreeMap::new(),
            nans: BTreeMap::new(),
        }
    }

    pub fn with_bounds(mut self, field: i32, lower: Vec<u8>, upper: Vec<u8>) -> Self {
        self.set_bounds(field, lower, upper);
        self
    }

    pub fn with_nulls(mut self, field: i32, count: u64) -> Self {
        self.set_nulls(field, count);
        self
    }

    pub fn with_nans(mut self, field: i32, count: u64) -> Self {
        self.set_nans(field, count);
        self
    }

    pub fn set_bounds(&mut self, field: i32, lower: Vec<u8>, upper: Vec<u8>) {
        self.lower.insert(field, lower);
        self.upper.insert(field, upper);
    }

    pub fn set_nulls(&mut self, field: i32, count: u64) {
        self.nulls.insert(field, count);
    }

    pub fn set_nans(&mut self, field: i32, count: u64) {
        self.nans.insert(field, count);
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn first_timestamp_ms(&self) -> i64 {
        self.first_timestamp_ms
    }

    pub fn lower(&self) -> &BTreeMap<i32, Vec<u8>> {
        &self.lower
    }

    pub fn upper(&self) -> &BTreeMap<i32, Vec<u8>> {
        &self.upper
    }

    pub fn nulls(&self) -> &BTreeMap<i32, u64> {
        &self.nulls
    }

    pub fn nans(&self) -> &BTreeMap<i32, u64> {
        &self.nans
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hourly() -> TimePartition {
        TimePartition::new(0, Some(498_108))
    }

    fn ranges(parts: &[(i64, i64)]) -> Vec<OffsetRange> {
        parts
            .iter()
            .map(|(start, end)| OffsetRange::new(*start, *end).unwrap())
            .collect()
    }

    #[test]
    fn files_are_rejected_with_an_empty_range_or_a_negative_id() {
        let partition = Partition::new("orders", 0).unwrap();
        let negative_spec = TimePartition::new(-1, None);

        assert_eq!(
            File::v1(partition.clone(), 5, 5, hourly(), 0),
            Err(Error::EmptyFile(5))
        );
        assert_eq!(
            File::v1(partition.clone(), -1, 5, hourly(), 0),
            Err(Error::OffsetRange { start: -1, end: 5 })
        );
        assert_eq!(
            File::v1(partition.clone(), 0, 5, hourly(), -1),
            Err(Error::SchemaId(-1))
        );
        assert_eq!(
            File::v1(partition, 0, 5, negative_spec, 0),
            Err(Error::SpecId(-1))
        );
    }

    #[test]
    fn assembled_files_need_ordered_parts_and_name_them() {
        let partition = Partition::new("orders", 0).unwrap();
        let assembled = |parts: &[(i64, i64)]| {
            File::assembled(
                partition.clone(),
                hourly(),
                0,
                FileVersion::V1,
                &ranges(parts),
            )
        };

        let file = assembled(&[(0, 10), (20, 30)]).unwrap();

        assert_eq!((file.start_offset(), file.end_offset()), (0, 30));
        assert!(file.assembles(&ranges(&[(0, 10), (20, 30)])));
        assert!(!file.assembles(&ranges(&[(0, 10), (20, 29)])));
        assert_eq!(assembled(&[(0, 10)]), Err(Error::Parts));
        assert_eq!(assembled(&[(0, 10), (5, 30)]), Err(Error::Parts));
        assert_eq!(assembled(&[(0, 10), (20, 20)]), Err(Error::Parts));
    }

    #[test]
    fn an_assembly_replaces_files_of_its_own_partition_time_schema_and_version() {
        let partition = Partition::new("orders", 0).unwrap();
        let parts = ranges(&[(0, 10), (10, 20)]);
        let assembled =
            File::assembled(partition.clone(), hourly(), 0, FileVersion::V1, &parts).unwrap();
        let built = BuiltFile::new(
            IndexedFile::new(assembled, Stats::new(2, 2, 0)),
            Bytes::new(),
        );
        let part = |start, schema_id| {
            File::v1(partition.clone(), start, start + 10, hourly(), schema_id).unwrap()
        };

        let same = Assembly::new(built.clone(), parts.clone(), vec![part(0, 0), part(10, 0)]);
        let other_schema =
            Assembly::new(built.clone(), parts.clone(), vec![part(0, 0), part(10, 1)]);
        let one = Assembly::new(built, parts, vec![part(0, 0)]);

        assert!(same.is_ok());
        assert!(matches!(other_schema, Err(Error::AssemblyFiles(_))));
        assert!(matches!(one, Err(Error::AssemblyFiles(_))));
    }
}
