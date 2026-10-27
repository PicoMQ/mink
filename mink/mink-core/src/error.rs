#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("{0:?} is not a legal Kafka topic name")]
    TopicName(String),
    #[error("a topic needs at least one partition, not {0}")]
    PartitionCount(i32),
    #[error("partition index {0} is negative")]
    PartitionIndex(i32),
    #[error("offsets {start}..{end} are not a range of non-negative offsets in order")]
    OffsetRange { start: i64, end: i64 },
    #[error("a file needs at least one offset, not the empty range at {0}")]
    EmptyFile(i64),
    #[error("schema id {0} is negative")]
    SchemaId(i32),
    #[error("a topic needs between one and i32::MAX time specs, not {0}")]
    TimeSpecs(usize),
    #[error("time spec id {0} is negative")]
    SpecId(i32),
    #[error(
        "an assembled file needs two or more non-empty parts in offset order, matching its hash"
    )]
    Parts,
    #[error("record offset {0} is negative")]
    RecordOffset(i64),
    #[error("snapshot id {0} is not positive")]
    SnapshotId(i64),
    #[error("snapshot epoch {0} is not positive")]
    SnapshotEpoch(i64),
    #[error("a snapshot needs the offsets of at least one partition")]
    EmptySnapshot,
    #[error("a snapshot's offsets must cover distinct partitions of one topic")]
    SnapshotPartitions,
    #[error("{0:?} cannot replace files of another partition, time, schema or version")]
    AssemblyFiles(crate::File),
    #[error("a theta sketch cannot be read: {0}")]
    Sketch(String),
}
