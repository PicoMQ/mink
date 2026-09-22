//! Every command the state machine accepts and the outcome each one produces.

use std::collections::BTreeMap;

use bytes::Bytes;
use mink_table::{Bucket, BucketId, Descriptor, Id, PartitionName, Path};
use s3stream::{CommitStreamSetObjectRequest, CompactStreamObjectRequest, StreamMetadata};

use crate::catalog::{Counter, KvSnapshotRow, LakeSnapshotRow};

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // short-lived; encoded and dropped
pub enum Command {
    RegisterNode {
        node_id: i32,
        node_epoch: i64,
        http_address: String,
        slots: u32,
        protocol_addresses: BTreeMap<String, String>,
    },

    PlaceStream {
        stream_id: u64,
    },

    CreateStream {
        node_id: i32,
        node_epoch: i64,
    },

    OpenStream {
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    },

    TrimStream {
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
        new_start_offset: u64,
    },

    CloseStream {
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    },

    DeleteStream {
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    },

    PrepareObject {
        node_id: i32,
        node_epoch: i64,
        count: u32,
        ttl_ms: i64,
        now_ms: i64,
    },

    CommitStreamSetObject {
        node_id: i32,
        node_epoch: i64,
        request: CommitStreamSetObjectRequest,
        now_ms: i64,
    },

    CompactStreamObject {
        node_id: i32,
        node_epoch: i64,
        request: CompactStreamObjectRequest,
        now_ms: i64,
    },

    ExpirePreparedObjects {
        now_ms: i64,
    },

    CleanDestroyedObjects {
        object_ids: Vec<u64>,
    },

    PutKv {
        key: String,
        value: Bytes,
    },

    PutKvIfAbsent {
        key: String,
        value: Bytes,
    },

    DeleteKv {
        key: String,
    },

    DeleteKvIfMatches {
        key: String,
        expected: Bytes,
    },

    TransferStream {
        stream_id: u64,
        from_node: i32,
        to_node: i32,
    },

    CompleteTransfer {
        stream_id: u64,
        epoch: i64,
    },

    CreateStreams {
        node_id: i32,
        node_epoch: i64,
        count: u32,
    },

    AllocateProducerIds {
        node_id: i32,
        node_epoch: i64,
        count: u32,
    },

    CreateDatabase {
        name: String,
        comment: Option<String>,
        custom: BTreeMap<String, String>,
        now_ms: i64,
    },

    DropDatabase {
        name: String,
    },

    CreateTable {
        path: Path,
        descriptor: Descriptor,
        leaders: BTreeMap<BucketId, i32>,
        coordinator_epoch: i32,
        now_ms: i64,
    },

    DropTable {
        path: Path,
    },

    AlterTable {
        path: Path,
        descriptor: Descriptor,
        now_ms: i64,
    },

    CreatePartition {
        path: Path,
        name: PartitionName,
        leaders: BTreeMap<BucketId, i32>,
        coordinator_epoch: i32,
        now_ms: i64,
    },

    DropPartition {
        path: Path,
        name: PartitionName,
    },

    LeadBucket {
        bucket: Bucket,
        node_id: i32,
        coordinator_epoch: i32,
    },

    CommitKvSnapshot {
        bucket: Bucket,
        snapshot: KvSnapshotRow,
        leader_epoch: i32,
        coordinator_epoch: i32,
    },

    DropKvSnapshot {
        bucket: Bucket,
        snapshot_id: u64,
    },

    CommitLakeSnapshot {
        table_id: Id,
        snapshot: LakeSnapshotRow,
    },

    Allocate {
        counter: Counter,
        count: u64,
    },

    RegisterCoordinator {
        node_id: i32,
        epoch: i32,
        address: String,
    },

    RegisterProducerOffsets {
        producer_id: String,
        offsets: BTreeMap<Bucket, i64>,
        expires_ms: i64,
        now_ms: i64,
    },

    DeleteProducerOffsets {
        producer_id: String,
    },

    ExpireProducerOffsets {
        now_ms: i64,
    },

    CommitGroupOffsets {
        group: String,
        offsets: BTreeMap<Bucket, i64>,
        expires_ms: i64,
    },

    DeleteGroupOffsets {
        group: String,
    },

    ExpireGroupOffsets {
        now_ms: i64,
    },
}

impl Command {
    pub fn type_code(&self) -> u8 {
        match self {
            Command::CreateStream { .. } => 1,
            Command::OpenStream { .. } => 2,
            Command::TrimStream { .. } => 3,
            Command::CloseStream { .. } => 4,
            Command::DeleteStream { .. } => 5,
            Command::PrepareObject { .. } => 6,
            Command::CommitStreamSetObject { .. } => 7,
            Command::CompactStreamObject { .. } => 8,
            Command::ExpirePreparedObjects { .. } => 9,
            Command::RegisterNode { .. } => 10,
            Command::CleanDestroyedObjects { .. } => 11,
            Command::PutKv { .. } => 12,
            Command::PutKvIfAbsent { .. } => 13,
            Command::DeleteKv { .. } => 14,
            Command::TransferStream { .. } => 15,
            Command::CompleteTransfer { .. } => 16,
            Command::CreateStreams { .. } => 17,
            Command::PlaceStream { .. } => 18,
            Command::DeleteKvIfMatches { .. } => 19,
            Command::AllocateProducerIds { .. } => 20,
            Command::CreateDatabase { .. } => 30,
            Command::DropDatabase { .. } => 31,
            Command::CreateTable { .. } => 32,
            Command::DropTable { .. } => 33,
            Command::AlterTable { .. } => 34,
            Command::CreatePartition { .. } => 36,
            Command::DropPartition { .. } => 37,
            Command::LeadBucket { .. } => 38,
            Command::CommitKvSnapshot { .. } => 39,
            Command::DropKvSnapshot { .. } => 40,
            Command::CommitLakeSnapshot { .. } => 41,
            Command::Allocate { .. } => 42,
            Command::RegisterCoordinator { .. } => 43,
            Command::RegisterProducerOffsets { .. } => 44,
            Command::DeleteProducerOffsets { .. } => 45,
            Command::ExpireProducerOffsets { .. } => 46,
            Command::CommitGroupOffsets { .. } => 47,
            Command::DeleteGroupOffsets { .. } => 48,
            Command::ExpireGroupOffsets { .. } => 49,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Unit,
    Id(u64),
    Count(u64),
    Stream(StreamMetadata),
    Value(Option<Bytes>),
    Ids(Vec<u64>),
    Bool(bool),
}
