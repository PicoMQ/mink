//! Binary encoding of commands and outcomes, with the shared field codecs the snapshot format reuses.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
pub(crate) use mink_common::codec::{
    get_bytes, get_i32, get_i64, get_json, get_opt_str, get_seq, get_str, get_str_map, get_u8,
    get_u32, get_u64, get_u64s, put_bytes, put_json, put_opt_str, put_seq, put_str, put_str_map,
    put_u64s,
};
use mink_table::{Bucket, BucketId, Id, Name, PartitionId, PartitionName, Path};
use s3stream::{
    CommitStreamSetObjectRequest, CompactOperations, CompactStreamObjectRequest, ObjectStreamRange,
    StreamMetadata, StreamObject, StreamState,
};

use crate::catalog::{Counter, KvSnapshotRow, LakeSnapshotRow, OffsetsRow};
use crate::command::{Command, Outcome};

pub const CODEC_VERSION: u8 = 0;

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("unsupported codec version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown command type {0}")]
    UnknownCommand(u8),
    #[error("unknown result tag {0}")]
    UnknownResult(u8),
    #[error("corrupt encoding: {0}")]
    Corrupt(String),
}

impl From<mink_common::codec::Error> for CodecError {
    fn from(error: mink_common::codec::Error) -> Self {
        match error {
            mink_common::codec::Error::Corrupt(reason) => CodecError::Corrupt(reason),
        }
    }
}

pub fn encode_command(command: &Command) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(CODEC_VERSION);
    put_command_body(&mut buf, command);

    buf.freeze()
}

pub fn decode_command(bytes: &[u8]) -> Result<Command, CodecError> {
    let mut buf = bytes;
    check_version(&mut buf, CODEC_VERSION)?;
    let command = get_command_body(&mut buf)?;
    ensure_drained(buf)?;

    Ok(command)
}

pub fn encode_batch(commands: &[Command]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(CODEC_VERSION);
    buf.put_u32_le(commands.len() as u32);
    for command in commands {
        put_command_body(&mut buf, command);
    }

    buf.freeze()
}

pub fn decode_batch(bytes: &[u8]) -> Result<Vec<Command>, CodecError> {
    let mut buf = bytes;
    check_version(&mut buf, CODEC_VERSION)?;
    let count = get_u32(&mut buf)? as usize;
    let mut commands = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        commands.push(get_command_body(&mut buf)?);
    }
    ensure_drained(buf)?;

    Ok(commands)
}

pub fn encode_result(result: &Outcome) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(CODEC_VERSION);
    match result {
        Outcome::Unit => buf.put_u8(0),
        Outcome::Id(id) => {
            buf.put_u8(1);
            buf.put_u64_le(*id);
        }
        Outcome::Count(count) => {
            buf.put_u8(2);
            buf.put_u64_le(*count);
        }
        Outcome::Stream(metadata) => {
            buf.put_u8(3);
            buf.put_u64_le(metadata.stream_id);
            buf.put_u64_le(metadata.epoch);
            buf.put_u64_le(metadata.start_offset);
            buf.put_u64_le(metadata.end_offset);
            put_stream_state(&mut buf, metadata.state);
            buf.put_i32_le(metadata.node_id);
        }
        Outcome::Value(Some(value)) => {
            buf.put_u8(5);
            put_bytes(&mut buf, value);
        }
        Outcome::Value(None) => buf.put_u8(6),
        Outcome::Ids(ids) => {
            buf.put_u8(7);
            put_u64s(&mut buf, ids);
        }
        Outcome::Bool(value) => {
            buf.put_u8(8);
            buf.put_u8(*value as u8);
        }
    }

    buf.freeze()
}

pub fn decode_result(bytes: &[u8]) -> Result<Outcome, CodecError> {
    let mut buf = bytes;
    check_version(&mut buf, CODEC_VERSION)?;
    let tag = get_u8(&mut buf)?;
    let result = match tag {
        0 => Outcome::Unit,
        1 => Outcome::Id(get_u64(&mut buf)?),
        2 => Outcome::Count(get_u64(&mut buf)?),
        3 => {
            let stream_id = get_u64(&mut buf)?;
            let epoch = get_u64(&mut buf)?;
            let start_offset = get_u64(&mut buf)?;
            let end_offset = get_u64(&mut buf)?;
            let state = get_stream_state(&mut buf)?;
            let node_id = get_i32(&mut buf)?;
            Outcome::Stream(StreamMetadata {
                stream_id,
                epoch,
                start_offset,
                end_offset,
                state,
                node_id,
            })
        }
        5 => Outcome::Value(Some(get_bytes(&mut buf)?)),
        6 => Outcome::Value(None),
        7 => Outcome::Ids(get_u64s(&mut buf)?),
        8 => Outcome::Bool(match get_u8(&mut buf)? {
            0 => false,
            1 => true,
            other => return Err(CodecError::Corrupt(format!("bool {other}"))),
        }),
        other => return Err(CodecError::UnknownResult(other)),
    };
    ensure_drained(buf)?;

    Ok(result)
}

fn put_command_body(buf: &mut BytesMut, command: &Command) {
    buf.put_u8(command.type_code());
    match command {
        Command::RegisterNode {
            node_id,
            node_epoch,
            http_address,
            slots,
            protocol_addresses,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            put_str(buf, http_address);
            buf.put_u32_le(*slots);
            put_str_map(buf, protocol_addresses);
        }
        Command::CreateStream {
            node_id,
            node_epoch,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
        }
        Command::OpenStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        }
        | Command::CloseStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        }
        | Command::DeleteStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_u64_le(*stream_id);
            buf.put_i64_le(*epoch);
        }
        Command::TrimStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
            new_start_offset,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_u64_le(*stream_id);
            buf.put_i64_le(*epoch);
            buf.put_u64_le(*new_start_offset);
        }
        Command::PrepareObject {
            node_id,
            node_epoch,
            count,
            ttl_ms,
            now_ms,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_u32_le(*count);
            buf.put_i64_le(*ttl_ms);
            buf.put_i64_le(*now_ms);
        }
        Command::CommitStreamSetObject {
            node_id,
            node_epoch,
            request,
            now_ms,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_i64_le(*now_ms);
            put_commit_request(buf, request);
        }
        Command::CompactStreamObject {
            node_id,
            node_epoch,
            request,
            now_ms,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_i64_le(*now_ms);
            put_compact_request(buf, request);
        }
        Command::ExpirePreparedObjects { now_ms } => {
            buf.put_i64_le(*now_ms);
        }
        Command::CleanDestroyedObjects { object_ids } => put_u64s(buf, object_ids),
        Command::PutKv { key, value } | Command::PutKvIfAbsent { key, value } => {
            put_str(buf, key);
            put_bytes(buf, value);
        }
        Command::DeleteKv { key } => {
            put_str(buf, key);
        }
        Command::DeleteKvIfMatches { key, expected } => {
            put_str(buf, key);
            put_bytes(buf, expected);
        }
        Command::TransferStream {
            stream_id,
            from_node,
            to_node,
        } => {
            buf.put_u64_le(*stream_id);
            buf.put_i32_le(*from_node);
            buf.put_i32_le(*to_node);
        }
        Command::CompleteTransfer { stream_id, epoch } => {
            buf.put_u64_le(*stream_id);
            buf.put_i64_le(*epoch);
        }
        Command::CreateStreams {
            node_id,
            node_epoch,
            count,
        }
        | Command::AllocateProducerIds {
            node_id,
            node_epoch,
            count,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i64_le(*node_epoch);
            buf.put_u32_le(*count);
        }
        Command::PlaceStream { stream_id } => {
            buf.put_u64_le(*stream_id);
        }
        Command::CreateDatabase {
            name,
            comment,
            custom,
            now_ms,
        } => {
            put_str(buf, name);
            put_opt_str(buf, comment.as_deref());
            put_str_map(buf, custom);
            buf.put_i64_le(*now_ms);
        }
        Command::DropDatabase { name } => put_str(buf, name),
        Command::CreateTable {
            path,
            descriptor,
            leaders,
            coordinator_epoch,
            now_ms,
        } => {
            put_path(buf, path);
            put_json(buf, descriptor);
            put_leaders(buf, leaders);
            buf.put_i32_le(*coordinator_epoch);
            buf.put_i64_le(*now_ms);
        }
        Command::DropTable { path } => put_path(buf, path),
        Command::AlterTable {
            path,
            descriptor,
            now_ms,
        } => {
            put_path(buf, path);
            put_json(buf, descriptor);
            buf.put_i64_le(*now_ms);
        }
        Command::CreatePartition {
            path,
            name,
            leaders,
            coordinator_epoch,
            now_ms,
        } => {
            put_path(buf, path);
            put_str(buf, name.as_str());
            put_leaders(buf, leaders);
            buf.put_i32_le(*coordinator_epoch);
            buf.put_i64_le(*now_ms);
        }
        Command::DropPartition { path, name } => {
            put_path(buf, path);
            put_str(buf, name.as_str());
        }
        Command::LeadBucket {
            bucket,
            node_id,
            coordinator_epoch,
        } => {
            put_bucket(buf, bucket);
            buf.put_i32_le(*node_id);
            buf.put_i32_le(*coordinator_epoch);
        }
        Command::CommitKvSnapshot {
            bucket,
            snapshot,
            leader_epoch,
            coordinator_epoch,
        } => {
            put_bucket(buf, bucket);
            put_kv_snapshot(buf, snapshot);
            buf.put_i32_le(*leader_epoch);
            buf.put_i32_le(*coordinator_epoch);
        }
        Command::DropKvSnapshot {
            bucket,
            snapshot_id,
        } => {
            put_bucket(buf, bucket);
            buf.put_u64_le(*snapshot_id);
        }
        Command::CommitLakeSnapshot { table_id, snapshot } => {
            buf.put_u64_le(table_id.0);
            put_lake_snapshot(buf, snapshot);
        }
        Command::Allocate { counter, count } => {
            put_counter(buf, counter);
            buf.put_u64_le(*count);
        }
        Command::RegisterCoordinator {
            node_id,
            epoch,
            address,
        } => {
            buf.put_i32_le(*node_id);
            buf.put_i32_le(*epoch);
            put_str(buf, address);
        }
        Command::RegisterProducerOffsets {
            producer_id,
            offsets,
            expires_ms,
            now_ms,
        } => {
            put_str(buf, producer_id);
            put_bucket_offsets(buf, offsets);
            buf.put_i64_le(*expires_ms);
            buf.put_i64_le(*now_ms);
        }
        Command::DeleteProducerOffsets { producer_id } => put_str(buf, producer_id),
        Command::ExpireProducerOffsets { now_ms } => buf.put_i64_le(*now_ms),
        Command::CommitGroupOffsets {
            group,
            offsets,
            expires_ms,
        } => {
            put_str(buf, group);
            put_bucket_offsets(buf, offsets);
            buf.put_i64_le(*expires_ms);
        }
        Command::DeleteGroupOffsets { group } => put_str(buf, group),
        Command::ExpireGroupOffsets { now_ms } => buf.put_i64_le(*now_ms),
    }
}

fn get_command_body(buf: &mut &[u8]) -> Result<Command, CodecError> {
    let type_code = get_u8(buf)?;
    Ok(match type_code {
        1 => Command::CreateStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
        },
        2 => Command::OpenStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
        },
        3 => Command::TrimStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
            new_start_offset: get_u64(buf)?,
        },
        4 => Command::CloseStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
        },
        5 => Command::DeleteStream {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
        },
        6 => Command::PrepareObject {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            count: get_u32(buf)?,
            ttl_ms: get_i64(buf)?,
            now_ms: get_i64(buf)?,
        },
        7 => {
            let node_id = get_i32(buf)?;
            let node_epoch = get_i64(buf)?;
            let now_ms = get_i64(buf)?;
            let request = get_commit_request(buf)?;
            Command::CommitStreamSetObject {
                node_id,
                node_epoch,
                request,
                now_ms,
            }
        }
        8 => {
            let node_id = get_i32(buf)?;
            let node_epoch = get_i64(buf)?;
            let now_ms = get_i64(buf)?;
            let request = get_compact_request(buf)?;
            Command::CompactStreamObject {
                node_id,
                node_epoch,
                request,
                now_ms,
            }
        }
        9 => Command::ExpirePreparedObjects {
            now_ms: get_i64(buf)?,
        },
        10 => {
            let node_id = get_i32(buf)?;
            let node_epoch = get_i64(buf)?;
            let http_address = get_str(buf)?;
            let slots = get_u32(buf)?;
            let protocol_addresses = get_str_map(buf)?;
            Command::RegisterNode {
                node_id,
                node_epoch,
                http_address,
                slots,
                protocol_addresses,
            }
        }
        11 => {
            let count = get_u32(buf)? as usize;
            let mut object_ids = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                object_ids.push(get_u64(buf)?);
            }
            Command::CleanDestroyedObjects { object_ids }
        }
        12 => Command::PutKv {
            key: get_str(buf)?,
            value: get_bytes(buf)?,
        },
        13 => Command::PutKvIfAbsent {
            key: get_str(buf)?,
            value: get_bytes(buf)?,
        },
        14 => Command::DeleteKv { key: get_str(buf)? },
        15 => Command::TransferStream {
            stream_id: get_u64(buf)?,
            from_node: get_i32(buf)?,
            to_node: get_i32(buf)?,
        },
        16 => Command::CompleteTransfer {
            stream_id: get_u64(buf)?,
            epoch: get_i64(buf)?,
        },
        17 => Command::CreateStreams {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            count: get_u32(buf)?,
        },
        18 => Command::PlaceStream {
            stream_id: get_u64(buf)?,
        },
        19 => Command::DeleteKvIfMatches {
            key: get_str(buf)?,
            expected: get_bytes(buf)?,
        },
        20 => Command::AllocateProducerIds {
            node_id: get_i32(buf)?,
            node_epoch: get_i64(buf)?,
            count: get_u32(buf)?,
        },
        30 => Command::CreateDatabase {
            name: get_str(buf)?,
            comment: get_opt_str(buf)?,
            custom: get_str_map(buf)?,
            now_ms: get_i64(buf)?,
        },
        31 => Command::DropDatabase {
            name: get_str(buf)?,
        },
        32 => Command::CreateTable {
            path: get_path(buf)?,
            descriptor: get_json(buf)?,
            leaders: get_leaders(buf)?,
            coordinator_epoch: get_i32(buf)?,
            now_ms: get_i64(buf)?,
        },
        33 => Command::DropTable {
            path: get_path(buf)?,
        },
        34 => Command::AlterTable {
            path: get_path(buf)?,
            descriptor: get_json(buf)?,
            now_ms: get_i64(buf)?,
        },
        36 => Command::CreatePartition {
            path: get_path(buf)?,
            name: get_partition_name(buf)?,
            leaders: get_leaders(buf)?,
            coordinator_epoch: get_i32(buf)?,
            now_ms: get_i64(buf)?,
        },
        37 => Command::DropPartition {
            path: get_path(buf)?,
            name: get_partition_name(buf)?,
        },
        38 => Command::LeadBucket {
            bucket: get_bucket(buf)?,
            node_id: get_i32(buf)?,
            coordinator_epoch: get_i32(buf)?,
        },
        39 => Command::CommitKvSnapshot {
            bucket: get_bucket(buf)?,
            snapshot: get_kv_snapshot(buf)?,
            leader_epoch: get_i32(buf)?,
            coordinator_epoch: get_i32(buf)?,
        },
        40 => Command::DropKvSnapshot {
            bucket: get_bucket(buf)?,
            snapshot_id: get_u64(buf)?,
        },
        41 => Command::CommitLakeSnapshot {
            table_id: Id(get_u64(buf)?),
            snapshot: get_lake_snapshot(buf)?,
        },
        42 => Command::Allocate {
            counter: get_counter(buf)?,
            count: get_u64(buf)?,
        },
        43 => Command::RegisterCoordinator {
            node_id: get_i32(buf)?,
            epoch: get_i32(buf)?,
            address: get_str(buf)?,
        },
        44 => Command::RegisterProducerOffsets {
            producer_id: get_str(buf)?,
            offsets: get_bucket_offsets(buf)?,
            expires_ms: get_i64(buf)?,
            now_ms: get_i64(buf)?,
        },
        45 => Command::DeleteProducerOffsets {
            producer_id: get_str(buf)?,
        },
        46 => Command::ExpireProducerOffsets {
            now_ms: get_i64(buf)?,
        },
        47 => Command::CommitGroupOffsets {
            group: get_str(buf)?,
            offsets: get_bucket_offsets(buf)?,
            expires_ms: get_i64(buf)?,
        },
        48 => Command::DeleteGroupOffsets {
            group: get_str(buf)?,
        },
        49 => Command::ExpireGroupOffsets {
            now_ms: get_i64(buf)?,
        },
        other => return Err(CodecError::UnknownCommand(other)),
    })
}

fn put_commit_request(buf: &mut BytesMut, request: &CommitStreamSetObjectRequest) {
    buf.put_u64_le(request.object_id);
    buf.put_u64_le(request.object_size);
    buf.put_u32_le(request.attributes);
    put_seq(buf, &request.stream_ranges, |buf, range| {
        buf.put_u64_le(range.stream_id);
        buf.put_u64_le(range.epoch);
        buf.put_u64_le(range.start_offset);
        buf.put_u64_le(range.end_offset);
        buf.put_u64_le(range.size);
    });
    put_seq(buf, &request.stream_objects, |buf, object| {
        buf.put_u64_le(object.object_id);
        buf.put_u64_le(object.object_size);
        buf.put_u64_le(object.stream_id);
        buf.put_u64_le(object.start_offset);
        buf.put_u64_le(object.end_offset);
        buf.put_u32_le(object.attributes);
    });
    put_u64s(buf, &request.compacted_object_ids);
}

fn get_commit_request(buf: &mut &[u8]) -> Result<CommitStreamSetObjectRequest, CodecError> {
    let object_id = get_u64(buf)?;
    let object_size = get_u64(buf)?;
    let attributes = get_u32(buf)?;
    let stream_ranges = get_seq(buf, |buf| {
        Ok::<_, CodecError>(ObjectStreamRange {
            stream_id: get_u64(buf)?,
            epoch: get_u64(buf)?,
            start_offset: get_u64(buf)?,
            end_offset: get_u64(buf)?,
            size: get_u64(buf)?,
        })
    })?;
    let stream_objects = get_seq(buf, |buf| {
        Ok::<_, CodecError>(StreamObject {
            object_id: get_u64(buf)?,
            object_size: get_u64(buf)?,
            stream_id: get_u64(buf)?,
            start_offset: get_u64(buf)?,
            end_offset: get_u64(buf)?,
            attributes: get_u32(buf)?,
        })
    })?;
    let compacted_object_ids = get_u64s(buf)?;

    Ok(CommitStreamSetObjectRequest {
        object_id,
        object_size,
        attributes,
        stream_ranges,
        stream_objects,
        compacted_object_ids,
    })
}

fn put_compact_request(buf: &mut BytesMut, request: &CompactStreamObjectRequest) {
    buf.put_u64_le(request.object_id);
    buf.put_u64_le(request.object_size);
    buf.put_u64_le(request.stream_id);
    buf.put_u64_le(request.stream_epoch);
    buf.put_u64_le(request.start_offset);
    buf.put_u64_le(request.end_offset);
    buf.put_u32_le(request.attributes);
    put_u64s(buf, &request.source_object_ids);
    put_seq(buf, &request.operations, |buf, op| put_compact_op(buf, *op));
}

fn get_compact_request(buf: &mut &[u8]) -> Result<CompactStreamObjectRequest, CodecError> {
    let object_id = get_u64(buf)?;
    let object_size = get_u64(buf)?;
    let stream_id = get_u64(buf)?;
    let stream_epoch = get_u64(buf)?;
    let start_offset = get_u64(buf)?;
    let end_offset = get_u64(buf)?;
    let attributes = get_u32(buf)?;
    let source_object_ids = get_u64s(buf)?;
    let operations = get_seq(buf, get_compact_op)?;

    Ok(CompactStreamObjectRequest {
        object_id,
        object_size,
        stream_id,
        stream_epoch,
        start_offset,
        end_offset,
        source_object_ids,
        operations,
        attributes,
    })
}

pub(crate) fn put_stream_state(buf: &mut BytesMut, state: StreamState) {
    buf.put_u8(match state {
        StreamState::Closed => 0,
        StreamState::Opened => 1,
    });
}

pub(crate) fn get_stream_state(buf: &mut &[u8]) -> Result<StreamState, CodecError> {
    match get_u8(buf)? {
        0 => Ok(StreamState::Closed),
        1 => Ok(StreamState::Opened),
        other => Err(CodecError::Corrupt(format!("stream state {other}"))),
    }
}

pub(crate) fn put_compact_op(buf: &mut BytesMut, op: CompactOperations) {
    buf.put_u8(op as u8);
}

pub(crate) fn get_compact_op(buf: &mut &[u8]) -> Result<CompactOperations, CodecError> {
    match get_u8(buf)? {
        0 => Ok(CompactOperations::Delete),
        1 => Ok(CompactOperations::KeepData),
        2 => Ok(CompactOperations::DeepDelete),
        other => Err(CodecError::Corrupt(format!("compact operation {other}"))),
    }
}

pub(crate) fn check_version(buf: &mut &[u8], expected: u8) -> Result<(), CodecError> {
    let version = get_u8(buf)?;
    if version != expected {
        return Err(CodecError::UnsupportedVersion(version));
    }

    Ok(())
}

pub(crate) fn ensure_drained(buf: &[u8]) -> Result<(), CodecError> {
    if buf.has_remaining() {
        return Err(CodecError::Corrupt(format!(
            "{} trailing bytes",
            buf.remaining()
        )));
    }

    Ok(())
}

pub(crate) fn put_path(buf: &mut BytesMut, path: &Path) {
    put_str(buf, path.database().as_str());
    put_str(buf, path.table().as_str());
}

pub(crate) fn get_path(buf: &mut &[u8]) -> Result<Path, CodecError> {
    let database = get_name(buf)?;
    let table = get_name(buf)?;

    Ok(Path::new(database, table))
}

fn get_name(buf: &mut &[u8]) -> Result<Name, CodecError> {
    Name::new(get_str(buf)?).map_err(|e| CodecError::Corrupt(e.to_string()))
}

pub(crate) fn get_partition_name(buf: &mut &[u8]) -> Result<PartitionName, CodecError> {
    get_str(buf)?
        .parse()
        .map_err(|e: mink_table::Error| CodecError::Corrupt(e.to_string()))
}

fn put_leaders(buf: &mut BytesMut, leaders: &BTreeMap<BucketId, i32>) {
    buf.put_u32_le(leaders.len() as u32);
    for (bucket, node) in leaders {
        buf.put_u32_le(bucket.0);
        buf.put_i32_le(*node);
    }
}

fn get_leaders(buf: &mut &[u8]) -> Result<BTreeMap<BucketId, i32>, CodecError> {
    let len = get_u32(buf)? as usize;
    let mut leaders = BTreeMap::new();
    for _ in 0..len {
        let bucket = BucketId(get_u32(buf)?);
        let node = get_i32(buf)?;
        leaders.insert(bucket, node);
    }

    Ok(leaders)
}

pub(crate) fn put_bucket(buf: &mut BytesMut, bucket: &Bucket) {
    buf.put_u64_le(bucket.table().0);
    match bucket.partition() {
        Some(partition) => {
            buf.put_u8(1);
            buf.put_u64_le(partition.0);
        }
        None => buf.put_u8(0),
    }
    buf.put_u32_le(bucket.bucket().0);
}

pub(crate) fn get_bucket(buf: &mut &[u8]) -> Result<Bucket, CodecError> {
    let table = Id(get_u64(buf)?);
    let partition = match get_u8(buf)? {
        0 => None,
        1 => Some(PartitionId(get_u64(buf)?)),
        other => return Err(CodecError::Corrupt(format!("partition tag {other}"))),
    };
    let bucket = BucketId(get_u32(buf)?);

    Ok(Bucket::of(table, partition, bucket))
}

pub(crate) fn put_kv_snapshot(buf: &mut BytesMut, snapshot: &KvSnapshotRow) {
    buf.put_u64_le(snapshot.snapshot_id);
    buf.put_i64_le(snapshot.log_offset);
    buf.put_i64_le(snapshot.row_count);
    put_str(buf, &snapshot.path);
}

pub(crate) fn get_kv_snapshot(buf: &mut &[u8]) -> Result<KvSnapshotRow, CodecError> {
    Ok(KvSnapshotRow {
        snapshot_id: get_u64(buf)?,
        log_offset: get_i64(buf)?,
        row_count: get_i64(buf)?,
        path: get_str(buf)?,
    })
}

pub(crate) fn put_lake_snapshot(buf: &mut BytesMut, snapshot: &LakeSnapshotRow) {
    buf.put_i64_le(snapshot.snapshot_id);
    put_bucket_offsets(buf, &snapshot.bucket_log_end_offset);
}

pub(crate) fn get_lake_snapshot(buf: &mut &[u8]) -> Result<LakeSnapshotRow, CodecError> {
    Ok(LakeSnapshotRow {
        snapshot_id: get_i64(buf)?,
        bucket_log_end_offset: get_bucket_offsets(buf)?,
    })
}

pub(crate) fn put_bucket_offsets(buf: &mut BytesMut, offsets: &BTreeMap<Bucket, i64>) {
    buf.put_u32_le(offsets.len() as u32);
    for (bucket, offset) in offsets {
        put_bucket(buf, bucket);
        buf.put_i64_le(*offset);
    }
}

pub(crate) fn get_bucket_offsets(buf: &mut &[u8]) -> Result<BTreeMap<Bucket, i64>, CodecError> {
    let len = get_u32(buf)? as usize;
    let mut offsets = BTreeMap::new();
    for _ in 0..len {
        let bucket = get_bucket(buf)?;
        let offset = get_i64(buf)?;
        offsets.insert(bucket, offset);
    }

    Ok(offsets)
}

pub(crate) fn put_offsets_row(buf: &mut BytesMut, row: &OffsetsRow) {
    buf.put_i64_le(row.expires_ms);
    put_bucket_offsets(buf, &row.offsets);
}

pub(crate) fn get_offsets_row(buf: &mut &[u8]) -> Result<OffsetsRow, CodecError> {
    Ok(OffsetsRow {
        expires_ms: get_i64(buf)?,
        offsets: get_bucket_offsets(buf)?,
    })
}

pub(crate) fn put_counter(buf: &mut BytesMut, counter: &Counter) {
    match counter {
        Counter::SnapshotId(bucket) => {
            buf.put_u8(0);
            put_bucket(buf, bucket);
        }
        Counter::AutoIncrement {
            table_id,
            column_id,
        } => {
            buf.put_u8(1);
            buf.put_u64_le(table_id.0);
            buf.put_u32_le(*column_id);
        }
    }
}

pub(crate) fn get_counter(buf: &mut &[u8]) -> Result<Counter, CodecError> {
    Ok(match get_u8(buf)? {
        0 => Counter::SnapshotId(get_bucket(buf)?),
        1 => Counter::AutoIncrement {
            table_id: Id(get_u64(buf)?),
            column_id: get_u32(buf)?,
        },
        other => return Err(CodecError::Corrupt(format!("counter tag {other}"))),
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn all_commands() -> Vec<Command> {
        vec![
            Command::CreateStream {
                node_id: 7,
                node_epoch: 100,
            },
            Command::OpenStream {
                node_id: 7,
                node_epoch: 100,
                stream_id: 3,
                epoch: 9,
            },
            Command::TrimStream {
                node_id: 7,
                node_epoch: 100,
                stream_id: 3,
                epoch: 9,
                new_start_offset: 4,
            },
            Command::CloseStream {
                node_id: 7,
                node_epoch: 100,
                stream_id: 3,
                epoch: 9,
            },
            Command::DeleteStream {
                node_id: 7,
                node_epoch: 100,
                stream_id: 3,
                epoch: 9,
            },
            Command::PrepareObject {
                node_id: 7,
                node_epoch: 100,
                count: 2,
                ttl_ms: 1000,
                now_ms: 50,
            },
            Command::CommitStreamSetObject {
                node_id: 7,
                node_epoch: 100,
                request: CommitStreamSetObjectRequest {
                    object_id: 11,
                    object_size: 128,
                    attributes: 1,
                    stream_ranges: vec![ObjectStreamRange {
                        stream_id: 3,
                        epoch: 1,
                        start_offset: 0,
                        end_offset: 10,
                        size: 128,
                    }],
                    stream_objects: vec![StreamObject {
                        object_id: 12,
                        object_size: 64,
                        stream_id: 3,
                        start_offset: 10,
                        end_offset: 20,
                        attributes: 2,
                    }],
                    compacted_object_ids: vec![1, 2],
                },
                now_ms: 123,
            },
            Command::CompactStreamObject {
                node_id: 7,
                node_epoch: 100,
                request: CompactStreamObjectRequest {
                    object_id: 20,
                    object_size: 64,
                    stream_id: 3,
                    stream_epoch: 9,
                    start_offset: 0,
                    end_offset: 20,
                    source_object_ids: vec![12, 13, 14],
                    operations: vec![
                        CompactOperations::Delete,
                        CompactOperations::KeepData,
                        CompactOperations::DeepDelete,
                    ],
                    attributes: 3,
                },
                now_ms: 456,
            },
            Command::ExpirePreparedObjects { now_ms: 99 },
            Command::RegisterNode {
                node_id: 7,
                node_epoch: 100,
                http_address: "http://127.0.0.1:8080".into(),
                slots: 4,
                protocol_addresses: BTreeMap::from([(
                    "kafka".to_owned(),
                    "127.0.0.1:9092".to_owned(),
                )]),
            },
            Command::RegisterNode {
                node_id: 7,
                node_epoch: 100,
                http_address: "".into(),
                slots: 1,
                protocol_addresses: Default::default(),
            },
            Command::PlaceStream { stream_id: 42 },
            Command::TransferStream {
                stream_id: 3,
                from_node: 7,
                to_node: 8,
            },
            Command::CompleteTransfer {
                stream_id: 3,
                epoch: 9,
            },
            Command::CreateStreams {
                node_id: 7,
                node_epoch: 100,
                count: 16,
            },
            Command::AllocateProducerIds {
                node_id: 7,
                node_epoch: 100,
                count: 8,
            },
            Command::CleanDestroyedObjects {
                object_ids: vec![1, 2, 3],
            },
            Command::PutKv {
                key: "path/a".into(),
                value: Bytes::from_static(&[1, 2, 3]),
            },
            Command::PutKvIfAbsent {
                key: "path/b".into(),
                value: Bytes::from_static(&[4, 5]),
            },
            Command::DeleteKv {
                key: "path/c".into(),
            },
            Command::DeleteKvIfMatches {
                key: "path/d".into(),
                expected: Bytes::from_static(&[6, 7, 8]),
            },
        ]
    }

    fn all_results() -> Vec<Outcome> {
        vec![
            Outcome::Unit,
            Outcome::Id(42),
            Outcome::Count(7),
            Outcome::Stream(StreamMetadata {
                stream_id: 3,
                epoch: 9,
                start_offset: 0,
                end_offset: 20,
                state: StreamState::Opened,
                node_id: 7,
            }),
            Outcome::Value(Some(Bytes::from_static(&[1, 2, 3]))),
            Outcome::Value(None),
            Outcome::Bool(true),
            Outcome::Bool(false),
        ]
    }

    #[test]
    fn every_command_roundtrips() {
        for command in all_commands() {
            let encoded = encode_command(&command);
            let decoded = decode_command(&encoded).unwrap();
            assert_eq!(decoded, command);
            assert_eq!(encode_command(&decoded), encoded);
        }
    }

    #[test]
    fn every_result_roundtrips() {
        for result in all_results() {
            let encoded = encode_result(&result);
            let decoded = decode_result(&encoded).unwrap();
            assert_eq!(decoded, result);
            assert_eq!(encode_result(&decoded), encoded);
        }
    }

    #[test]
    fn batch_roundtrips_in_order() {
        let commands = all_commands();
        let encoded = encode_batch(&commands);
        assert_eq!(decode_batch(&encoded).unwrap(), commands);

        let empty = encode_batch(&[]);
        assert_eq!(decode_batch(&empty).unwrap(), Vec::<Command>::new());
    }

    #[test]
    fn rejects_unknown_version_type_and_tag() {
        let mut bytes = encode_command(&all_commands()[0]).to_vec();
        bytes[0] = CODEC_VERSION + 1;
        assert!(matches!(
            decode_command(&bytes),
            Err(CodecError::UnsupportedVersion(v)) if v == CODEC_VERSION + 1
        ));

        for type_code in [0u8, 21, 200] {
            let bytes = [CODEC_VERSION, type_code];
            assert!(matches!(
                decode_command(&bytes),
                Err(CodecError::UnknownCommand(t)) if t == type_code
            ));
        }

        let bytes = [CODEC_VERSION, 4u8];
        assert!(matches!(
            decode_result(&bytes),
            Err(CodecError::UnknownResult(4))
        ));
    }

    #[test]
    fn rejects_truncation_and_trailing_bytes() {
        for command in all_commands() {
            let bytes = encode_command(&command);
            let old_register_len =
                matches!(&command, Command::RegisterNode { .. }).then(|| bytes.len() - 4);
            for len in 0..bytes.len() {
                if old_register_len == Some(len) {
                    continue;
                }
                assert!(
                    decode_command(&bytes[..len]).is_err(),
                    "prefix of {len} bytes must be rejected for {command:?}"
                );
            }
            let mut extended = bytes.to_vec();
            extended.push(0);
            assert!(matches!(
                decode_command(&extended),
                Err(CodecError::Corrupt(_))
            ));
        }
    }

    fn arb_command() -> impl Strategy<Value = Command> {
        prop_oneof![
            (any::<i32>(), any::<i64>()).prop_map(|(node_id, node_epoch)| {
                Command::CreateStream {
                    node_id,
                    node_epoch,
                }
            }),
            (any::<i32>(), any::<i64>(), any::<u64>(), any::<i64>()).prop_map(
                |(node_id, node_epoch, stream_id, epoch)| Command::OpenStream {
                    node_id,
                    node_epoch,
                    stream_id,
                    epoch
                }
            ),
            (
                any::<i32>(),
                any::<i64>(),
                any::<u64>(),
                any::<i64>(),
                any::<u64>()
            )
                .prop_map(
                    |(node_id, node_epoch, stream_id, epoch, new_start_offset)| {
                        Command::TrimStream {
                            node_id,
                            node_epoch,
                            stream_id,
                            epoch,
                            new_start_offset,
                        }
                    }
                ),
            (
                any::<i32>(),
                any::<i64>(),
                "[a-z/]{0,32}",
                1u32..8,
                proptest::collection::btree_map("[a-z]{1,8}", "[a-z:]{0,16}", 0..3)
            )
                .prop_map(
                    |(node_id, node_epoch, http_address, slots, protocol_addresses)| {
                        Command::RegisterNode {
                            node_id,
                            node_epoch,
                            http_address,
                            slots,
                            protocol_addresses,
                        }
                    }
                ),
            any::<u64>().prop_map(|stream_id| Command::PlaceStream { stream_id }),
            (any::<u64>(), any::<i32>(), any::<i32>()).prop_map(
                |(stream_id, from_node, to_node)| Command::TransferStream {
                    stream_id,
                    from_node,
                    to_node,
                }
            ),
            (any::<u64>(), any::<i64>())
                .prop_map(|(stream_id, epoch)| { Command::CompleteTransfer { stream_id, epoch } }),
            (any::<i32>(), any::<i64>(), any::<u32>()).prop_map(|(node_id, node_epoch, count)| {
                Command::CreateStreams {
                    node_id,
                    node_epoch,
                    count,
                }
            }),
            (any::<i32>(), any::<i64>(), any::<u32>()).prop_map(|(node_id, node_epoch, count)| {
                Command::AllocateProducerIds {
                    node_id,
                    node_epoch,
                    count,
                }
            }),
            proptest::collection::vec(any::<u64>(), 0..64)
                .prop_map(|object_ids| Command::CleanDestroyedObjects { object_ids }),
            (
                "[a-z/]{0,32}",
                proptest::collection::vec(any::<u8>(), 0..64)
            )
                .prop_map(|(key, value)| Command::PutKv {
                    key,
                    value: Bytes::from(value)
                }),
            (
                "[a-z/]{0,32}",
                proptest::collection::vec(any::<u8>(), 0..64)
            )
                .prop_map(|(key, expected)| Command::DeleteKvIfMatches {
                    key,
                    expected: Bytes::from(expected)
                }),
        ]
    }

    proptest! {
        #[test]
        fn roundtrip_arbitrary_commands(command in arb_command()) {
            let encoded = encode_command(&command);
            prop_assert_eq!(&decode_command(&encoded).unwrap(), &command);
            prop_assert_eq!(encode_command(&decode_command(&encoded).unwrap()), encoded);
        }

        #[test]
        fn roundtrip_arbitrary_batches(
            commands in proptest::collection::vec(arb_command(), 0..16)
        ) {
            let encoded = encode_batch(&commands);
            prop_assert_eq!(decode_batch(&encoded).unwrap(), commands);
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = decode_command(&bytes);
            let _ = decode_batch(&bytes);
            let _ = decode_result(&bytes);
        }
    }
}
