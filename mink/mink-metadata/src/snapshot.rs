//! Binary encoding of the whole state for snapshots, rebuilding derived indexes on decode.

use bytes::{BufMut, Bytes, BytesMut};
use im::OrdMap;
use mink_common::codec::{
    get_bytes, get_i32, get_i64, get_seq, get_str, get_str_map, get_u8, get_u32, get_u64,
    put_bytes, put_seq,
};
use mink_table::{Id, PartitionId};
use s3stream::{ObjectAttributes, S3ObjectMetadata, S3ObjectType, StreamOffsetRange, StreamState};

use crate::catalog::{
    BucketRow, Catalog, CoordinatorRow, DatabaseRow, OffsetsRow, PartitionRow, TableRow,
};
use crate::codec::{self, CodecError};
use crate::state::{
    NodeRow, PendingTransfer, State, StreamObjectRow, StreamRow, StreamSetObjectRow,
};

pub const SNAPSHOT_VERSION: u8 = 0;

pub fn encode(state: &State) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(SNAPSHOT_VERSION);

    buf.put_u64_le(state.next_stream_id);
    buf.put_u64_le(state.streams.len() as u64);
    for (_, row) in state.streams.iter() {
        buf.put_u64_le(row.stream_id);
        buf.put_i64_le(row.epoch);
        buf.put_u64_le(row.start_offset);
        buf.put_u64_le(row.end_offset);
        codec::put_stream_state(&mut buf, row.state);
        buf.put_i32_le(row.node_id);
    }

    buf.put_u64_le(state.nodes.len() as u64);
    for (_, node) in state.nodes.iter() {
        buf.put_i32_le(node.node_id);
        buf.put_i64_le(node.epoch);
        codec::put_str(&mut buf, &node.http_address);
        buf.put_u32_le(node.slots);
        codec::put_str_map(&mut buf, &node.protocol_addresses);
    }
    buf.put_u64_le(state.next_producer_id);

    buf.put_u64_le(state.next_object_id);
    buf.put_u64_le(state.prepared.len() as u64);
    for (id, deadline) in state.prepared.iter() {
        buf.put_u64_le(*id);
        buf.put_i64_le(*deadline);
    }

    buf.put_u64_le(state.stream_set_objects.len() as u64);
    for (_, row) in state.stream_set_objects.iter() {
        buf.put_i32_le(row.node_id);
        put_object(&mut buf, &row.object);
    }

    buf.put_u64_le(state.stream_objects.len() as u64);
    for (_, row) in state.stream_objects.iter() {
        put_object(&mut buf, &row.object);
    }

    buf.put_u64_le(state.mark_destroyed.len() as u64);
    for (seq, (object_id, op)) in state.mark_destroyed.iter() {
        buf.put_u64_le(*seq);
        buf.put_u64_le(*object_id);
        codec::put_compact_op(&mut buf, *op);
    }
    buf.put_u64_le(state.next_destroyed_seq);

    buf.put_u64_le(state.kv.len() as u64);
    for (key, value) in state.kv.iter() {
        codec::put_str(&mut buf, key);
        put_bytes(&mut buf, value);
    }

    buf.put_u64_le(state.pending_transfers.len() as u64);
    for (stream_id, pending) in state.pending_transfers.iter() {
        buf.put_u64_le(*stream_id);
        buf.put_i32_le(pending.from_node);
        buf.put_i32_le(pending.to_node);
    }

    put_catalog(&mut buf, &state.catalog);

    buf.freeze()
}

pub fn decode(bytes: &[u8]) -> Result<State, CodecError> {
    let mut buf = bytes;
    codec::check_version(&mut buf, SNAPSHOT_VERSION)?;

    let mut state = State::new();
    state.next_stream_id = get_u64(&mut buf)?;
    for _ in 0..get_u64(&mut buf)? {
        let stream_id = get_u64(&mut buf)?;
        let epoch = get_i64(&mut buf)?;
        let start_offset = get_u64(&mut buf)?;
        let end_offset = get_u64(&mut buf)?;
        let stream_state = codec::get_stream_state(&mut buf)?;
        let node_id = get_i32(&mut buf)?;
        let row = StreamRow {
            stream_id,
            epoch,
            start_offset,
            end_offset,
            state: stream_state,
            node_id,
        };
        state.streams.insert(stream_id, row);
        if stream_state == StreamState::Opened {
            state.opening_by_node.insert((node_id, stream_id), ());
        } else if stream_state == StreamState::Closed && epoch == -1 && node_id != -1 {
            state.placed_by_node.insert((node_id, stream_id), ());
        }
    }

    for _ in 0..get_u64(&mut buf)? {
        let node_id = get_i32(&mut buf)?;
        let epoch = get_i64(&mut buf)?;
        let http_address = get_str(&mut buf)?;
        let slots = get_u32(&mut buf)?;
        let protocol_addresses = get_str_map(&mut buf)?;
        state.nodes.insert(
            node_id,
            NodeRow {
                node_id,
                epoch,
                http_address,
                slots,
                protocol_addresses,
            },
        );
    }
    state.next_producer_id = get_u64(&mut buf)?;

    state.next_object_id = get_u64(&mut buf)?;
    for _ in 0..get_u64(&mut buf)? {
        let id = get_u64(&mut buf)?;
        let deadline = get_i64(&mut buf)?;
        state.prepared.insert(id, deadline);
        state.prepared_by_deadline.insert((deadline, id), ());
    }

    for _ in 0..get_u64(&mut buf)? {
        let node_id = get_i32(&mut buf)?;
        let object = get_object(&mut buf)?;
        for range in &object.offset_ranges {
            state.sso_ranges.insert(
                (range.stream_id, range.start_offset, object.object_id),
                range.end_offset,
            );
        }
        state.sso_by_node.insert((node_id, object.object_id), ());
        state
            .stream_set_objects
            .insert(object.object_id, StreamSetObjectRow { node_id, object });
    }

    for _ in 0..get_u64(&mut buf)? {
        let object = get_object(&mut buf)?;
        let [range] = object.offset_ranges.as_slice() else {
            return Err(CodecError::Corrupt(
                "stream object must have exactly one range".into(),
            ));
        };
        let key = (range.stream_id, range.start_offset, object.object_id);
        state.stream_object_ids.insert(object.object_id, key);
        state.stream_objects.insert(key, StreamObjectRow { object });
    }

    for _ in 0..get_u64(&mut buf)? {
        let seq = get_u64(&mut buf)?;
        let object_id = get_u64(&mut buf)?;
        let op = codec::get_compact_op(&mut buf)?;
        state.mark_destroyed.insert(seq, (object_id, op));
        state.destroyed_by_id.insert(object_id, seq);
    }
    state.next_destroyed_seq = get_u64(&mut buf)?;

    for _ in 0..get_u64(&mut buf)? {
        let key = get_str(&mut buf)?;
        let value = get_bytes(&mut buf)?;
        state.kv_bytes += (key.len() + value.len()) as u64;
        state.kv.insert(key, value);
    }

    for _ in 0..get_u64(&mut buf)? {
        let stream_id = get_u64(&mut buf)?;
        let from_node = get_i32(&mut buf)?;
        let to_node = get_i32(&mut buf)?;
        state
            .pending_transfers
            .insert(stream_id, PendingTransfer { from_node, to_node });
    }

    state.catalog = get_catalog(&mut buf)?;
    codec::ensure_drained(buf)?;

    Ok(state)
}

fn put_catalog(buf: &mut BytesMut, catalog: &Catalog) {
    buf.put_u64_le(catalog.next_table_id);
    buf.put_u64_le(catalog.next_partition_id);

    buf.put_u64_le(catalog.databases.len() as u64);
    for (name, row) in catalog.databases.iter() {
        codec::put_str(buf, name);
        codec::put_opt_str(buf, row.comment.as_deref());
        codec::put_str_map(buf, &row.custom);
        buf.put_i64_le(row.created_ms);
        buf.put_i64_le(row.modified_ms);
    }

    buf.put_u64_le(catalog.tables.len() as u64);
    for (path, row) in catalog.tables.iter() {
        codec::put_path(buf, path);
        buf.put_u64_le(row.table_id.0);
        codec::put_json(buf, &row.descriptor);
        buf.put_u32_le(row.schemas.len() as u32);
        for schema in &row.schemas {
            codec::put_json(buf, schema);
        }
        buf.put_i64_le(row.created_ms);
        buf.put_i64_le(row.modified_ms);
    }

    buf.put_u64_le(catalog.partitions.len() as u64);
    for (_, row) in catalog.partitions.iter() {
        buf.put_u64_le(row.partition_id.0);
        buf.put_u64_le(row.table_id.0);
        codec::put_str(buf, row.name.as_str());
        buf.put_i64_le(row.created_ms);
    }

    buf.put_u64_le(catalog.buckets.len() as u64);
    for (bucket, row) in catalog.buckets.iter() {
        codec::put_bucket(buf, bucket);
        buf.put_u64_le(row.stream_id);
        buf.put_i32_le(row.leader);
        buf.put_i32_le(row.leader_epoch);
        buf.put_i32_le(row.coordinator_epoch);
    }

    buf.put_u64_le(catalog.kv_snapshots.len() as u64);
    for ((bucket, _), row) in catalog.kv_snapshots.iter() {
        codec::put_bucket(buf, bucket);
        codec::put_kv_snapshot(buf, row);
    }

    buf.put_u64_le(catalog.lake.len() as u64);
    for (table_id, row) in catalog.lake.iter() {
        buf.put_u64_le(table_id.0);
        codec::put_lake_snapshot(buf, row);
    }

    buf.put_u64_le(catalog.counters.len() as u64);
    for (counter, next) in catalog.counters.iter() {
        codec::put_counter(buf, counter);
        buf.put_u64_le(*next);
    }

    match &catalog.coordinator {
        Some(row) => {
            buf.put_u8(1);
            buf.put_i32_le(row.node_id);
            buf.put_i32_le(row.epoch);
            codec::put_str(buf, &row.address);
        }
        None => buf.put_u8(0),
    }

    put_offsets(buf, &catalog.producer_offsets);
    put_offsets(buf, &catalog.group_offsets);
}

fn put_offsets(buf: &mut BytesMut, rows: &OrdMap<String, OffsetsRow>) {
    buf.put_u64_le(rows.len() as u64);
    for (id, row) in rows.iter() {
        codec::put_str(buf, id);
        codec::put_offsets_row(buf, row);
    }
}

fn get_offsets(
    buf: &mut &[u8],
    rows: &mut OrdMap<String, OffsetsRow>,
    by_expiry: &mut OrdMap<(i64, String), ()>,
) -> Result<(), CodecError> {
    for _ in 0..get_u64(buf)? {
        let id = get_str(buf)?;
        let row = codec::get_offsets_row(buf)?;
        by_expiry.insert((row.expires_ms, id.clone()), ());
        rows.insert(id, row);
    }

    Ok(())
}

fn get_catalog(buf: &mut &[u8]) -> Result<Catalog, CodecError> {
    let mut catalog = Catalog {
        next_table_id: get_u64(buf)?,
        next_partition_id: get_u64(buf)?,
        ..Catalog::default()
    };

    for _ in 0..get_u64(buf)? {
        let name = get_str(buf)?;
        let row = DatabaseRow {
            comment: codec::get_opt_str(buf)?,
            custom: get_str_map(buf)?,
            created_ms: get_i64(buf)?,
            modified_ms: get_i64(buf)?,
        };
        catalog.databases.insert(name, row);
    }

    for _ in 0..get_u64(buf)? {
        let path = codec::get_path(buf)?;
        let table_id = Id(get_u64(buf)?);
        let descriptor = codec::get_json(buf)?;
        let mut schemas = Vec::new();
        for _ in 0..get_u32(buf)? {
            schemas.push(codec::get_json(buf)?);
        }
        let row = TableRow {
            table_id,
            descriptor,
            schemas,
            created_ms: get_i64(buf)?,
            modified_ms: get_i64(buf)?,
        };
        catalog.table_paths.insert(table_id, path.clone());
        catalog.tables.insert(path, row);
    }

    for _ in 0..get_u64(buf)? {
        let row = PartitionRow {
            partition_id: PartitionId(get_u64(buf)?),
            table_id: Id(get_u64(buf)?),
            name: codec::get_partition_name(buf)?,
            created_ms: get_i64(buf)?,
        };
        let key = (row.table_id, row.name.clone());
        catalog
            .partition_names
            .insert(row.partition_id, key.clone());
        catalog.partitions.insert(key, row);
    }

    for _ in 0..get_u64(buf)? {
        let bucket = codec::get_bucket(buf)?;
        let row = BucketRow {
            stream_id: get_u64(buf)?,
            leader: get_i32(buf)?,
            leader_epoch: get_i32(buf)?,
            coordinator_epoch: get_i32(buf)?,
        };
        catalog.buckets.insert(bucket, row);
    }

    for _ in 0..get_u64(buf)? {
        let bucket = codec::get_bucket(buf)?;
        let row = codec::get_kv_snapshot(buf)?;
        catalog.kv_snapshots.insert((bucket, row.snapshot_id), row);
    }

    for _ in 0..get_u64(buf)? {
        let table_id = Id(get_u64(buf)?);
        let row = codec::get_lake_snapshot(buf)?;
        catalog.lake.insert(table_id, row);
    }

    for _ in 0..get_u64(buf)? {
        let counter = codec::get_counter(buf)?;
        let next = get_u64(buf)?;
        catalog.counters.insert(counter, next);
    }

    catalog.coordinator = match get_u8(buf)? {
        0 => None,
        1 => Some(CoordinatorRow {
            node_id: get_i32(buf)?,
            epoch: get_i32(buf)?,
            address: get_str(buf)?,
        }),
        other => return Err(CodecError::Corrupt(format!("coordinator tag {other}"))),
    };

    get_offsets(
        buf,
        &mut catalog.producer_offsets,
        &mut catalog.producer_offsets_by_expiry,
    )?;
    get_offsets(
        buf,
        &mut catalog.group_offsets,
        &mut catalog.group_offsets_by_expiry,
    )?;

    Ok(catalog)
}

fn put_object(buf: &mut BytesMut, object: &S3ObjectMetadata) {
    buf.put_u64_le(object.object_id);
    buf.put_u8(match object.object_type {
        S3ObjectType::StreamSet => 0,
        S3ObjectType::Stream => 1,
    });
    buf.put_u64_le(object.object_size);
    buf.put_u32_le(object.attributes.0);
    buf.put_i64_le(object.committed_timestamp_ms);
    buf.put_i64_le(object.data_timestamp_ms);
    put_seq(buf, &object.offset_ranges, |buf, range| {
        buf.put_u64_le(range.stream_id);
        buf.put_u64_le(range.start_offset);
        buf.put_u64_le(range.end_offset);
    });
}

fn get_object(buf: &mut &[u8]) -> Result<S3ObjectMetadata, CodecError> {
    let object_id = get_u64(buf)?;
    let object_type = match get_u8(buf)? {
        0 => S3ObjectType::StreamSet,
        1 => S3ObjectType::Stream,
        other => return Err(CodecError::Corrupt(format!("object type {other}"))),
    };
    let object_size = get_u64(buf)?;
    let attributes = ObjectAttributes(get_u32(buf)?);
    let committed_timestamp_ms = get_i64(buf)?;
    let data_timestamp_ms = get_i64(buf)?;
    let offset_ranges = get_seq(buf, |buf| {
        Ok::<_, CodecError>(StreamOffsetRange {
            stream_id: get_u64(buf)?,
            start_offset: get_u64(buf)?,
            end_offset: get_u64(buf)?,
        })
    })?;

    Ok(S3ObjectMetadata {
        object_id,
        object_type,
        offset_ranges,
        object_size,
        attributes,
        committed_timestamp_ms,
        data_timestamp_ms,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use s3stream::{
        CommitStreamSetObjectRequest, CompactOperations, CompactStreamObjectRequest,
        ObjectStreamRange,
    };

    use super::*;
    use crate::apply::apply;
    use crate::command::Command;

    fn rich_state() -> State {
        let mut state = State::new();
        for (node_id, node_epoch, addr, kafka) in [
            (1, 10i64, "http://n1:9090", Some("n1:9092")),
            (2, 20, "", None),
        ] {
            apply(
                &mut state,
                &Command::RegisterNode {
                    node_id,
                    node_epoch,
                    http_address: addr.into(),
                    slots: 1,
                    protocol_addresses: kafka
                        .map(|address| BTreeMap::from([("kafka".to_owned(), address.to_owned())]))
                        .unwrap_or_default(),
                },
            )
            .unwrap();
        }
        apply(
            &mut state,
            &Command::AllocateProducerIds {
                node_id: 1,
                node_epoch: 10,
                count: 5,
            },
        )
        .unwrap();
        for _ in 0..3 {
            apply(
                &mut state,
                &Command::CreateStream {
                    node_id: 1,
                    node_epoch: 10,
                },
            )
            .unwrap();
        }
        apply(
            &mut state,
            &Command::OpenStream {
                node_id: 1,
                node_epoch: 10,
                stream_id: 0,
                epoch: 1,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &Command::PrepareObject {
                node_id: 1,
                node_epoch: 10,
                count: 4,
                ttl_ms: 60_000,
                now_ms: 7,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &Command::CommitStreamSetObject {
                node_id: 1,
                node_epoch: 10,
                request: CommitStreamSetObjectRequest {
                    object_id: 0,
                    object_size: 64,
                    attributes: 5,
                    stream_ranges: vec![ObjectStreamRange {
                        stream_id: 0,
                        epoch: 1,
                        start_offset: 0,
                        end_offset: 8,
                        size: 64,
                    }],
                    stream_objects: vec![],
                    compacted_object_ids: vec![],
                },
                now_ms: 11,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &Command::CompactStreamObject {
                node_id: 1,
                node_epoch: 10,
                request: CompactStreamObjectRequest {
                    object_id: 1,
                    object_size: 32,
                    stream_id: 0,
                    stream_epoch: 1,
                    start_offset: 0,
                    end_offset: 4,
                    source_object_ids: vec![100, 101, 102],
                    operations: vec![
                        CompactOperations::Delete,
                        CompactOperations::KeepData,
                        CompactOperations::DeepDelete,
                    ],
                    attributes: 0,
                },
                now_ms: 13,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &Command::CleanDestroyedObjects {
                object_ids: vec![100],
            },
        )
        .unwrap();
        for (key, value) in [("meta/a", "one"), ("meta/b", "two")] {
            apply(
                &mut state,
                &Command::PutKv {
                    key: key.into(),
                    value: Bytes::from(value),
                },
            )
            .unwrap();
        }
        apply(
            &mut state,
            &Command::TransferStream {
                stream_id: 0,
                from_node: 1,
                to_node: 2,
            },
        )
        .unwrap();
        state
    }

    #[test]
    fn roundtrip_is_identity() {
        let state = rich_state();
        let encoded = encode(&state);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, state, "decode(encode(s)) == s, indexes included");
        assert_eq!(encode(&decoded), encoded, "re-encode is byte-identical");
    }

    #[test]
    fn empty_state_roundtrips() {
        let state = State::new();
        assert_eq!(decode(&encode(&state)).unwrap(), state);
    }

    #[test]
    fn encoding_is_deterministic() {
        assert_eq!(encode(&rich_state()), encode(&rich_state()));
    }

    #[test]
    fn rejects_unknown_version() {
        let mut bytes = encode(&rich_state()).to_vec();
        bytes[0] = SNAPSHOT_VERSION + 1;
        assert!(
            matches!(decode(&bytes), Err(CodecError::UnsupportedVersion(v)) if v == SNAPSHOT_VERSION + 1)
        );
    }

    #[test]
    fn rejects_truncation_and_trailing_bytes() {
        let bytes = encode(&rich_state());
        for len in 0..bytes.len() {
            assert!(
                decode(&bytes[..len]).is_err(),
                "prefix of {len} bytes must be rejected"
            );
        }
        let mut extended = bytes.to_vec();
        extended.push(0);
        assert!(matches!(decode(&extended), Err(CodecError::Corrupt(_))));
    }
}
