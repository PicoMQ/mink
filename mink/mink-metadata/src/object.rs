//! Handlers for preparing, committing, compacting, expiring and destroying stream-set and stream objects.

use s3stream::{
    CommitStreamSetObjectRequest, CompactOperations, CompactStreamObjectRequest, NOOP_OBJECT_ID,
    ObjectAttributes, S3ObjectMetadata, S3ObjectType, StreamOffsetRange,
};

use crate::command::Outcome;
use crate::error::Error;
use crate::state::{State, StreamObjectRow, StreamOffsetKey, StreamRow, StreamSetObjectRow};
use crate::stream::{node_epoch_check, require_stream};

pub(crate) fn prepare_object(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    count: u32,
    ttl_ms: i64,
    now_ms: i64,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    if count == 0 {
        return Err(Error::Unexpected {
            message: "prepare count must be positive".into(),
        });
    }

    let first = state.next_object_id;
    state.next_object_id += count as u64;
    let deadline = now_ms + ttl_ms.max(0);
    for id in first..first + count as u64 {
        state.prepared.insert(id, deadline);
        state.prepared_by_deadline.insert((deadline, id), ());
    }

    Ok(Outcome::Id(first))
}

fn commit_prepared(state: &mut State, object_id: u64) {
    if let Some(deadline) = state.prepared.remove(&object_id) {
        state.prepared_by_deadline.remove(&(deadline, object_id));
    }
}

fn check_within(stream: &StreamRow, start_offset: u64, end_offset: u64) -> Result<(), Error> {
    let stream_id = stream.stream_id;
    if stream.end_offset < end_offset {
        return Err(Error::Unexpected {
            message: format!(
                "stream {stream_id} end offset {} is lesser than request {end_offset}",
                stream.end_offset
            ),
        });
    }
    if stream.start_offset > start_offset {
        return Err(Error::Unexpected {
            message: format!(
                "stream {stream_id} start offset {} is greater than request {start_offset}",
                stream.start_offset
            ),
        });
    }

    Ok(())
}

fn extend_end(
    state: &mut State,
    stream_id: u64,
    start_offset: u64,
    end_offset: u64,
) -> Result<(), Error> {
    let stream = require_stream(state, stream_id)?;
    if stream.end_offset != start_offset {
        return Err(Error::Unexpected {
            message: format!(
                "stream {stream_id} end offset {} is not equal to start offset of request {start_offset}",
                stream.end_offset
            ),
        });
    }

    state.streams.insert(
        stream_id,
        StreamRow {
            end_offset,
            ..stream
        },
    );

    Ok(())
}

pub(crate) fn mark_destroyed(state: &mut State, object_id: u64, op: CompactOperations) {
    if let Some(seq) = state.destroyed_by_id.get(&object_id).copied() {
        state.mark_destroyed.insert(seq, (object_id, op));
        return;
    }

    let seq = state.next_destroyed_seq;
    state.next_destroyed_seq += 1;
    state.mark_destroyed.insert(seq, (object_id, op));
    state.destroyed_by_id.insert(object_id, seq);
}

pub(crate) fn mark_destroy_objects(
    state: &mut State,
    ids: &[u64],
    ops: &[CompactOperations],
) -> Result<(), Error> {
    if ids.is_empty() {
        return Ok(());
    }
    if ops.is_empty() {
        for &id in ids {
            mark_destroyed(state, id, CompactOperations::Delete);
        }
        return Ok(());
    }
    if ops.len() != ids.len() {
        return Err(Error::Unexpected {
            message: format!(
                "mark destroy ids size {} does not match operations size {}",
                ids.len(),
                ops.len()
            ),
        });
    }
    for (&id, &op) in ids.iter().zip(ops.iter()) {
        mark_destroyed(state, id, op);
    }

    Ok(())
}

fn stream_object_committed(state: &State, stream_id: u64, object_id: u64) -> bool {
    matches!(state.stream_object_ids.get(&object_id), Some(key) if key.0 == stream_id)
}

fn check_object_id_free(
    state: &State,
    object_id: u64,
    new_key: Option<StreamOffsetKey>,
) -> Result<(), Error> {
    if state.stream_set_objects.contains_key(&object_id) {
        return Err(Error::Unexpected {
            message: format!("object {object_id} is already a committed stream-set object"),
        });
    }
    if let Some(existing) = state.stream_object_ids.get(&object_id)
        && new_key != Some(*existing)
    {
        return Err(Error::Unexpected {
            message: format!(
                "object {object_id} is already committed as stream object {existing:?}"
            ),
        });
    }

    Ok(())
}

fn redundant_commit_check(
    state: &State,
    request: &CommitStreamSetObjectRequest,
) -> Result<(), Error> {
    if request.object_id != NOOP_OBJECT_ID {
        if state.stream_set_objects.contains_key(&request.object_id) {
            return Err(Error::Redundant {
                message: format!("object {} already committed", request.object_id),
            });
        }
        return Ok(());
    }
    if request.stream_objects.is_empty() {
        return Ok(());
    }

    let all_committed = request
        .stream_objects
        .iter()
        .all(|so| stream_object_committed(state, so.stream_id, so.object_id));
    if all_committed {
        return Err(Error::Redundant {
            message: "all stream objects in commit already committed".into(),
        });
    }

    Ok(())
}

pub(crate) fn commit_stream_set_object(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    request: &CommitStreamSetObjectRequest,
    now_ms: i64,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    redundant_commit_check(state, request)?;

    let compact = !request.compacted_object_ids.is_empty();
    let advance = |state: &mut State, range: &StreamOffsetRange| {
        if compact {
            let stream = require_stream(state, range.stream_id)?;
            check_within(&stream, range.start_offset, range.end_offset)
        } else {
            extend_end(state, range.stream_id, range.start_offset, range.end_offset)
        }
    };

    let mut data_time_ms = now_ms;
    if compact {
        for &id in &request.compacted_object_ids {
            let owned = state
                .stream_set_objects
                .get(&id)
                .ok_or_else(|| Error::Unexpected {
                    message: format!("compacted stream-set object {id} not found"),
                })?;
            data_time_ms = data_time_ms.min(owned.object.data_timestamp_ms);
        }
        for &id in &request.compacted_object_ids {
            remove_stream_set_object(state, id);
            mark_destroyed(state, id, CompactOperations::Delete);
        }
    }

    if request.object_id != NOOP_OBJECT_ID {
        check_object_id_free(state, request.object_id, None)?;
        commit_prepared(state, request.object_id);

        let ranges: Vec<StreamOffsetRange> = request
            .stream_ranges
            .iter()
            .map(|r| StreamOffsetRange {
                stream_id: r.stream_id,
                start_offset: r.start_offset,
                end_offset: r.end_offset,
            })
            .collect();
        for range in &ranges {
            advance(state, range)?;
            state.sso_ranges.insert(
                (range.stream_id, range.start_offset, request.object_id),
                range.end_offset,
            );
        }

        let object = S3ObjectMetadata {
            object_id: request.object_id,
            object_type: S3ObjectType::StreamSet,
            offset_ranges: ranges,
            object_size: request.object_size,
            attributes: ObjectAttributes(request.attributes),
            committed_timestamp_ms: now_ms,
            data_timestamp_ms: data_time_ms,
        };
        state.sso_by_node.insert((node_id, request.object_id), ());
        state
            .stream_set_objects
            .insert(request.object_id, StreamSetObjectRow { node_id, object });
    }

    for so in &request.stream_objects {
        let range = StreamOffsetRange {
            stream_id: so.stream_id,
            start_offset: so.start_offset,
            end_offset: so.end_offset,
        };
        check_object_id_free(
            state,
            so.object_id,
            Some((so.stream_id, so.start_offset, so.object_id)),
        )?;
        commit_prepared(state, so.object_id);
        advance(state, &range)?;
        insert_stream_object(
            state,
            range,
            so.object_id,
            so.object_size,
            so.attributes,
            data_time_ms,
            now_ms,
        );
    }

    Ok(Outcome::Unit)
}

pub(crate) fn compact_stream_object(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    request: &CompactStreamObjectRequest,
    now_ms: i64,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream_id = request.stream_id;

    if request.object_id != NOOP_OBJECT_ID
        && stream_object_committed(state, stream_id, request.object_id)
    {
        return Err(Error::Redundant {
            message: format!(
                "compact object {} already committed for stream {stream_id}",
                request.object_id
            ),
        });
    }

    let stream = require_stream(state, stream_id)?;
    if stream.epoch != request.stream_epoch as i64 {
        return Err(Error::ExpiredEpoch {
            stream_id,
            epoch: request.stream_epoch as i64,
            message: format!(
                "stream {stream_id} epoch {} is not equal to request {}",
                stream.epoch, request.stream_epoch
            ),
        });
    }
    check_within(&stream, request.start_offset, request.end_offset)?;

    if request.object_id != NOOP_OBJECT_ID {
        check_object_id_free(
            state,
            request.object_id,
            Some((stream_id, request.start_offset, request.object_id)),
        )?;
    }
    commit_prepared(state, request.object_id);
    if request.object_id != NOOP_OBJECT_ID {
        insert_stream_object(
            state,
            StreamOffsetRange {
                stream_id,
                start_offset: request.start_offset,
                end_offset: request.end_offset,
            },
            request.object_id,
            request.object_size,
            request.attributes,
            now_ms,
            now_ms,
        );
    }

    for &source_id in &request.source_object_ids {
        if let Some(key) = state.stream_object_ids.get(&source_id).copied()
            && key.0 == stream_id
        {
            state.stream_objects.remove(&key);
            state.stream_object_ids.remove(&source_id);
        }
    }
    mark_destroy_objects(state, &request.source_object_ids, &request.operations)?;

    Ok(Outcome::Unit)
}

fn insert_stream_object(
    state: &mut State,
    range: StreamOffsetRange,
    object_id: u64,
    object_size: u64,
    attributes: u32,
    data_time_ms: i64,
    now_ms: i64,
) {
    let key = (range.stream_id, range.start_offset, object_id);
    state.stream_objects.insert(
        key,
        StreamObjectRow {
            object: S3ObjectMetadata {
                object_id,
                object_type: S3ObjectType::Stream,
                offset_ranges: vec![range],
                object_size,
                attributes: ObjectAttributes(attributes),
                committed_timestamp_ms: now_ms,
                data_timestamp_ms: data_time_ms,
            },
        },
    );
    state.stream_object_ids.insert(object_id, key);
}

pub(crate) fn remove_stream_set_object(state: &mut State, object_id: u64) {
    if let Some(row) = state.stream_set_objects.remove(&object_id) {
        for range in &row.object.offset_ranges {
            state
                .sso_ranges
                .remove(&(range.stream_id, range.start_offset, object_id));
        }
        state.sso_by_node.remove(&(row.node_id, object_id));
    }
}

pub(crate) fn expire_prepared_objects(state: &mut State, now_ms: i64) -> Result<Outcome, Error> {
    let expired: Vec<(i64, u64)> = state
        .prepared_by_deadline
        .range(..=(now_ms, u64::MAX))
        .map(|(key, _)| *key)
        .collect();
    for (deadline, object_id) in &expired {
        state.prepared_by_deadline.remove(&(*deadline, *object_id));
        state.prepared.remove(object_id);
    }

    Ok(Outcome::Count(expired.len() as u64))
}

pub(crate) fn clean_destroyed_objects(
    state: &mut State,
    object_ids: &[u64],
) -> Result<Outcome, Error> {
    for id in object_ids {
        if let Some(seq) = state.destroyed_by_id.remove(id) {
            state.mark_destroyed.remove(&seq);
        }
    }

    Ok(Outcome::Unit)
}
