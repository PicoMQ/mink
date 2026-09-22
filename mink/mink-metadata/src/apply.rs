//! Applies a command to a copy of the state and commits the copy only if it succeeds, dispatching to the
//! catalog, stream, object and key-value handlers.

use bytes::Bytes;

use crate::catalog;
use crate::command::{Command, Outcome};
use crate::error::Error;
use crate::object::{
    clean_destroyed_objects, commit_stream_set_object, compact_stream_object,
    expire_prepared_objects, prepare_object,
};
use crate::state::State;
use crate::stream::{
    allocate_producer_ids, close_stream, complete_transfer, create_stream, create_streams,
    delete_stream, open_stream, place_stream, register_node, transfer_stream, trim_stream,
};

pub fn apply(state: &mut State, command: &Command) -> Result<Outcome, Error> {
    let mut next = state.clone();
    let result = apply_inner(&mut next, command)?;
    *state = next;

    Ok(result)
}

fn apply_inner(state: &mut State, command: &Command) -> Result<Outcome, Error> {
    match command {
        Command::CreateDatabase {
            name,
            comment,
            custom,
            now_ms,
        } => catalog::create_database(state, name, comment.as_deref(), custom, *now_ms),
        Command::DropDatabase { name } => catalog::drop_database(state, name),
        Command::CreateTable {
            path,
            descriptor,
            leaders,
            coordinator_epoch,
            now_ms,
        } => catalog::create_table(
            state,
            path,
            descriptor,
            leaders,
            *coordinator_epoch,
            *now_ms,
        ),
        Command::DropTable { path } => catalog::drop_table(state, path),
        Command::AlterTable {
            path,
            descriptor,
            now_ms,
        } => catalog::alter_table(state, path, descriptor, *now_ms),
        Command::CreatePartition {
            path,
            name,
            leaders,
            coordinator_epoch,
            now_ms,
        } => catalog::create_partition(state, path, name, leaders, *coordinator_epoch, *now_ms),
        Command::DropPartition { path, name } => catalog::drop_partition(state, path, name),
        Command::LeadBucket {
            bucket,
            node_id,
            coordinator_epoch,
        } => catalog::lead_bucket(state, *bucket, *node_id, *coordinator_epoch),
        Command::CommitKvSnapshot {
            bucket,
            snapshot,
            leader_epoch,
            coordinator_epoch,
        } => {
            catalog::commit_kv_snapshot(state, *bucket, snapshot, *leader_epoch, *coordinator_epoch)
        }
        Command::DropKvSnapshot {
            bucket,
            snapshot_id,
        } => catalog::drop_kv_snapshot(state, *bucket, *snapshot_id),
        Command::CommitLakeSnapshot { table_id, snapshot } => {
            catalog::commit_lake_snapshot(state, *table_id, snapshot)
        }
        Command::Allocate { counter, count } => catalog::allocate(state, *counter, *count),
        Command::RegisterCoordinator {
            node_id,
            epoch,
            address,
        } => catalog::register_coordinator(state, *node_id, *epoch, address),
        Command::RegisterProducerOffsets {
            producer_id,
            offsets,
            expires_ms,
            now_ms,
        } => catalog::register_producer_offsets(state, producer_id, offsets, *expires_ms, *now_ms),
        Command::DeleteProducerOffsets { producer_id } => {
            catalog::delete_producer_offsets(state, producer_id)
        }
        Command::ExpireProducerOffsets { now_ms } => {
            catalog::expire_producer_offsets(state, *now_ms)
        }
        Command::CommitGroupOffsets {
            group,
            offsets,
            expires_ms,
        } => catalog::commit_group_offsets(state, group, offsets, *expires_ms),
        Command::DeleteGroupOffsets { group } => catalog::delete_group_offsets(state, group),
        Command::ExpireGroupOffsets { now_ms } => catalog::expire_group_offsets(state, *now_ms),
        Command::RegisterNode {
            node_id,
            node_epoch,
            http_address,
            slots,
            protocol_addresses,
        } => register_node(
            state,
            *node_id,
            *node_epoch,
            http_address,
            *slots,
            protocol_addresses,
        ),
        Command::PlaceStream { stream_id } => place_stream(state, *stream_id),
        Command::CreateStream {
            node_id,
            node_epoch,
        } => create_stream(state, *node_id, *node_epoch),
        Command::OpenStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        } => open_stream(state, *node_id, *node_epoch, *stream_id, *epoch),
        Command::TrimStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
            new_start_offset,
        } => trim_stream(
            state,
            *node_id,
            *node_epoch,
            *stream_id,
            *epoch,
            *new_start_offset,
        ),
        Command::CloseStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        } => close_stream(state, *node_id, *node_epoch, *stream_id, *epoch),
        Command::DeleteStream {
            node_id,
            node_epoch,
            stream_id,
            epoch,
        } => delete_stream(state, *node_id, *node_epoch, *stream_id, *epoch),
        Command::PrepareObject {
            node_id,
            node_epoch,
            count,
            ttl_ms,
            now_ms,
        } => prepare_object(state, *node_id, *node_epoch, *count, *ttl_ms, *now_ms),
        Command::CommitStreamSetObject {
            node_id,
            node_epoch,
            request,
            now_ms,
        } => commit_stream_set_object(state, *node_id, *node_epoch, request, *now_ms),
        Command::CompactStreamObject {
            node_id,
            node_epoch,
            request,
            now_ms,
        } => compact_stream_object(state, *node_id, *node_epoch, request, *now_ms),
        Command::ExpirePreparedObjects { now_ms } => expire_prepared_objects(state, *now_ms),
        Command::CleanDestroyedObjects { object_ids } => clean_destroyed_objects(state, object_ids),
        Command::PutKv { key, value } => put_kv(state, key, value),
        Command::PutKvIfAbsent { key, value } => put_kv_if_absent(state, key, value),
        Command::DeleteKv { key } => delete_kv(state, key),
        Command::DeleteKvIfMatches { key, expected } => delete_kv_if_matches(state, key, expected),
        Command::TransferStream {
            stream_id,
            from_node,
            to_node,
        } => transfer_stream(state, *stream_id, *from_node, *to_node),
        Command::CompleteTransfer { stream_id, epoch } => {
            complete_transfer(state, *stream_id, *epoch)
        }
        Command::CreateStreams {
            node_id,
            node_epoch,
            count,
        } => create_streams(state, *node_id, *node_epoch, *count),
        Command::AllocateProducerIds {
            node_id,
            node_epoch,
            count,
        } => allocate_producer_ids(state, *node_id, *node_epoch, *count),
    }
}

fn put_kv(state: &mut State, key: &str, value: &Bytes) -> Result<Outcome, Error> {
    if let Some(old) = state.kv.insert(key.to_owned(), value.clone()) {
        state.kv_bytes -= (key.len() + old.len()) as u64;
    }
    state.kv_bytes += (key.len() + value.len()) as u64;
    Ok(Outcome::Value(Some(value.clone())))
}

fn put_kv_if_absent(state: &mut State, key: &str, value: &Bytes) -> Result<Outcome, Error> {
    if let Some(existing) = state.kv.get(key) {
        return Ok(Outcome::Value(Some(existing.clone())));
    }
    state.kv.insert(key.to_owned(), value.clone());
    state.kv_bytes += (key.len() + value.len()) as u64;
    Ok(Outcome::Value(Some(value.clone())))
}

fn delete_kv(state: &mut State, key: &str) -> Result<Outcome, Error> {
    let removed = state.kv.remove(key);
    if let Some(old) = &removed {
        state.kv_bytes -= (key.len() + old.len()) as u64;
    }
    Ok(Outcome::Value(removed))
}

fn delete_kv_if_matches(state: &mut State, key: &str, expected: &Bytes) -> Result<Outcome, Error> {
    match state.kv.get(key) {
        Some(current) if current == expected => {
            let removed = state.kv.remove(key);
            if let Some(old) = &removed {
                state.kv_bytes -= (key.len() + old.len()) as u64;
            }
            Ok(Outcome::Value(removed))
        }
        _ => Err(Error::Redundant {
            message: format!("kv key {key} missing or value mismatch"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use s3stream::{
        CommitStreamSetObjectRequest, CompactOperations, CompactStreamObjectRequest,
        ObjectStreamRange, StreamState,
    };

    use super::*;
    use crate::object::mark_destroy_objects;
    use crate::stream::MAX_BATCH;

    const NODE_1: i32 = 1;
    const NODE_2: i32 = 2;
    const EPOCH_1: i64 = 10;
    const EPOCH_2: i64 = 20;

    fn setup() -> State {
        let mut state = State::new();
        register(&mut state, NODE_1, EPOCH_1);
        register(&mut state, NODE_2, EPOCH_2);
        state
    }

    fn register(state: &mut State, node_id: i32, node_epoch: i64) {
        register_with_slots(state, node_id, node_epoch, 1);
    }

    fn register_with_slots(state: &mut State, node_id: i32, node_epoch: i64, slots: u32) {
        apply(
            state,
            &Command::RegisterNode {
                node_id,
                node_epoch,
                http_address: String::new(),
                slots,
                protocol_addresses: Default::default(),
            },
        )
        .unwrap();
    }

    fn place(state: &mut State, stream_id: u64) -> i32 {
        match apply(state, &Command::PlaceStream { stream_id }).unwrap() {
            Outcome::Id(node_id) => node_id as i32,
            other => panic!("unexpected result {other:?}"),
        }
    }

    fn create(state: &mut State, node_id: i32, node_epoch: i64) -> u64 {
        match apply(
            state,
            &Command::CreateStream {
                node_id,
                node_epoch,
            },
        )
        .unwrap()
        {
            Outcome::Id(id) => id,
            other => panic!("unexpected result {other:?}"),
        }
    }

    fn open(
        state: &mut State,
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    ) -> Result<Outcome, Error> {
        apply(
            state,
            &Command::OpenStream {
                node_id,
                node_epoch,
                stream_id,
                epoch,
            },
        )
    }

    fn close(
        state: &mut State,
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    ) -> Result<Outcome, Error> {
        apply(
            state,
            &Command::CloseStream {
                node_id,
                node_epoch,
                stream_id,
                epoch,
            },
        )
    }

    fn prepare(state: &mut State, count: u32, ttl_ms: i64, now_ms: i64) -> u64 {
        match apply(
            state,
            &Command::PrepareObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count,
                ttl_ms,
                now_ms,
            },
        )
        .unwrap()
        {
            Outcome::Id(id) => id,
            other => panic!("unexpected result {other:?}"),
        }
    }

    #[test]
    fn writes_require_registered_node_epoch() {
        let mut state = setup();
        let err = apply(
            &mut state,
            &Command::CreateStream {
                node_id: 3,
                node_epoch: 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
        let err = apply(
            &mut state,
            &Command::CreateStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
    }

    #[test]
    fn stale_node_epoch_fenced_after_re_registration() {
        let mut state = setup();
        register(&mut state, NODE_1, EPOCH_1 + 5);
        let err = apply(
            &mut state,
            &Command::CreateStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
        let err = apply(
            &mut state,
            &Command::RegisterNode {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                http_address: String::new(),
                slots: 1,
                protocol_addresses: Default::default(),
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
    }

    #[test]
    fn register_node_keeps_protocol_addresses_unless_replaced() {
        let addrs = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let mut state = State::new();
        apply(
            &mut state,
            &Command::RegisterNode {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                http_address: "http://n1:9090".into(),
                slots: 1,
                protocol_addresses: addrs(&[("kafka", "n1:9092")]),
            },
        )
        .unwrap();
        assert_eq!(
            state
                .nodes
                .get(&NODE_1)
                .unwrap()
                .protocol_addresses
                .get("kafka")
                .map(String::as_str),
            Some("n1:9092")
        );

        apply(
            &mut state,
            &Command::RegisterNode {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 1,
                http_address: String::new(),
                slots: 2,
                protocol_addresses: Default::default(),
            },
        )
        .unwrap();
        let node = state.nodes.get(&NODE_1).unwrap();
        assert_eq!(
            node.protocol_addresses.get("kafka").map(String::as_str),
            Some("n1:9092")
        );
        assert_eq!(node.http_address, "http://n1:9090");

        apply(
            &mut state,
            &Command::RegisterNode {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 2,
                http_address: String::new(),
                slots: 2,
                protocol_addresses: addrs(&[("kafka", "n1:19092")]),
            },
        )
        .unwrap();
        assert_eq!(
            state
                .nodes
                .get(&NODE_1)
                .unwrap()
                .protocol_addresses
                .get("kafka")
                .map(String::as_str),
            Some("n1:19092")
        );
    }

    #[test]
    fn allocate_producer_ids_advances_and_validates() {
        let mut state = setup();
        let allocate = |state: &mut State, count| {
            apply(
                state,
                &Command::AllocateProducerIds {
                    node_id: NODE_1,
                    node_epoch: EPOCH_1,
                    count,
                },
            )
        };
        assert_eq!(allocate(&mut state, 3).unwrap(), Outcome::Id(0));
        assert_eq!(allocate(&mut state, 1).unwrap(), Outcome::Id(3));
        assert_eq!(state.next_producer_id, 4);

        assert_eq!(allocate(&mut state, 0).unwrap_err().code(), 99);
        assert_eq!(allocate(&mut state, MAX_BATCH + 1).unwrap_err().code(), 99);
        let err = apply(
            &mut state,
            &Command::AllocateProducerIds {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 1,
                count: 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
        assert_eq!(state.next_producer_id, 4);
    }

    #[test]
    fn open_stream_lifecycle_and_fencing() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);

        let opened = open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let Outcome::Stream(meta) = opened else {
            panic!("expected stream")
        };
        assert_eq!(meta.state, StreamState::Opened);

        let retried = open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let Outcome::Stream(meta) = retried else {
            panic!("expected stream")
        };
        assert_eq!(meta.state, StreamState::Opened);

        assert_eq!(
            open(&mut state, NODE_2, EPOCH_2, stream_id, 1)
                .unwrap_err()
                .code(),
            3
        );
        assert_eq!(
            open(&mut state, NODE_2, EPOCH_2, stream_id, 0)
                .unwrap_err()
                .code(),
            3
        );
        assert_eq!(
            open(&mut state, NODE_2, EPOCH_2, stream_id, 2)
                .unwrap_err()
                .code(),
            2
        );

        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let reopened = open(&mut state, NODE_2, EPOCH_2, stream_id, 2).unwrap();
        let Outcome::Stream(meta) = reopened else {
            panic!("expected stream")
        };
        assert_eq!(meta.node_id, NODE_2);
    }

    #[test]
    fn close_stream_is_idempotent() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        assert_eq!(
            state.streams.get(&stream_id).unwrap().state,
            StreamState::Closed
        );
        assert!(state.opening_by_node.get(&(NODE_1, stream_id)).is_none());
    }

    #[test]
    fn delete_stream_idempotent_and_marks_objects_destroyed() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let object_id = prepare(&mut state, 1, 60_000, 0);
        apply(
            &mut state,
            &Command::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: CompactStreamObjectRequest {
                    object_id,
                    object_size: 10,
                    stream_id,
                    stream_epoch: 1,
                    start_offset: 0,
                    end_offset: 0,
                    source_object_ids: vec![],
                    operations: vec![],
                    attributes: 0,
                },
                now_ms: 1,
            },
        )
        .unwrap();
        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        apply(
            &mut state,
            &Command::DeleteStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        apply(
            &mut state,
            &Command::DeleteStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        let destroyed: Vec<_> = state.mark_destroyed.values().cloned().collect();
        assert_eq!(destroyed, vec![(object_id, CompactOperations::Delete)]);
        assert!(state.stream_objects.is_empty());
        assert!(state.stream_object_ids.is_empty());
    }

    #[test]
    fn opening_by_node_index_filters_by_node() {
        let mut state = setup();
        let stream1 = create(&mut state, NODE_1, EPOCH_1);
        let stream2 = create(&mut state, NODE_2, EPOCH_2);
        open(&mut state, NODE_1, EPOCH_1, stream1, 1).unwrap();
        open(&mut state, NODE_2, EPOCH_2, stream2, 1).unwrap();

        let node1: Vec<u64> = state
            .opening_by_node
            .range((NODE_1, 0)..=(NODE_1, u64::MAX))
            .map(|(k, _)| k.1)
            .collect();
        assert_eq!(node1, vec![stream1]);
        let node2: Vec<u64> = state
            .opening_by_node
            .range((NODE_2, 0)..=(NODE_2, u64::MAX))
            .map(|(k, _)| k.1)
            .collect();
        assert_eq!(node2, vec![stream2]);
    }

    #[test]
    fn commit_advances_end_offset_and_is_redundant_on_retry() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let object_id = prepare(&mut state, 1, 60_000, 0);
        let request = CommitStreamSetObjectRequest {
            object_id,
            object_size: 64,
            attributes: 0,
            stream_ranges: vec![ObjectStreamRange {
                stream_id,
                epoch: 1,
                start_offset: 0,
                end_offset: 8,
                size: 64,
            }],
            stream_objects: vec![],
            compacted_object_ids: vec![],
        };
        apply(
            &mut state,
            &Command::CommitStreamSetObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: request.clone(),
                now_ms: 1,
            },
        )
        .unwrap();
        assert_eq!(state.streams.get(&stream_id).unwrap().end_offset, 8);
        assert!(state.prepared.is_empty(), "commit consumes the lease");

        let err = apply(
            &mut state,
            &Command::CommitStreamSetObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request,
                now_ms: 2,
            },
        )
        .unwrap_err();
        assert!(err.is_redundant());
        assert_eq!(state.streams.get(&stream_id).unwrap().end_offset, 8);
        assert_eq!(
            state.stream_set_objects.len() + state.stream_objects.len(),
            1
        );
        assert_eq!(state.sso_ranges.get(&(stream_id, 0, object_id)), Some(&8));
        assert!(state.sso_by_node.contains_key(&(NODE_1, object_id)));
    }

    #[test]
    fn compact_retry_is_redundant() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let object_id = prepare(&mut state, 1, 60_000, 0);
        let request = CompactStreamObjectRequest {
            object_id,
            object_size: 10,
            stream_id,
            stream_epoch: 1,
            start_offset: 0,
            end_offset: 0,
            source_object_ids: vec![],
            operations: vec![],
            attributes: 0,
        };
        apply(
            &mut state,
            &Command::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: request.clone(),
                now_ms: 1,
            },
        )
        .unwrap();
        let err = apply(
            &mut state,
            &Command::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request,
                now_ms: 2,
            },
        )
        .unwrap_err();
        assert!(err.is_redundant());
        assert_eq!(state.stream_objects.len(), 1);
    }

    #[test]
    fn mark_destroy_rejects_mismatched_sizes_atomically() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        let object_id = prepare(&mut state, 1, 60_000, 0);
        let before = state.clone();
        let err = apply(
            &mut state,
            &Command::CompactStreamObject {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                request: CompactStreamObjectRequest {
                    object_id,
                    object_size: 10,
                    stream_id,
                    stream_epoch: 1,
                    start_offset: 0,
                    end_offset: 0,
                    source_object_ids: vec![1, 2],
                    operations: vec![CompactOperations::Delete],
                    attributes: 0,
                },
                now_ms: 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 99);
        assert_eq!(state, before, "failed apply must not mutate state");
    }

    #[test]
    fn clean_destroyed_objects_preserves_fifo() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        mark_destroy_objects(
            &mut state,
            &[1, 2, 3],
            &[
                CompactOperations::KeepData,
                CompactOperations::Delete,
                CompactOperations::DeepDelete,
            ],
        )
        .unwrap();
        assert_eq!(state.mark_destroyed.len(), 3);
        apply(
            &mut state,
            &Command::CleanDestroyedObjects {
                object_ids: vec![1, 2],
            },
        )
        .unwrap();
        let remaining: Vec<_> = state.mark_destroyed.values().cloned().collect();
        assert_eq!(remaining, vec![(3, CompactOperations::DeepDelete)]);
    }

    #[test]
    fn expire_prepared_objects_removes_only_expired() {
        let mut state = setup();
        prepare(&mut state, 2, 100, 0);
        prepare(&mut state, 1, 10_000, 0);
        let result = apply(&mut state, &Command::ExpirePreparedObjects { now_ms: 200 }).unwrap();
        assert_eq!(result, Outcome::Count(2));
        assert_eq!(state.prepared.len(), 1);
        assert_eq!(state.prepared_by_deadline.len(), 1);
    }

    #[test]
    fn kv_put_get_delete_and_put_if_absent() {
        let mut state = State::new();
        let hello = Bytes::from_static(b"hello");
        let world = Bytes::from_static(b"world");

        let result = apply(
            &mut state,
            &Command::PutKv {
                key: "k".into(),
                value: hello.clone(),
            },
        )
        .unwrap();
        assert_eq!(result, Outcome::Value(Some(hello.clone())));
        assert_eq!(state.kv.get("k"), Some(&hello));

        let result = apply(
            &mut state,
            &Command::PutKvIfAbsent {
                key: "k".into(),
                value: world.clone(),
            },
        )
        .unwrap();
        assert_eq!(result, Outcome::Value(Some(hello.clone())));
        let result = apply(
            &mut state,
            &Command::PutKvIfAbsent {
                key: "k2".into(),
                value: world.clone(),
            },
        )
        .unwrap();
        assert_eq!(result, Outcome::Value(Some(world.clone())));

        let result = apply(&mut state, &Command::DeleteKv { key: "k".into() }).unwrap();
        assert_eq!(result, Outcome::Value(Some(hello)));
        assert!(state.kv.get("k").is_none());
        let result = apply(
            &mut state,
            &Command::DeleteKv {
                key: "missing".into(),
            },
        )
        .unwrap();
        assert_eq!(result, Outcome::Value(None));
    }

    #[test]
    fn kv_delete_if_matches_and_rejects_mismatch() {
        let mut state = State::new();
        let hello = Bytes::from_static(b"hello");
        let world = Bytes::from_static(b"world");
        apply(
            &mut state,
            &Command::PutKv {
                key: "k".into(),
                value: hello.clone(),
            },
        )
        .unwrap();

        assert!(
            apply(
                &mut state,
                &Command::DeleteKvIfMatches {
                    key: "k".into(),
                    expected: world.clone(),
                },
            )
            .unwrap_err()
            .is_redundant()
        );
        assert_eq!(state.kv.get("k"), Some(&hello));

        let result = apply(
            &mut state,
            &Command::DeleteKvIfMatches {
                key: "k".into(),
                expected: hello.clone(),
            },
        )
        .unwrap();
        assert_eq!(result, Outcome::Value(Some(hello)));
        assert!(state.kv.get("k").is_none());

        assert!(
            apply(
                &mut state,
                &Command::DeleteKvIfMatches {
                    key: "k".into(),
                    expected: world,
                },
            )
            .unwrap_err()
            .is_redundant()
        );
    }

    #[test]
    fn applying_same_operations_produces_identical_state() {
        fn run(seed: u64) -> State {
            let mut rng = seed;
            let mut next = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let mut state = State::new();
            register(&mut state, NODE_1, EPOCH_1);
            for i in 0..200 {
                let command = match next() % 4 {
                    0 => Command::CreateStream {
                        node_id: NODE_1,
                        node_epoch: EPOCH_1,
                    },
                    1 => Command::OpenStream {
                        node_id: NODE_1,
                        node_epoch: EPOCH_1,
                        stream_id: next() % 20,
                        epoch: (next() % 5) as i64,
                    },
                    2 => Command::CloseStream {
                        node_id: NODE_1,
                        node_epoch: EPOCH_1,
                        stream_id: next() % 20,
                        epoch: (next() % 5) as i64,
                    },
                    _ => Command::PrepareObject {
                        node_id: NODE_1,
                        node_epoch: EPOCH_1,
                        count: 1 + (next() % 3) as u32,
                        ttl_ms: 1000,
                        now_ms: i,
                    },
                };
                let _ = apply(&mut state, &command);
            }
            state
        }
        assert_eq!(run(424242), run(424242));
    }

    fn assert_indexes_consistent(state: &State) {
        let derived_opening: Vec<(i32, u64)> = state
            .streams
            .iter()
            .filter(|(_, row)| row.state == StreamState::Opened)
            .map(|(id, row)| (row.node_id, *id))
            .collect();
        let mut indexed_opening: Vec<(i32, u64)> = state.opening_by_node.keys().copied().collect();
        indexed_opening.sort_unstable();
        let mut derived_opening = derived_opening;
        derived_opening.sort_unstable();
        assert_eq!(indexed_opening, derived_opening, "opening_by_node");

        let derived_placed: Vec<(i32, u64)> = state
            .streams
            .iter()
            .filter(|(_, row)| {
                row.state == StreamState::Closed && row.epoch == -1 && row.node_id != -1
            })
            .map(|(id, row)| (row.node_id, *id))
            .collect();
        let mut indexed_placed: Vec<(i32, u64)> = state.placed_by_node.keys().copied().collect();
        indexed_placed.sort_unstable();
        let mut derived_placed = derived_placed;
        derived_placed.sort_unstable();
        assert_eq!(indexed_placed, derived_placed, "placed_by_node");

        assert_eq!(
            state.prepared.len(),
            state.prepared_by_deadline.len(),
            "prepared index size"
        );
        for (id, deadline) in state.prepared.iter() {
            assert!(
                state.prepared_by_deadline.contains_key(&(*deadline, *id)),
                "prepared index"
            );
        }

        for (key, _) in state.stream_objects.iter() {
            assert_eq!(
                state.stream_object_ids.get(&key.2),
                Some(key),
                "stream_object_ids"
            );
        }
        assert_eq!(state.stream_objects.len(), state.stream_object_ids.len());

        let mut derived_ranges = 0usize;
        for (object_id, row) in state.stream_set_objects.iter() {
            assert!(
                state.sso_by_node.contains_key(&(row.node_id, *object_id)),
                "sso_by_node"
            );
            for range in &row.object.offset_ranges {
                derived_ranges += 1;
                assert_eq!(
                    state
                        .sso_ranges
                        .get(&(range.stream_id, range.start_offset, *object_id)),
                    Some(&range.end_offset),
                    "sso_ranges"
                );
            }
        }
        assert_eq!(state.sso_ranges.len(), derived_ranges);
        assert_eq!(state.sso_by_node.len(), state.stream_set_objects.len());

        assert_eq!(state.mark_destroyed.len(), state.destroyed_by_id.len());
        for (seq, (id, _)) in state.mark_destroyed.iter() {
            assert_eq!(state.destroyed_by_id.get(id), Some(seq), "destroyed_by_id");
        }

        for (stream_id, _) in state.pending_transfers.iter() {
            assert!(
                state.streams.contains_key(stream_id),
                "pending transfer references a missing stream"
            );
        }
    }

    mod properties {
        use proptest::prelude::*;

        use super::*;

        fn arb_command() -> impl Strategy<Value = Command> {
            let node = prop_oneof![Just((NODE_1, EPOCH_1)), Just((NODE_2, EPOCH_2))];
            let stream_id = 0u64..8;
            let epoch = 0i64..4;
            prop_oneof![
                node.clone().prop_map(|(n, e)| Command::CreateStream {
                    node_id: n,
                    node_epoch: e
                }),
                stream_id
                    .clone()
                    .prop_map(|s| Command::PlaceStream { stream_id: s }),
                (node.clone(), stream_id.clone(), epoch.clone()).prop_map(|((n, ne), s, e)| {
                    Command::OpenStream {
                        node_id: n,
                        node_epoch: ne,
                        stream_id: s,
                        epoch: e,
                    }
                }),
                (node.clone(), stream_id.clone(), epoch.clone()).prop_map(|((n, ne), s, e)| {
                    Command::CloseStream {
                        node_id: n,
                        node_epoch: ne,
                        stream_id: s,
                        epoch: e,
                    }
                }),
                (node.clone(), stream_id.clone(), epoch.clone()).prop_map(|((n, ne), s, e)| {
                    Command::DeleteStream {
                        node_id: n,
                        node_epoch: ne,
                        stream_id: s,
                        epoch: e,
                    }
                }),
                (stream_id.clone(), 1i32..3, 1i32..3).prop_map(|(s, from, to)| {
                    Command::TransferStream {
                        stream_id: s,
                        from_node: from,
                        to_node: to,
                    }
                }),
                (stream_id.clone(), epoch.clone()).prop_map(|(s, e)| {
                    Command::CompleteTransfer {
                        stream_id: s,
                        epoch: e,
                    }
                }),
                (node.clone(), 1u32..4).prop_map(|((n, ne), c)| Command::CreateStreams {
                    node_id: n,
                    node_epoch: ne,
                    count: c,
                }),
                (node.clone(), 1u32..4).prop_map(|((n, ne), c)| {
                    Command::AllocateProducerIds {
                        node_id: n,
                        node_epoch: ne,
                        count: c,
                    }
                }),
                (node.clone(), 1u32..3, 0i64..100).prop_map(|((n, ne), c, now)| {
                    Command::PrepareObject {
                        node_id: n,
                        node_epoch: ne,
                        count: c,
                        ttl_ms: 50,
                        now_ms: now,
                    }
                }),
                (
                    node.clone(),
                    stream_id.clone(),
                    epoch.clone(),
                    0u64..8,
                    0u64..16
                )
                    .prop_map(|((n, ne), s, e, obj, end)| {
                        Command::CommitStreamSetObject {
                            node_id: n,
                            node_epoch: ne,
                            request: CommitStreamSetObjectRequest {
                                object_id: obj,
                                object_size: 64,
                                attributes: 0,
                                stream_ranges: vec![ObjectStreamRange {
                                    stream_id: s,
                                    epoch: e.max(0) as u64,
                                    start_offset: 0,
                                    end_offset: end,
                                    size: 64,
                                }],
                                stream_objects: vec![],
                                compacted_object_ids: vec![],
                            },
                            now_ms: 1,
                        }
                    }),
                (node.clone(), stream_id, epoch, 0u64..8).prop_map(|((n, ne), s, e, obj)| {
                    Command::CompactStreamObject {
                        node_id: n,
                        node_epoch: ne,
                        request: CompactStreamObjectRequest {
                            object_id: obj,
                            object_size: 10,
                            stream_id: s,
                            stream_epoch: e.max(0) as u64,
                            start_offset: 0,
                            end_offset: 0,
                            source_object_ids: vec![],
                            operations: vec![],
                            attributes: 0,
                        },
                        now_ms: 1,
                    }
                }),
                (0i64..200).prop_map(|now| Command::ExpirePreparedObjects { now_ms: now }),
                proptest::collection::vec(0u64..8, 0..3)
                    .prop_map(|ids| Command::CleanDestroyedObjects { object_ids: ids }),
                ("[a-c]{1,2}", proptest::collection::vec(any::<u8>(), 0..4)).prop_map(|(k, v)| {
                    Command::PutKv {
                        key: k,
                        value: Bytes::from(v),
                    }
                }),
                "[a-c]{1,2}".prop_map(|k| Command::DeleteKv { key: k }),
                ("[a-c]{1,2}", proptest::collection::vec(any::<u8>(), 0..4)).prop_map(|(k, v)| {
                    Command::DeleteKvIfMatches {
                        key: k,
                        expected: Bytes::from(v),
                    }
                }),
            ]
        }

        proptest! {
                                    #[test]
            fn indexes_consistent_and_apply_atomic(
                commands in proptest::collection::vec(arb_command(), 0..60)
            ) {
                let mut state = setup();
                for command in &commands {
                    let before = state.clone();
                    match apply(&mut state, command) {
                        Ok(_) => {}
                        Err(_) => prop_assert_eq!(&state, &before, "failed apply mutated state"),
                    }
                    assert_indexes_consistent(&state);
                }
                                let mut replay = setup();
                for command in &commands {
                    let _ = apply(&mut replay, command);
                }
                prop_assert_eq!(&state, &replay);
                                let restored = crate::snapshot::decode(&crate::snapshot::encode(&state))
                    .expect("snapshot of reachable state must decode");
                prop_assert_eq!(state, restored);
            }
        }
    }

    #[test]
    fn slot_weighted_placement_distributes_by_capacity() {
        let mut state = State::new();
        register_with_slots(&mut state, 1, EPOCH_1, 4);
        register_with_slots(&mut state, 2, EPOCH_2, 1);
        let mut owners = Vec::new();
        for _ in 0..5 {
            let stream_id = create(&mut state, NODE_1, EPOCH_1);
            owners.push(place(&mut state, stream_id));
        }
        assert_eq!(owners.iter().filter(|&&n| n == 1).count(), 4);
        assert_eq!(owners.iter().filter(|&&n| n == 2).count(), 1);
    }

    #[test]
    fn placement_tie_breaks_on_lowest_node_id() {
        let mut state = State::new();
        register_with_slots(&mut state, 2, EPOCH_2, 1);
        register_with_slots(&mut state, 1, EPOCH_1, 1);
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        assert_eq!(place(&mut state, stream_id), 1);
    }

    #[test]
    fn placement_is_idempotent_for_already_placed_stream() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        assert_eq!(place(&mut state, stream_id), NODE_1);
        let after_first = state.clone();
        assert_eq!(place(&mut state, stream_id), NODE_1);
        assert_eq!(state, after_first);
    }

    #[test]
    fn place_open_close_delete_keeps_indexes_balanced() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        place(&mut state, stream_id);
        assert_eq!(state.placed_by_node.len(), 1);
        assert_eq!(state.opening_by_node.len(), 0);

        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        assert_eq!(state.placed_by_node.len(), 0);
        assert_eq!(state.opening_by_node.len(), 1);

        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        assert_eq!(state.placed_by_node.len(), 0);
        assert_eq!(state.opening_by_node.len(), 0);

        apply(
            &mut state,
            &Command::DeleteStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        assert_eq!(state.placed_by_node.len(), 0);
        assert_eq!(state.opening_by_node.len(), 0);
    }

    #[test]
    fn place_then_open_by_different_node_moves_indexes() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        place(&mut state, stream_id);
        assert!(state.placed_by_node.contains_key(&(NODE_1, stream_id)));

        open(&mut state, NODE_2, EPOCH_2, stream_id, 1).unwrap();
        assert!(!state.placed_by_node.contains_key(&(NODE_1, stream_id)));
        assert!(state.opening_by_node.contains_key(&(NODE_2, stream_id)));
    }

    #[test]
    fn placing_nonexistent_stream_errors() {
        let mut state = setup();
        let err = apply(&mut state, &Command::PlaceStream { stream_id: 999 }).unwrap_err();
        assert_eq!(err.code(), 1);
    }

    #[test]
    fn placement_without_registered_nodes_errors() {
        let mut state = State::new();
        let stream_id = {
            register(&mut state, NODE_1, EPOCH_1);
            create(&mut state, NODE_1, EPOCH_1)
        };
        state.nodes.remove(&NODE_1);
        let err = apply(&mut state, &Command::PlaceStream { stream_id }).unwrap_err();
        assert_eq!(err.code(), 5);
    }

    #[test]
    fn snapshot_roundtrip_preserves_placed_by_node() {
        let mut state = State::new();
        register_with_slots(&mut state, 1, EPOCH_1, 4);
        register_with_slots(&mut state, 2, EPOCH_2, 1);
        for _ in 0..3 {
            let stream_id = create(&mut state, NODE_1, EPOCH_1);
            place(&mut state, stream_id);
        }
        let before = state.placed_by_node.clone();
        let restored = crate::snapshot::decode(&crate::snapshot::encode(&state)).unwrap();
        assert_eq!(restored.placed_by_node, before);
        assert_eq!(restored, state);
    }

    fn transfer(
        state: &mut State,
        stream_id: u64,
        from_node: i32,
        to_node: i32,
    ) -> Result<Outcome, Error> {
        apply(
            state,
            &Command::TransferStream {
                stream_id,
                from_node,
                to_node,
            },
        )
    }

    fn complete(state: &mut State, stream_id: u64, epoch: i64) -> Result<Outcome, Error> {
        apply(state, &Command::CompleteTransfer { stream_id, epoch })
    }

    #[test]
    fn transfer_records_pending_and_is_idempotent() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();

        transfer(&mut state, stream_id, NODE_1, NODE_2).unwrap();
        let pending = state.pending_transfers.get(&stream_id).unwrap();
        assert_eq!((pending.from_node, pending.to_node), (NODE_1, NODE_2));

        let after_first = state.clone();
        transfer(&mut state, stream_id, NODE_1, NODE_2).unwrap();
        assert_eq!(state, after_first);

        let err = transfer(&mut state, stream_id, NODE_2, NODE_1).unwrap_err();
        assert_eq!(err.code(), 99);
        assert_eq!(state, after_first);
    }

    #[test]
    fn transfer_requires_opened_on_source_and_registered_target() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);

        assert_eq!(
            transfer(&mut state, stream_id, NODE_1, NODE_2)
                .unwrap_err()
                .code(),
            99
        );
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();

        assert_eq!(
            transfer(&mut state, stream_id, NODE_2, NODE_1)
                .unwrap_err()
                .code(),
            99
        );
        assert_eq!(
            transfer(&mut state, stream_id, NODE_1, 9)
                .unwrap_err()
                .code(),
            5
        );
        assert_eq!(
            transfer(&mut state, stream_id, NODE_1, NODE_1)
                .unwrap_err()
                .code(),
            99
        );
        assert_eq!(
            transfer(&mut state, 999, NODE_1, NODE_2)
                .unwrap_err()
                .code(),
            1
        );
        assert!(state.pending_transfers.is_empty());
    }

    #[test]
    fn complete_transfer_repoints_stream_and_clears_pending() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        transfer(&mut state, stream_id, NODE_1, NODE_2).unwrap();

        assert_eq!(complete(&mut state, stream_id, 1).unwrap_err().code(), 2);

        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        assert_eq!(complete(&mut state, stream_id, 0).unwrap_err().code(), 4);

        complete(&mut state, stream_id, 1).unwrap();
        let row = state.streams.get(&stream_id).unwrap();
        assert_eq!(row.node_id, NODE_2);
        assert_eq!(row.state, StreamState::Closed);
        assert!(state.pending_transfers.is_empty());
        assert!(state.placed_by_node.is_empty());

        assert!(
            complete(&mut state, stream_id, 1)
                .unwrap_err()
                .is_redundant()
        );

        open(&mut state, NODE_2, EPOCH_2, stream_id, 2).unwrap();
        assert_eq!(state.streams.get(&stream_id).unwrap().node_id, NODE_2);
        assert!(state.opening_by_node.contains_key(&(NODE_2, stream_id)));
    }

    #[test]
    fn delete_stream_clears_pending_transfer() {
        let mut state = setup();
        let stream_id = create(&mut state, NODE_1, EPOCH_1);
        open(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        transfer(&mut state, stream_id, NODE_1, NODE_2).unwrap();
        close(&mut state, NODE_1, EPOCH_1, stream_id, 1).unwrap();
        apply(
            &mut state,
            &Command::DeleteStream {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                stream_id,
                epoch: 1,
            },
        )
        .unwrap();
        assert!(state.pending_transfers.is_empty());
    }

    #[test]
    fn create_streams_assigns_consecutive_ids() {
        let mut state = setup();
        let single = create(&mut state, NODE_1, EPOCH_1);
        let result = apply(
            &mut state,
            &Command::CreateStreams {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count: 3,
            },
        )
        .unwrap();
        assert_eq!(result, Outcome::Id(single + 1));
        for id in single + 1..single + 4 {
            let row = state.streams.get(&id).unwrap();
            assert_eq!(row.epoch, -1);
            assert_eq!(row.node_id, -1);
            assert_eq!(row.state, StreamState::Closed);
        }
        assert_eq!(state.next_stream_id, single + 4);
    }

    #[test]
    fn create_streams_validates_count_and_epoch() {
        let mut state = setup();
        let before = state.clone();
        let err = apply(
            &mut state,
            &Command::CreateStreams {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count: 0,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 99);
        let err = apply(
            &mut state,
            &Command::CreateStreams {
                node_id: NODE_1,
                node_epoch: EPOCH_1,
                count: MAX_BATCH + 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 99);
        let err = apply(
            &mut state,
            &Command::CreateStreams {
                node_id: NODE_1,
                node_epoch: EPOCH_1 + 1,
                count: 1,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), 5);
        assert_eq!(state, before);
    }
}
