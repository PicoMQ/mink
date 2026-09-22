//! The state machine's tables: streams, nodes, objects, transfers, key-value entries and the catalog.

use std::collections::BTreeMap;

use bytes::Bytes;
use im::OrdMap;
use s3stream::{CompactOperations, S3ObjectMetadata, StreamMetadata, StreamState};

use crate::catalog::Catalog;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRow {
    pub stream_id: u64,
    pub epoch: i64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub state: StreamState,
    pub node_id: i32,
}

impl StreamRow {
    pub fn to_stream_metadata(self) -> StreamMetadata {
        StreamMetadata {
            stream_id: self.stream_id,
            epoch: self.epoch.max(0) as u64,
            start_offset: self.start_offset,
            end_offset: self.end_offset,
            state: self.state,
            node_id: self.node_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRow {
    pub node_id: i32,
    pub epoch: i64,
    pub http_address: String,
    pub slots: u32,
    pub protocol_addresses: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingTransfer {
    pub from_node: i32,
    pub to_node: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamSetObjectRow {
    pub node_id: i32,
    pub object: S3ObjectMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamObjectRow {
    pub object: S3ObjectMetadata,
}

pub type StreamOffsetKey = (u64, u64, u64);

#[derive(Debug, Clone, Default, PartialEq)]
pub struct State {
    pub next_stream_id: u64,
    pub streams: OrdMap<u64, StreamRow>,
    pub nodes: OrdMap<i32, NodeRow>,
    pub opening_by_node: OrdMap<(i32, u64), ()>,
    pub placed_by_node: OrdMap<(i32, u64), ()>,
    pub pending_transfers: OrdMap<u64, PendingTransfer>,

    pub next_producer_id: u64,

    pub next_object_id: u64,
    pub prepared: OrdMap<u64, i64>,
    pub prepared_by_deadline: OrdMap<(i64, u64), ()>,
    pub stream_set_objects: OrdMap<u64, StreamSetObjectRow>,
    pub sso_ranges: OrdMap<StreamOffsetKey, u64>,
    pub sso_by_node: OrdMap<(i32, u64), ()>,
    pub stream_objects: OrdMap<StreamOffsetKey, StreamObjectRow>,
    pub stream_object_ids: OrdMap<u64, StreamOffsetKey>,
    pub mark_destroyed: OrdMap<u64, (u64, CompactOperations)>,
    pub destroyed_by_id: OrdMap<u64, u64>,
    pub next_destroyed_seq: u64,

    pub kv: OrdMap<String, Bytes>,
    pub kv_bytes: u64,

    pub catalog: Catalog,
}

impl State {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn alloc_stream(&mut self) -> u64 {
        let id = self.next_stream_id;
        self.next_stream_id += 1;

        self.streams.insert(
            id,
            StreamRow {
                stream_id: id,
                epoch: -1,
                start_offset: 0,
                end_offset: 0,
                state: StreamState::Closed,
                node_id: -1,
            },
        );

        id
    }
}
