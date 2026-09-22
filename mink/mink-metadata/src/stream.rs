//! Handlers for node registration and the stream lifecycle: create, place, open, trim, close, transfer, delete.

use std::collections::BTreeMap;

use s3stream::{CompactOperations, StreamState};

use crate::command::Outcome;
use crate::error::Error;
use crate::object::mark_destroyed;
use crate::state::{NodeRow, PendingTransfer, State, StreamRow};

pub(crate) fn register_node(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    http_address: &str,
    slots: u32,
    protocol_addresses: &BTreeMap<String, String>,
) -> Result<Outcome, Error> {
    if let Some(node) = state.nodes.get(&node_id)
        && node.epoch > node_epoch
    {
        return Err(Error::NodeEpochMismatch {
            node_id,
            message: format!(
                "node {node_id} epoch {} fences register with epoch {node_epoch}",
                node.epoch
            ),
        });
    }

    let previous = state.nodes.get(&node_id);
    let http_address = if http_address.is_empty() {
        previous.map(|n| n.http_address.clone()).unwrap_or_default()
    } else {
        http_address.to_owned()
    };
    let protocol_addresses = if protocol_addresses.is_empty() {
        previous
            .map(|n| n.protocol_addresses.clone())
            .unwrap_or_default()
    } else {
        protocol_addresses.clone()
    };

    state.nodes.insert(
        node_id,
        NodeRow {
            node_id,
            epoch: node_epoch,
            http_address,
            slots,
            protocol_addresses,
        },
    );

    Ok(Outcome::Unit)
}

pub(crate) fn node_epoch_check(state: &State, node_id: i32, node_epoch: i64) -> Result<(), Error> {
    match state.nodes.get(&node_id) {
        None => Err(Error::NodeEpochMismatch {
            node_id,
            message: format!("node {node_id} is not registered"),
        }),
        Some(node) if node.epoch != node_epoch => Err(Error::NodeEpochMismatch {
            node_id,
            message: format!(
                "node {node_id} epoch mismatch current={} request={node_epoch}",
                node.epoch
            ),
        }),
        Some(_) => Ok(()),
    }
}

pub(crate) fn create_stream(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;

    Ok(Outcome::Id(state.alloc_stream()))
}

pub(crate) const MAX_BATCH: u32 = 4096;

fn check_batch(what: &str, count: u32) -> Result<(), Error> {
    if count == 0 {
        return Err(Error::Unexpected {
            message: format!("{what} count must be positive"),
        });
    }
    if count > MAX_BATCH {
        return Err(Error::Unexpected {
            message: format!("{what} count {count} exceeds {MAX_BATCH}"),
        });
    }

    Ok(())
}

fn check_epoch(stream: &StreamRow, epoch: i64) -> Result<(), Error> {
    if stream.epoch != epoch {
        return Err(Error::ExpiredEpoch {
            stream_id: stream.stream_id,
            epoch,
            message: format!(
                "stream {} epoch {epoch} is not equal to current epoch {}",
                stream.stream_id, stream.epoch
            ),
        });
    }

    Ok(())
}

fn check_opened(stream: &StreamRow) -> Result<(), Error> {
    if stream.state != StreamState::Opened {
        return Err(Error::Unexpected {
            message: format!("stream {} is not opened", stream.stream_id),
        });
    }

    Ok(())
}

pub(crate) fn create_streams(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    count: u32,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    check_batch("create", count)?;

    let first = state.next_stream_id;
    for _ in 0..count {
        state.alloc_stream();
    }

    Ok(Outcome::Id(first))
}

pub(crate) fn place_stream(state: &mut State, stream_id: u64) -> Result<Outcome, Error> {
    let stream = require_stream(state, stream_id)?;

    if stream.node_id != -1 {
        return Ok(Outcome::Id(stream.node_id as u64));
    }
    if stream.state != StreamState::Closed || stream.epoch != -1 {
        return Err(Error::Unexpected {
            message: format!(
                "stream {stream_id} cannot be placed in state {:?} epoch {}",
                stream.state, stream.epoch
            ),
        });
    }

    let mut winner: Option<(i32, u64)> = None;
    for (node_id, node) in state.nodes.iter() {
        if node.slots < 1 {
            continue;
        }
        let opening_count = state
            .opening_by_node
            .range((*node_id, 0)..=(*node_id, u64::MAX))
            .count() as u64;
        let placed_count = state
            .placed_by_node
            .range((*node_id, 0)..=(*node_id, u64::MAX))
            .count() as u64;
        let score = (opening_count + placed_count) * 1000 / node.slots as u64;
        match winner {
            None => winner = Some((*node_id, score)),
            Some((best_id, best_score))
                if score < best_score || (score == best_score && *node_id < best_id) =>
            {
                winner = Some((*node_id, score));
            }
            _ => {}
        }
    }

    let Some((winner, _)) = winner else {
        return Err(Error::NodeEpochMismatch {
            node_id: -1,
            message: "no registered nodes with slots >= 1 for stream placement".into(),
        });
    };

    state.streams.insert(
        stream_id,
        StreamRow {
            node_id: winner,
            ..stream
        },
    );
    state.placed_by_node.insert((winner, stream_id), ());

    Ok(Outcome::Id(winner as u64))
}

pub(crate) fn require_stream(state: &State, stream_id: u64) -> Result<StreamRow, Error> {
    state
        .streams
        .get(&stream_id)
        .copied()
        .ok_or(Error::StreamNotExist { stream_id })
}

pub(crate) fn open_stream(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    stream_id: u64,
    epoch: i64,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream = require_stream(state, stream_id)?;

    if stream.epoch > epoch {
        return Err(Error::StreamFenced {
            stream_id,
            epoch,
            message: format!(
                "stream {stream_id} epoch {} fences request epoch {epoch}",
                stream.epoch
            ),
        });
    }
    if stream.epoch == epoch {
        if stream.state == StreamState::Opened && stream.node_id == node_id {
            return Ok(Outcome::Stream(stream.to_stream_metadata()));
        }
        return Err(Error::StreamFenced {
            stream_id,
            epoch,
            message: format!("stream {stream_id} epoch {epoch} already used"),
        });
    }
    if stream.state == StreamState::Opened {
        return Err(Error::StreamNotClosed { stream_id });
    }

    let opened = StreamRow {
        epoch,
        state: StreamState::Opened,
        node_id,
        ..stream
    };
    state.streams.insert(stream_id, opened);
    state.placed_by_node.remove(&(stream.node_id, stream_id));
    state.opening_by_node.insert((node_id, stream_id), ());

    Ok(Outcome::Stream(opened.to_stream_metadata()))
}

pub(crate) fn require_opened_stream(
    state: &State,
    stream_id: u64,
    epoch: i64,
) -> Result<StreamRow, Error> {
    let stream = require_stream(state, stream_id)?;
    check_opened(&stream)?;
    check_epoch(&stream, epoch)?;

    Ok(stream)
}

pub(crate) fn trim_stream(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    stream_id: u64,
    epoch: i64,
    new_start_offset: u64,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream = require_opened_stream(state, stream_id, epoch)?;
    if new_start_offset < stream.start_offset {
        return Err(Error::Unexpected {
            message: format!(
                "stream {stream_id} new start offset {new_start_offset} is less than current start offset {}",
                stream.start_offset
            ),
        });
    }
    if new_start_offset > stream.end_offset {
        return Err(Error::Unexpected {
            message: format!(
                "stream {stream_id} new start offset {new_start_offset} is greater than current end offset {}",
                stream.end_offset
            ),
        });
    }

    state.streams.insert(
        stream_id,
        StreamRow {
            start_offset: new_start_offset,
            ..stream
        },
    );

    Ok(Outcome::Unit)
}

pub(crate) fn close_stream(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    stream_id: u64,
    epoch: i64,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    let stream = require_stream(state, stream_id)?;

    if stream.state == StreamState::Closed && stream.epoch == epoch {
        return Ok(Outcome::Unit);
    }
    check_opened(&stream)?;
    check_epoch(&stream, epoch)?;

    state.streams.insert(
        stream_id,
        StreamRow {
            state: StreamState::Closed,
            ..stream
        },
    );
    state.opening_by_node.remove(&(stream.node_id, stream_id));

    Ok(Outcome::Unit)
}

pub(crate) fn transfer_stream(
    state: &mut State,
    stream_id: u64,
    from_node: i32,
    to_node: i32,
) -> Result<Outcome, Error> {
    let stream = require_stream(state, stream_id)?;

    if let Some(pending) = state.pending_transfers.get(&stream_id) {
        if pending.from_node == from_node && pending.to_node == to_node {
            return Ok(Outcome::Unit);
        }
        return Err(Error::Unexpected {
            message: format!(
                "stream {stream_id} already transferring from {} to {}",
                pending.from_node, pending.to_node
            ),
        });
    }
    if from_node == to_node {
        return Err(Error::Unexpected {
            message: format!("stream {stream_id} transfer target equals source {from_node}"),
        });
    }
    match state.nodes.get(&to_node) {
        Some(node) if node.slots >= 1 => {}
        _ => {
            return Err(Error::NodeEpochMismatch {
                node_id: to_node,
                message: format!("transfer target {to_node} is not a registered node with slots"),
            });
        }
    }
    if stream.state != StreamState::Opened || stream.node_id != from_node {
        return Err(Error::Unexpected {
            message: format!(
                "stream {stream_id} is not opened on node {from_node}, state {:?} node {}",
                stream.state, stream.node_id
            ),
        });
    }

    state
        .pending_transfers
        .insert(stream_id, PendingTransfer { from_node, to_node });

    Ok(Outcome::Unit)
}

pub(crate) fn complete_transfer(
    state: &mut State,
    stream_id: u64,
    epoch: i64,
) -> Result<Outcome, Error> {
    let Some(pending) = state.pending_transfers.get(&stream_id).copied() else {
        return Err(Error::Redundant {
            message: format!("no pending transfer for stream {stream_id}"),
        });
    };
    let stream = require_stream(state, stream_id)?;
    if stream.state != StreamState::Closed {
        return Err(Error::StreamNotClosed { stream_id });
    }
    check_epoch(&stream, epoch)?;

    state.streams.insert(
        stream_id,
        StreamRow {
            node_id: pending.to_node,
            ..stream
        },
    );
    state.pending_transfers.remove(&stream_id);

    Ok(Outcome::Unit)
}

pub(crate) fn delete_stream(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    stream_id: u64,
    epoch: i64,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    let Some(stream) = state.streams.get(&stream_id).copied() else {
        return Ok(Outcome::Unit);
    };
    if stream.state != StreamState::Closed {
        return Err(Error::StreamNotClosed { stream_id });
    }
    check_epoch(&stream, epoch)?;

    state.streams.remove(&stream_id);
    remove_placed_stream(state, stream_id);
    state.pending_transfers.remove(&stream_id);

    let keys: Vec<_> = state
        .stream_objects
        .range((stream_id, 0, 0)..=(stream_id, u64::MAX, u64::MAX))
        .map(|(key, _)| *key)
        .collect();
    for key in keys {
        state.stream_objects.remove(&key);
        state.stream_object_ids.remove(&key.2);
        mark_destroyed(state, key.2, CompactOperations::Delete);
    }

    Ok(Outcome::Unit)
}

pub(crate) fn remove_placed_stream(state: &mut State, stream_id: u64) {
    let keys: Vec<_> = state
        .placed_by_node
        .keys()
        .filter(|(_, sid)| *sid == stream_id)
        .copied()
        .collect();
    for key in keys {
        state.placed_by_node.remove(&key);
    }
}

pub(crate) fn allocate_producer_ids(
    state: &mut State,
    node_id: i32,
    node_epoch: i64,
    count: u32,
) -> Result<Outcome, Error> {
    node_epoch_check(state, node_id, node_epoch)?;
    check_batch("producer id", count)?;

    let first = state.next_producer_id;
    state.next_producer_id += count as u64;

    Ok(Outcome::Id(first))
}
