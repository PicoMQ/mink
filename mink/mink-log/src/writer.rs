//! Per-writer sequence tracking that detects duplicate and out-of-order batches, with expiry.

use std::collections::{HashMap, VecDeque};

use mink_record::Header;
use mink_record::header::{NO_BATCH_SEQUENCE, NO_WRITER_ID};
use serde::{Deserialize, Serialize};

use crate::Error;

pub const BATCHES_TO_RETAIN: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMetadata {
    pub batch_sequence: i32,
    pub last_offset: i64,
    pub offset_delta: i32,
    pub timestamp: i64,
}

impl BatchMetadata {
    pub fn first_offset(&self) -> i64 {
        self.last_offset - i64::from(self.offset_delta)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State {
    writer_id: i64,
    last_timestamp: i64,
    batches: VecDeque<BatchMetadata>,
}

impl State {
    pub fn empty(writer_id: i64) -> Self {
        State {
            writer_id,
            last_timestamp: -1,
            batches: VecDeque::with_capacity(BATCHES_TO_RETAIN),
        }
    }

    pub fn with_batch(writer_id: i64, last_timestamp: i64, batch: BatchMetadata) -> Self {
        let mut state = State::empty(writer_id);
        state.push(batch);
        state.last_timestamp = last_timestamp;
        state
    }

    pub fn writer_id(&self) -> i64 {
        self.writer_id
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn last_batch_sequence(&self) -> i32 {
        self.batches
            .back()
            .map_or(NO_BATCH_SEQUENCE, |b| b.batch_sequence)
    }

    pub fn last_data_offset(&self) -> i64 {
        self.batches.back().map_or(-1, |b| b.last_offset)
    }

    pub fn last_offset_delta(&self) -> i32 {
        self.batches.back().map_or(-1, |b| b.offset_delta)
    }

    pub fn last_batch_timestamp(&self) -> i64 {
        self.last_timestamp
    }

    pub fn last_batch(&self) -> Option<&BatchMetadata> {
        self.batches.back()
    }

    pub fn add_batch(&mut self, batch: BatchMetadata) {
        self.last_timestamp = batch.timestamp;
        self.push(batch);
    }

    pub fn update(&mut self, next: State) {
        for batch in next.batches {
            self.push(batch);
        }
        self.last_timestamp = next.last_timestamp;
    }

    pub fn find_duplicate(&self, batch_sequence: i32) -> Option<BatchMetadata> {
        self.batches
            .iter()
            .copied()
            .find(|b| b.batch_sequence == batch_sequence)
    }

    fn push(&mut self, batch: BatchMetadata) {
        if self.batches.len() == BATCHES_TO_RETAIN {
            self.batches.pop_front();
        }
        self.batches.push_back(batch);
    }
}

#[derive(Debug)]
pub(crate) struct Append {
    writer_id: i64,
    current: State,
    updated: State,
}

impl Append {
    fn new(current: State) -> Self {
        let writer_id = current.writer_id();
        Append {
            writer_id,
            current,
            updated: State::empty(writer_id),
        }
    }

    pub(crate) fn writer_id(&self) -> i64 {
        self.writer_id
    }

    pub(crate) fn append(&mut self, header: &Header, expired: bool) -> Result<(), Error> {
        let current = if self.updated.is_empty() {
            self.current.last_batch_sequence()
        } else {
            self.updated.last_batch_sequence()
        };
        if !in_sequence(current, header.batch_sequence, expired) {
            return Err(Error::OutOfOrderSequence {
                writer_id: self.writer_id,
                incoming: header.batch_sequence,
                current,
            });
        }
        self.updated.add_batch(BatchMetadata {
            batch_sequence: header.batch_sequence,
            last_offset: header.last_offset(),
            offset_delta: header.last_offset_delta,
            timestamp: header.commit_timestamp,
        });

        Ok(())
    }

    pub(crate) fn into_state(self) -> State {
        self.updated
    }
}

fn in_sequence(last: i32, next: i32, expired: bool) -> bool {
    (last == NO_BATCH_SEQUENCE && expired)
        || i64::from(next) == i64::from(last) + 1
        || (next == 0 && last == i32::MAX)
}

#[derive(Debug)]
pub struct Writers {
    expiration_ms: i64,
    writers: HashMap<i64, State>,
    end: i64,
}

impl Writers {
    pub fn new(expiration_ms: i64) -> Self {
        Writers {
            expiration_ms,
            writers: HashMap::new(),
            end: 0,
        }
    }

    pub fn expiration_ms(&self) -> i64 {
        self.expiration_ms
    }

    pub fn map_end_offset(&self) -> i64 {
        self.end
    }

    pub fn set_map_end_offset(&mut self, offset: i64) {
        self.end = offset
    }

    pub fn writer_count(&self) -> usize {
        self.writers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }

    pub fn last_entry(&self, writer_id: i64) -> Option<&State> {
        self.writers.get(&writer_id)
    }

    pub fn active_writers(&self) -> impl Iterator<Item = &State> {
        self.writers.values()
    }

    pub(crate) fn prepare_update(&self, writer_id: i64) -> Append {
        let current = self
            .writers
            .get(&writer_id)
            .cloned()
            .unwrap_or_else(|| State::empty(writer_id));

        Append::new(current)
    }

    pub(crate) fn update(&mut self, append: Append) {
        let writer_id = append.writer_id();
        assert_ne!(writer_id, NO_WRITER_ID, "cannot track NO_WRITER_ID");
        let updated = append.into_state();
        match self.writers.get_mut(&writer_id) {
            Some(current) => current.update(updated),
            None => {
                self.writers.insert(writer_id, updated);
            }
        }
    }

    pub fn load(&mut self, state: State) {
        self.writers.insert(state.writer_id(), state);
    }

    pub fn remove_expired(&mut self, now_ms: i64) {
        let expiration = self.expiration_ms;
        self.writers
            .retain(|_, state| now_ms - state.last_batch_timestamp() <= expiration);
    }

    pub fn is_batch_expired(&self, now_ms: i64, header: &Header) -> bool {
        now_ms - header.commit_timestamp > self.expiration_ms
    }

    pub fn truncate_fully_and_start_at(&mut self, offset: i64) {
        self.writers.clear();
        self.end = offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(writer_id: i64, seq: i32, base: i64, count: i32, ts: i64) -> Header {
        Header {
            base_offset: base,
            size: 52,
            commit_timestamp: ts,
            leader_epoch: -1,
            crc: 0,
            schema_id: mink_table::SchemaId(1),
            append_only: true,
            last_offset_delta: (count - 1).max(0),
            writer_id,
            batch_sequence: seq,
            record_count: count,
        }
    }

    fn appended(manager: &mut Writers, h: &Header) -> Result<(), Error> {
        let mut append = manager.prepare_update(h.writer_id);
        append.append(h, false)?;
        manager.update(append);
        Ok(())
    }

    #[test]
    fn fresh_writer_must_start_at_zero() {
        let mut manager = Writers::new(1000);
        let err = appended(&mut manager, &header(7, 3, 0, 2, 10)).unwrap_err();
        assert!(matches!(
            err,
            Error::OutOfOrderSequence {
                writer_id: 7,
                incoming: 3,
                current: NO_BATCH_SEQUENCE
            }
        ));
        appended(&mut manager, &header(7, 0, 0, 2, 10)).unwrap();
        assert_eq!(manager.last_entry(7).unwrap().last_data_offset(), 1);
    }

    #[test]
    fn expired_unknown_writer_may_start_anywhere() {
        let mut manager = Writers::new(1000);
        let mut append = manager.prepare_update(7);
        append.append(&header(7, 42, 0, 1, 0), true).unwrap();
        manager.update(append);
        assert_eq!(manager.last_entry(7).unwrap().last_batch_sequence(), 42);
    }

    #[test]
    fn sequence_wraps_from_max_to_zero() {
        let mut manager = Writers::new(1000);
        manager.load(State::with_batch(
            7,
            10,
            BatchMetadata {
                batch_sequence: i32::MAX,
                last_offset: 5,
                offset_delta: 0,
                timestamp: 10,
            },
        ));
        appended(&mut manager, &header(7, 0, 6, 1, 11)).unwrap();
        let err = appended(&mut manager, &header(7, 2, 7, 1, 12)).unwrap_err();
        assert!(matches!(err, Error::OutOfOrderSequence { current: 0, .. }));
    }

    #[test]
    fn within_one_append_sequences_chain() {
        let manager = Writers::new(1000);
        let mut append = manager.prepare_update(1);
        append.append(&header(1, 0, 0, 1, 1), false).unwrap();
        append.append(&header(1, 1, 1, 1, 1), false).unwrap();
        let err = append.append(&header(1, 3, 2, 1, 1), false).unwrap_err();
        assert!(matches!(
            err,
            Error::OutOfOrderSequence {
                incoming: 3,
                current: 1,
                ..
            }
        ));
    }

    #[test]
    fn duplicate_window_keeps_five_batches() {
        let mut manager = Writers::new(1000);
        for seq in 0..7 {
            appended(&mut manager, &header(1, seq, i64::from(seq), 1, 1)).unwrap();
        }
        let state = manager.last_entry(1).unwrap();
        assert!(state.find_duplicate(1).is_none());
        assert_eq!(state.find_duplicate(2).unwrap().last_offset, 2);
        assert_eq!(state.find_duplicate(6).unwrap().first_offset(), 6);
    }

    #[test]
    fn expiry_uses_last_batch_timestamp() {
        let mut manager = Writers::new(1000);
        appended(&mut manager, &header(1, 0, 0, 1, 100)).unwrap();
        appended(&mut manager, &header(2, 0, 1, 1, 900)).unwrap();
        manager.remove_expired(1500);
        assert!(manager.last_entry(1).is_none());
        assert!(manager.last_entry(2).is_some());
        assert!(manager.is_batch_expired(1500, &header(3, 0, 0, 1, 100)));
        assert!(!manager.is_batch_expired(1100, &header(3, 0, 0, 1, 100)));
    }
}
