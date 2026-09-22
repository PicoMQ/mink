//! Rows staged between writing the changelog and flushing to the store, with rollback to a sequence.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use mink_kv::Writer;
use mink_record::ChangeType;

use crate::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    change: ChangeType,
    key: Bytes,
    value: Option<Bytes>,
    sequence: i64,
    previous: Option<Box<Entry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncateReason {
    Duplicated,
    Error,
}

#[derive(Debug, Default)]
pub struct Buffer {
    latest: HashMap<Bytes, Entry>,
    all: VecDeque<Entry>,
    max_sequence: i64,
    truncated_as_duplicated: u64,
    truncated_as_error: u64,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer {
            max_sequence: -1,
            ..Default::default()
        }
    }

    pub fn insert(&mut self, key: Bytes, value: Bytes, sequence: i64) -> Result<(), Error> {
        self.put(ChangeType::Insert, key, Some(value), sequence)
    }

    pub fn update(&mut self, key: Bytes, value: Bytes, sequence: i64) -> Result<(), Error> {
        self.put(ChangeType::UpdateAfter, key, Some(value), sequence)
    }

    pub fn delete(&mut self, key: Bytes, sequence: i64) -> Result<(), Error> {
        self.put(ChangeType::Delete, key, None, sequence)
    }

    fn put(
        &mut self,
        change: ChangeType,
        key: Bytes,
        value: Option<Bytes>,
        sequence: i64,
    ) -> Result<(), Error> {
        if self.max_sequence >= sequence {
            return Err(Error::SequenceOrder {
                current: self.max_sequence,
                incoming: sequence,
            });
        }

        let previous = self.latest.remove(&key).map(Box::new);
        let entry = Entry {
            change,
            key: key.clone(),
            value,
            sequence,
            previous,
        };
        self.latest.insert(key, entry.clone());
        self.all.push_back(entry);
        self.max_sequence = sequence;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<&Bytes>> {
        self.latest.get(key).map(|entry| entry.value.as_ref())
    }

    pub fn max_sequence(&self) -> i64 {
        self.max_sequence
    }

    pub fn len(&self) -> usize {
        self.all.len()
    }

    pub fn is_empty(&self) -> bool {
        self.all.is_empty()
    }

    pub fn truncate_to(&mut self, sequence: i64, reason: TruncateReason) {
        match reason {
            TruncateReason::Duplicated => self.truncated_as_duplicated += 1,
            TruncateReason::Error => self.truncated_as_error += 1,
        }

        while let Some(last) = self.all.back() {
            if last.sequence < sequence {
                self.max_sequence = last.sequence;
                return;
            }
            let entry = self.all.pop_back().expect("checked back");
            if self.latest.get(&entry.key) == Some(&entry) {
                self.latest.remove(&entry.key);
                if let Some(previous) = entry.previous {
                    self.latest.insert(previous.key.clone(), *previous);
                }
            }
        }
        self.max_sequence = -1;
    }

    pub async fn flush(&mut self, sequence: i64, writer: &mut Writer<'_>) -> Result<i64, Error> {
        let mut row_count_delta = 0;
        let mut flushed = 0;
        while let Some(front) = self.all.front() {
            if front.sequence >= sequence {
                break;
            }
            let entry = self.all.pop_front().expect("checked front");
            match &entry.value {
                Some(value) => writer.put(entry.key.clone(), value.clone()).await?,
                None => writer.delete(entry.key.clone()).await?,
            }
            flushed += 1;
            match entry.change {
                ChangeType::Insert => row_count_delta += 1,
                ChangeType::Delete => row_count_delta -= 1,
                _ => {}
            }
            if self.latest.get(&entry.key) == Some(&entry) {
                self.latest.remove(&entry.key);
            }
        }
        if flushed > 0 {
            writer.flush().await?;
        }
        Ok(row_count_delta)
    }

    pub fn truncated_as_duplicated(&self) -> u64 {
        self.truncated_as_duplicated
    }

    pub fn truncated_as_error(&self) -> u64 {
        self.truncated_as_error
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use mink_kv::{Engine, MemoryEngine, Options, Store};

    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn store() -> Box<dyn Store> {
        MemoryEngine
            .open(Path::new("/unused"), Options::default())
            .unwrap()
    }

    #[test]
    fn latest_write_wins_and_deletes_are_visible() {
        let mut buffer = Buffer::new();
        buffer.insert(b("k"), b("v1"), 0).unwrap();
        buffer.update(b("k"), b("v2"), 1).unwrap();
        assert_eq!(buffer.get(b"k"), Some(Some(&b("v2"))));
        buffer.delete(b("k"), 2).unwrap();
        assert_eq!(buffer.get(b"k"), Some(None));
        assert_eq!(buffer.get(b"other"), None);
        assert_eq!(buffer.max_sequence(), 2);
    }

    #[test]
    fn sequences_must_increase() {
        let mut buffer = Buffer::new();
        buffer.insert(b("k"), b("v"), 5).unwrap();
        assert!(matches!(
            buffer.insert(b("k"), b("v"), 5),
            Err(Error::SequenceOrder {
                current: 5,
                incoming: 5
            })
        ));
    }

    #[test]
    fn truncate_restores_shadowed_entries() {
        let mut buffer = Buffer::new();
        buffer.insert(b("k"), b("v1"), 0).unwrap();
        buffer.insert(b("j"), b("w1"), 1).unwrap();
        buffer.update(b("k"), b("v2"), 2).unwrap();
        buffer.delete(b("j"), 3).unwrap();

        buffer.truncate_to(2, TruncateReason::Error);
        assert_eq!(buffer.get(b"k"), Some(Some(&b("v1"))));
        assert_eq!(buffer.get(b"j"), Some(Some(&b("w1"))));
        assert_eq!(buffer.max_sequence(), 1);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.truncated_as_error(), 1);

        buffer.truncate_to(0, TruncateReason::Duplicated);
        assert!(buffer.is_empty());
        assert_eq!(buffer.max_sequence(), -1);
        assert_eq!(buffer.get(b"k"), None);
        assert_eq!(buffer.truncated_as_duplicated(), 1);
    }

    #[tokio::test]
    async fn flush_writes_prefix_and_counts_rows() {
        let store = store();
        let mut buffer = Buffer::new();
        buffer.insert(b("a"), b("1"), 0).unwrap();
        buffer.insert(b("b"), b("2"), 1).unwrap();
        buffer.delete(b("a"), 2).unwrap();
        buffer.update(b("b"), b("3"), 3).unwrap();
        buffer.insert(b("c"), b("4"), 4).unwrap();

        let mut writer = Writer::new(store.as_ref(), 0);
        let delta = buffer.flush(3, &mut writer).await.unwrap();
        assert_eq!(delta, 1);
        assert_eq!(store.get(b"a").unwrap(), None);
        assert_eq!(store.get(b"b").unwrap(), Some(b("2")));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.get(b"b"), Some(Some(&b("3"))));
        assert_eq!(buffer.get(b"a"), None);

        let delta = buffer.flush(i64::MAX, &mut writer).await.unwrap();
        assert_eq!(delta, 1);
        assert!(buffer.is_empty());
        assert_eq!(store.get(b"b").unwrap(), Some(b("3")));
        assert_eq!(store.get(b"c").unwrap(), Some(b("4")));
    }
}
