//! In-memory store over a sorted map, checkpointed to a single checksummed file.

use std::collections::BTreeMap;
use std::collections::btree_map::IntoIter;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use mink_common::codec;
use mink_common::sync::{read, write};

use crate::batch::{Batch, Op};
use crate::checkpoint::Checkpoint;
use crate::store::{Engine, Snapshot, Store};
use crate::{Error, Options};

const FILE_NAME: &str = "entries.minkkv";
const MAGIC: &[u8; 6] = b"MINKV\x01";

#[derive(Debug, Default)]
pub struct MemoryStore {
    state: RwLock<State>,
}

#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<Bytes, Bytes>,
    closed: bool,
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore::default()
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, State>, Error> {
        let state = read(&self.state);
        if state.closed {
            return Err(Error::Closed);
        }
        Ok(state)
    }

    fn writable(&self) -> Result<RwLockWriteGuard<'_, State>, Error> {
        let state = write(&self.state);
        if state.closed {
            return Err(Error::Closed);
        }
        Ok(state)
    }

    fn load(path: &Path) -> Result<BTreeMap<Bytes, Bytes>, Error> {
        let mut bytes = Vec::new();
        fs::File::open(path.join(FILE_NAME))?.read_to_end(&mut bytes)?;
        decode(&bytes)
    }
}

fn encode(entries: &BTreeMap<Bytes, Bytes>) -> Vec<u8> {
    let mut out = BytesMut::new();
    out.put_slice(MAGIC);
    for (key, value) in entries {
        codec::put_bytes(&mut out, key);
        codec::put_bytes(&mut out, value);
    }

    let crc = crc32c::crc32c(&out);
    out.put_u32_le(crc);

    out.to_vec()
}

fn decode(bytes: &[u8]) -> Result<BTreeMap<Bytes, Bytes>, Error> {
    let corrupt = |what: &str| Error::Corrupt(format!("memory checkpoint: {what}"));
    if bytes.len() < MAGIC.len() + 4 || &bytes[..MAGIC.len()] != MAGIC {
        return Err(corrupt("bad header"));
    }

    let (body, trailer) = bytes.split_at(bytes.len() - 4);
    let stored = u32::from_le_bytes(trailer.try_into().map_err(|_| corrupt("bad trailer"))?);
    if crc32c::crc32c(body) != stored {
        return Err(corrupt("crc mismatch"));
    }

    let mut cursor = &body[MAGIC.len()..];
    let mut entries = BTreeMap::new();
    while cursor.has_remaining() {
        let key = codec::get_bytes(&mut cursor).map_err(|_| corrupt("truncated key"))?;
        let value = codec::get_bytes(&mut cursor).map_err(|_| corrupt("truncated value"))?;
        entries.insert(key, value);
    }

    Ok(entries)
}

#[async_trait]
impl Store for MemoryStore {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        Ok(self.read()?.entries.get(key).cloned())
    }

    fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>, Error> {
        let state = self.read()?;

        Ok(keys
            .iter()
            .map(|k| state.entries.get(*k).cloned())
            .collect())
    }

    fn prefix_lookup(&self, prefix: &[u8]) -> Result<Vec<Bytes>, Error> {
        let state = self.read()?;

        Ok(state
            .entries
            .range(Bytes::copy_from_slice(prefix)..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(_, v)| v.clone())
            .collect())
    }

    fn limit_scan(&self, limit: usize) -> Result<Vec<Bytes>, Error> {
        let state = self.read()?;
        Ok(state.entries.values().take(limit).cloned().collect())
    }

    fn snapshot(&self) -> Result<Box<dyn Snapshot>, Error> {
        Ok(Box::new(Cursor(self.read()?.entries.clone().into_iter())))
    }

    async fn write(&self, batch: Batch) -> Result<(), Error> {
        let mut state = self.writable()?;
        for op in batch.into_ops() {
            match op {
                Op::Put { key, value } => {
                    state.entries.insert(key, value);
                }
                Op::Delete { key } => {
                    state.entries.remove(&key);
                }
            }
        }

        Ok(())
    }

    fn checkpoint(&self, dir: &Path) -> Result<Checkpoint, Error> {
        let encoded = encode(&self.read()?.entries);
        fs::create_dir_all(dir)?;
        fs::File::create(dir.join(FILE_NAME))?.write_all(&encoded)?;
        Checkpoint::scan(dir)
    }

    async fn close(&self) -> Result<(), Error> {
        write(&self.state).closed = true;
        Ok(())
    }
}

struct Cursor(IntoIter<Bytes, Bytes>);

impl Snapshot for Cursor {
    fn next_page(&mut self, max: usize) -> Result<Vec<(Bytes, Bytes)>, Error> {
        Ok(self.0.by_ref().take(max).collect())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryEngine;

impl Engine for MemoryEngine {
    fn open(&self, _path: &Path, _options: Options) -> Result<Box<dyn Store>, Error> {
        Ok(Box::new(MemoryStore::new()))
    }

    fn restore(
        &self,
        _path: &Path,
        checkpoint: &Path,
        _options: Options,
    ) -> Result<Box<dyn Store>, Error> {
        let entries = MemoryStore::load(checkpoint)?;
        Ok(Box::new(MemoryStore {
            state: RwLock::new(State {
                entries,
                closed: false,
            }),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_file_round_trips_and_detects_corruption() {
        let mut entries = BTreeMap::new();
        entries.insert(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        entries.insert(Bytes::from_static(b""), Bytes::new());
        let mut bytes = encode(&entries);
        assert_eq!(decode(&bytes).unwrap(), entries);

        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(matches!(decode(&bytes), Err(Error::Corrupt(_))));
        assert!(matches!(decode(b"nope"), Err(Error::Corrupt(_))));
    }
}
