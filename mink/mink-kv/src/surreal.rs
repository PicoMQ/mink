//! Persistent store on an LSM tree, with keys namespaced so range scans have a bounded end.

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use surrealkv::{Durability, LSMIterator, Mode, Transaction, Tree, TreeBuilder};

use crate::batch::{Batch, Op};
use crate::checkpoint::Checkpoint;
use crate::store::{Engine, Snapshot, Store};
use crate::{Error, Options};

// Keys live under a 0x00 prefix so the whole keyspace has an exclusive end.
const NAMESPACE: u8 = 0x00;
const END: [u8; 1] = [NAMESPACE + 1];

pub struct SurrealStore {
    tree: Tree,
    closed: AtomicBool,
}

impl SurrealStore {
    pub fn open(path: &Path, options: Options) -> Result<Self, Error> {
        let tree = TreeBuilder::new()
            .with_path(path.to_path_buf())
            .with_max_memtable_size(options.memtable_bytes)
            .with_block_size(options.block_bytes)
            .with_block_cache_capacity(options.block_cache_bytes)
            .build()
            .map_err(engine)?;

        Ok(SurrealStore {
            tree,
            closed: AtomicBool::new(false),
        })
    }

    pub fn restore(path: &Path, checkpoint: &Path, options: Options) -> Result<Self, Error> {
        // Object stores drop empty directories; open needs wal/ and sstables/.
        for subdir in ["wal", "sstables"] {
            fs::create_dir_all(checkpoint.join(subdir))?;
        }

        let store = SurrealStore::open(path, options)?;
        store
            .tree
            .restore_from_checkpoint(checkpoint)
            .map_err(engine)?;

        Ok(store)
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        Ok(())
    }

    fn reader(&self) -> Result<Transaction, Error> {
        self.ensure_open()?;
        self.tree.begin_with_mode(Mode::ReadOnly).map_err(engine)
    }

    fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<Bytes>, Error> {
        let tx = self.reader()?;
        let mut iter = tx.range(start, end).map_err(engine)?;

        let mut values = Vec::new();
        let mut valid = iter.seek_first().map_err(engine)?;
        while valid && values.len() < limit {
            values.push(Bytes::from(iter.value().map_err(engine)?));
            valid = iter.next().map_err(engine)?;
        }

        Ok(values)
    }
}

fn namespaced(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 1);
    out.push(NAMESPACE);
    out.extend_from_slice(key);
    out
}

fn successor(mut prefix: Vec<u8>) -> Vec<u8> {
    while let Some(last) = prefix.pop() {
        if last < u8::MAX {
            prefix.push(last + 1);
            return prefix;
        }
    }

    END.to_vec()
}

fn engine(error: surrealkv::Error) -> Error {
    Error::Engine(error.to_string())
}

#[async_trait]
impl Store for SurrealStore {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        let tx = self.reader()?;
        Ok(tx
            .get(namespaced(key).as_slice())
            .map_err(engine)?
            .map(Bytes::from))
    }

    fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>, Error> {
        let tx = self.reader()?;
        keys.iter()
            .map(|key| {
                Ok(tx
                    .get(namespaced(key).as_slice())
                    .map_err(engine)?
                    .map(Bytes::from))
            })
            .collect()
    }

    fn prefix_lookup(&self, prefix: &[u8]) -> Result<Vec<Bytes>, Error> {
        let start = namespaced(prefix);
        let end = successor(start.clone());
        self.scan(&start, &end, usize::MAX)
    }

    fn limit_scan(&self, limit: usize) -> Result<Vec<Bytes>, Error> {
        self.scan(&[NAMESPACE], &END, limit)
    }

    fn snapshot(&self) -> Result<Box<dyn Snapshot>, Error> {
        Ok(Box::new(Cursor {
            tx: self.reader()?,
            next: Some(vec![NAMESPACE]),
        }))
    }

    async fn write(&self, batch: Batch) -> Result<(), Error> {
        self.ensure_open()?;
        if batch.is_empty() {
            return Ok(());
        }

        let mut tx = self.tree.begin_with_mode(Mode::WriteOnly).map_err(engine)?;
        tx.set_durability(Durability::Eventual);
        for op in batch.into_ops() {
            match op {
                Op::Put { key, value } => {
                    tx.set(namespaced(&key), value.to_vec()).map_err(engine)?
                }
                Op::Delete { key } => tx.delete(namespaced(&key)).map_err(engine)?,
            }
        }

        tx.commit().await.map_err(engine)
    }

    fn checkpoint(&self, dir: &Path) -> Result<Checkpoint, Error> {
        self.ensure_open()?;
        self.tree.create_checkpoint(dir).map_err(engine)?;
        Checkpoint::scan(dir)
    }

    async fn close(&self) -> Result<(), Error> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.tree.close().await.map_err(engine)
    }
}

struct Cursor {
    tx: Transaction,
    next: Option<Vec<u8>>,
}

impl Snapshot for Cursor {
    fn next_page(&mut self, max: usize) -> Result<Vec<(Bytes, Bytes)>, Error> {
        let Some(start) = self.next.take() else {
            return Ok(Vec::new());
        };
        let mut iter = self.tx.range(start.as_slice(), &END[..]).map_err(engine)?;

        let mut page = Vec::with_capacity(max.min(1024));
        let mut valid = iter.seek_first().map_err(engine)?;
        while valid && page.len() < max {
            let key = iter.key().user_key();
            page.push((
                Bytes::copy_from_slice(&key[1..]),
                Bytes::from(iter.value().map_err(engine)?),
            ));
            valid = iter.next().map_err(engine)?;
        }

        if valid {
            // last_key ++ [0] is the smallest namespaced key strictly after it.
            let mut after = namespaced(&page.last().expect("valid implies a row").0);
            after.push(0);
            self.next = Some(after);
        }

        Ok(page)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SurrealEngine;

impl Engine for SurrealEngine {
    fn open(&self, path: &Path, options: Options) -> Result<Box<dyn Store>, Error> {
        Ok(Box::new(SurrealStore::open(path, options)?))
    }

    fn restore(
        &self,
        path: &Path,
        checkpoint: &Path,
        options: Options,
    ) -> Result<Box<dyn Store>, Error> {
        Ok(Box::new(SurrealStore::restore(path, checkpoint, options)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successor_carries_into_the_namespace_end() {
        assert_eq!(successor(vec![0, 1, 2]), vec![0, 1, 3]);
        assert_eq!(successor(vec![0, 1, 0xff]), vec![0, 2]);
        assert_eq!(successor(vec![0, 0xff, 0xff]), vec![1]);
        assert_eq!(successor(vec![0]), vec![1]);
    }
}
