//! Accumulates operations into batches and flushes them when a count or byte threshold is reached.

use std::mem;

use bytes::Bytes;

use crate::Error;
use crate::batch::Batch;
use crate::store::Store;

pub const BATCH_CAPACITY: usize = 500;

pub struct Writer<'a> {
    store: &'a dyn Store,
    batch: Batch,
    max_bytes: usize,
}

impl<'a> Writer<'a> {
    pub fn new(store: &'a dyn Store, max_bytes: usize) -> Self {
        Writer {
            store,
            batch: Batch::new(),
            max_bytes,
        }
    }

    pub async fn put(
        &mut self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<(), Error> {
        self.batch.put(key, value);
        self.flush_if_needed().await
    }

    pub async fn delete(&mut self, key: impl Into<Bytes>) -> Result<(), Error> {
        self.batch.delete(key);
        self.flush_if_needed().await
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let batch = mem::take(&mut self.batch);
        self.store.write(batch).await
    }

    pub fn pending(&self) -> usize {
        self.batch.len()
    }

    pub async fn close(mut self) -> Result<(), Error> {
        self.flush().await
    }

    async fn flush_if_needed(&mut self) -> Result<(), Error> {
        let full = self.batch.len() >= BATCH_CAPACITY
            || (self.max_bytes > 0 && self.batch.size() >= self.max_bytes);
        if full { self.flush().await } else { Ok(()) }
    }
}
