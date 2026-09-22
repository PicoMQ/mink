//! The store, snapshot cursor and engine interfaces every key-value backend implements.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use crate::batch::Batch;
use crate::checkpoint::Checkpoint;
use crate::{Error, Options};

#[async_trait]
pub trait Store: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Error>;

    fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>, Error>;

    fn prefix_lookup(&self, prefix: &[u8]) -> Result<Vec<Bytes>, Error>;

    fn limit_scan(&self, limit: usize) -> Result<Vec<Bytes>, Error>;

    fn snapshot(&self) -> Result<Box<dyn Snapshot>, Error>;

    async fn write(&self, batch: Batch) -> Result<(), Error>;

    fn checkpoint(&self, dir: &Path) -> Result<Checkpoint, Error>;

    async fn close(&self) -> Result<(), Error>;
}

pub trait Snapshot: Send {
    fn next_page(&mut self, max: usize) -> Result<Vec<(Bytes, Bytes)>, Error>;
}

pub trait Engine: Send + Sync {
    fn open(&self, path: &Path, options: Options) -> Result<Box<dyn Store>, Error>;

    fn restore(
        &self,
        path: &Path,
        checkpoint: &Path,
        options: Options,
    ) -> Result<Box<dyn Store>, Error>;
}
