//! The set of buckets a node hosts, each with its log, optional key-value tablet and retention frontier.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use mink_common::sync::{lock, read, write};
use mink_table::{Bucket, Descriptor, Path};
use mink_tablet::{Tablet, Uploader};
use tokio::sync::Mutex as AsyncMutex;

pub struct Kv {
    pub tablet: Tablet,
    pub dir: PathBuf,
    pub(crate) snapshots: AsyncMutex<SnapshotState>,
}

pub(crate) struct SnapshotState {
    pub uploader: Uploader,
    pub log_offset: i64,
}

pub struct Hosted {
    pub bucket: Bucket,
    pub path: Path,
    pub descriptor: Arc<Descriptor>,
    pub stream_id: u64,
    pub leader_epoch: i32,
    pub log: Arc<mink_log::Tablet>,
    pub kv: Option<Kv>,
    pub retention: Mutex<Option<RetentionFrontier>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionFrontier {
    pub offset: i64,
    pub timestamp: Option<i64>,
}

impl Hosted {
    pub fn kv(&self) -> Option<&Tablet> {
        self.kv.as_ref().map(|kv| &kv.tablet)
    }
}

#[derive(Default)]
pub struct Registry {
    hosted: RwLock<HashMap<Bucket, Arc<Hosted>>>,
    pending: Mutex<HashSet<Bucket>>,
}

impl Registry {
    pub fn get(&self, bucket: Bucket) -> Option<Arc<Hosted>> {
        read(&self.hosted).get(&bucket).cloned()
    }

    pub fn is_pending(&self, bucket: Bucket) -> bool {
        lock(&self.pending).contains(&bucket)
    }

    pub fn contains(&self, bucket: Bucket) -> bool {
        read(&self.hosted).contains_key(&bucket)
    }

    pub fn all(&self) -> Vec<Arc<Hosted>> {
        read(&self.hosted).values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        read(&self.hosted).len()
    }

    pub fn is_empty(&self) -> bool {
        read(&self.hosted).is_empty()
    }

    pub(crate) fn begin(&self, bucket: Bucket) {
        lock(&self.pending).insert(bucket);
    }

    pub(crate) fn finish(&self, bucket: Bucket) {
        lock(&self.pending).remove(&bucket);
    }

    pub(crate) fn insert(&self, hosted: Arc<Hosted>) {
        write(&self.hosted).insert(hosted.bucket, hosted);
    }

    pub(crate) fn remove(&self, bucket: Bucket) -> Option<Arc<Hosted>> {
        write(&self.hosted).remove(&bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_sync<T: Send + Sync>() {}

    #[test]
    fn hosted_is_shareable() {
        assert_sync::<Hosted>();
        assert_sync::<Registry>();
        assert_sync::<crate::node::Node>();
    }
}
