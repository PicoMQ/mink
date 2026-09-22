//! The read-side interfaces: plan the splits of a lake snapshot once, per partition and bucket, then
//! stream the rows of each split.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use mink_table::{BucketId, PartitionName, Path};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::predicate::Predicate;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Split<F> {
    pub bucket: Option<BucketId>,
    pub partition: Option<PartitionName>,
    pub files: usize,
    pub rows: Option<u64>,
    pub bytes: u64,
    pub inner: F,
}

impl<F> Split<F> {
    pub fn map<G>(self, f: impl FnOnce(F) -> G) -> Split<G> {
        Split {
            bucket: self.bucket,
            partition: self.partition,
            files: self.files,
            rows: self.rows,
            bytes: self.bytes,
            inner: f(self.inner),
        }
    }
}

/// What a format planned for one split, handed back to the same reader to run.
#[derive(Clone)]
pub struct Tasks(Arc<dyn Any + Send + Sync>);

impl Tasks {
    pub fn new<T: Any + Send + Sync>(tasks: T) -> Self {
        Tasks(Arc::new(tasks))
    }

    pub fn downcast<T: Any>(&self) -> Result<&T> {
        self.0
            .downcast_ref()
            .ok_or_else(|| Error::Other("the split was planned by another lake reader".into()))
    }
}

impl fmt::Debug for Tasks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Tasks")
    }
}

pub type LakeSplit = Split<Tasks>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanOptions {
    pub projection: Option<Vec<usize>>,
    pub limit: Option<usize>,
}

#[async_trait]
pub trait Reader: Send + Sync {
    async fn user_schema(&self, path: &Path) -> Result<SchemaRef>;

    async fn plan(
        &self,
        path: &Path,
        snapshot_id: i64,
        filter: Option<&Predicate>,
    ) -> Result<Vec<LakeSplit>>;

    async fn read(
        &self,
        split: LakeSplit,
        options: ScanOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

#[async_trait]
pub trait Source: Send + Sync {
    type Split: Send + Sync + 'static;

    async fn plan(
        &self,
        snapshot_id: i64,
        filter: Option<&Predicate>,
    ) -> Result<Vec<Split<Self::Split>>>;

    async fn read(
        &self,
        split: Split<Self::Split>,
        options: ScanOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

pub fn take_rows<S, E>(
    rows: S,
    limit: usize,
) -> impl Stream<Item = std::result::Result<RecordBatch, E>>
where
    S: Stream<Item = std::result::Result<RecordBatch, E>>,
{
    let mut remaining = limit;
    rows.take_while(move |_| futures::future::ready(remaining > 0))
        .map_ok(move |batch| {
            let n = batch.num_rows().min(remaining);
            remaining -= n;
            batch.slice(0, n)
        })
}
