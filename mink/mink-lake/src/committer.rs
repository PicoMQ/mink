//! The committer interface that turns write results into a lake snapshot, and the offsets it records.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use mink_table::{Bucket, Descriptor, Path};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const SNAPSHOT_OFFSETS_PROPERTY: &str = "mink-offsets";

#[derive(Debug, Clone)]
pub struct CommitterContext {
    pub path: Path,
    pub descriptor: Arc<Descriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedSnapshot {
    pub snapshot_id: i64,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    pub committed_snapshot_id: i64,
    pub readable: Option<ReadableSnapshot>,
    pub earliest_snapshot_to_keep: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadableSnapshot {
    pub snapshot_id: i64,
    pub tiered_log_end_offsets: BTreeMap<Bucket, i64>,
    pub readable_log_end_offsets: BTreeMap<Bucket, i64>,
}

impl CommitResult {
    pub fn readable(committed_snapshot_id: i64) -> Self {
        CommitResult {
            committed_snapshot_id,
            readable: None,
            earliest_snapshot_to_keep: None,
        }
    }

    pub fn committed_is_readable(&self) -> bool {
        self.readable.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketOffset {
    pub bucket: Bucket,
    pub log_end_offset: i64,
}

impl BucketOffset {
    pub fn encode(offsets: &BTreeMap<Bucket, i64>) -> Result<String> {
        let list: Vec<BucketOffset> = offsets
            .iter()
            .map(|(bucket, log_end_offset)| BucketOffset {
                bucket: *bucket,
                log_end_offset: *log_end_offset,
            })
            .collect();

        serde_json::to_string(&list).map_err(Error::other)
    }

    pub fn decode(snapshot_id: i64, text: &str) -> Result<BTreeMap<Bucket, i64>> {
        let list: Vec<BucketOffset> = serde_json::from_str(text).map_err(|e| {
            Error::Other(format!(
                "lake snapshot {snapshot_id} has an unreadable {SNAPSHOT_OFFSETS_PROPERTY}: {e}"
            ))
        })?;

        Ok(list
            .into_iter()
            .map(|o| (o.bucket, o.log_end_offset))
            .collect())
    }
}

#[async_trait]
pub trait Committer<R, C>: Send {
    async fn to_committable(&mut self, results: Vec<R>) -> Result<C>;

    fn is_empty(&self, committable: &C) -> bool;

    async fn commit(
        &mut self,
        committable: C,
        properties: BTreeMap<String, String>,
    ) -> Result<CommitResult>;

    async fn abort(&mut self, committable: C) -> Result<()>;

    async fn missing_snapshot(&mut self, known: Option<i64>) -> Result<Option<CommittedSnapshot>>;
}
