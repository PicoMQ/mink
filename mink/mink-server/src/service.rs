//! The request-facing operations on hosted buckets, with leadership checks and long-poll reads.

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mink_log::{AppendInfo, FetchInfo, FetchIsolation};
use mink_metadata::View;
use mink_record::Projection;
use mink_table::{Bucket, BucketId, Descriptor, Id, Path};
use mink_tablet::{Put, Scan, Tablet};
use tokio::time::Instant;

use crate::error::Error;
use crate::node::Node;
use crate::registry::Hosted;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    Local,
    Remote { node_id: i32, address: String },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_id: Id,
    pub path: Path,
    pub descriptor: Descriptor,
    pub leaders: Vec<(BucketId, Option<i32>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    Earliest,
    Latest,
    Timestamp(i64),
}

#[derive(Clone)]
pub struct Service {
    node: Arc<Node>,
}

impl Service {
    pub fn new(node: Arc<Node>) -> Self {
        Service { node }
    }

    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn view(&self) -> Arc<View> {
        self.node.metadata().views().load()
    }

    pub fn table(&self, path: &Path) -> Result<TableInfo, Error> {
        let view = self.view();
        let row = view
            .state
            .catalog
            .tables
            .get(path)
            .ok_or_else(|| Error::TableNotExist(path.clone()))?;
        let leaders = view
            .state
            .catalog
            .buckets_of(row.table_id, None)
            .map(|(bucket, row)| (bucket.bucket(), (row.leader >= 0).then_some(row.leader)))
            .collect();

        Ok(TableInfo {
            table_id: row.table_id,
            path: path.clone(),
            descriptor: row.descriptor.clone(),
            leaders,
        })
    }

    pub fn owner(&self, bucket: Bucket) -> Owner {
        if self.node.registry().contains(bucket) {
            return Owner::Local;
        }
        let view = self.view();
        let Some(row) = view.state.catalog.buckets.get(&bucket) else {
            return Owner::Unknown;
        };
        if row.leader == self.node.node_id() {
            return Owner::Local;
        }
        match view.state.nodes.get(&row.leader) {
            Some(node) => Owner::Remote {
                node_id: row.leader,
                address: node.http_address.clone(),
            },
            None => Owner::Unknown,
        }
    }

    pub async fn init_writer(&self) -> Result<i64, Error> {
        Ok(self.node.metadata().allocate_producer_ids(1).await? as i64)
    }

    pub async fn append(&self, bucket: Bucket, records: Bytes) -> Result<AppendInfo, Error> {
        let hosted = self.leader(bucket)?;
        if hosted.kv.is_some() {
            return Err(Error::NotLogTable(bucket));
        }

        Ok(hosted.log.append(records).await?)
    }

    pub async fn fetch(
        &self,
        bucket: Bucket,
        offset: i64,
        max_bytes: usize,
        projection: Option<&Projection>,
    ) -> Result<FetchInfo, Error> {
        let hosted = self.leader(bucket)?;
        Ok(hosted
            .log
            .read(offset, max_bytes, FetchIsolation::HighWatermark, projection)
            .await?)
    }

    pub async fn fetch_wait(
        &self,
        bucket: Bucket,
        offset: i64,
        max_bytes: usize,
        min_bytes: usize,
        max_wait: Duration,
        projection: Option<&Projection>,
    ) -> Result<FetchInfo, Error> {
        let hosted = self.leader(bucket)?;
        let read = |hosted: Arc<Hosted>| async move {
            hosted
                .log
                .read(offset, max_bytes, FetchIsolation::HighWatermark, projection)
                .await
        };
        let info = read(hosted.clone()).await?;
        if info.size() >= min_bytes.max(1) || max_wait.is_zero() {
            return Ok(info);
        }

        let deadline = Instant::now() + max_wait;
        let mut next = info.fetch_offset;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(info);
            }
            let high_watermark = hosted.log.wait_past(next, remaining).await;
            if high_watermark <= next {
                return Ok(info);
            }
            let again = read(hosted.clone()).await?;
            if again.size() >= min_bytes.max(1) {
                return Ok(again);
            }
            next = high_watermark;
        }
    }

    pub async fn list_offset(&self, bucket: Bucket, spec: OffsetSpec) -> Result<i64, Error> {
        let hosted = self.leader(bucket)?;
        Ok(match spec {
            OffsetSpec::Earliest => hosted.log.log_start_offset(),
            OffsetSpec::Latest => hosted.log.high_watermark(),
            OffsetSpec::Timestamp(ts) => hosted.log.offset_for_timestamp(ts).await?,
        })
    }

    pub async fn put(&self, bucket: Bucket, put: Put) -> Result<AppendInfo, Error> {
        Ok(self.kv(bucket)?.put(put).await?)
    }

    pub fn lookup(&self, bucket: Bucket, key: &[u8]) -> Result<Option<Bytes>, Error> {
        Ok(self.kv(bucket)?.lookup(key)?)
    }

    pub fn multi_lookup(
        &self,
        bucket: Bucket,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Bytes>>, Error> {
        Ok(self.kv(bucket)?.multi_lookup(keys)?)
    }

    pub fn prefix_lookup(&self, bucket: Bucket, prefix: &[u8]) -> Result<Vec<Bytes>, Error> {
        Ok(self.kv(bucket)?.prefix_lookup(prefix)?)
    }

    pub async fn snapshot_scan(&self, bucket: Bucket) -> Result<Scan, Error> {
        Ok(self.kv(bucket)?.snapshot_scan().await?)
    }

    pub async fn limit_scan(&self, bucket: Bucket, limit: usize) -> Result<LimitScan, Error> {
        let hosted = self.leader(bucket)?;
        match hosted.kv() {
            Some(kv) => Ok(LimitScan::Rows(kv.limit_scan(limit)?)),
            None => {
                let offsets = hosted.log.offsets();
                let from = offsets.log_start.max(offsets.high_watermark - limit as i64);
                let info = hosted
                    .log
                    .read(from, usize::MAX, FetchIsolation::HighWatermark, None)
                    .await?;
                Ok(LimitScan::Batches(info.batches))
            }
        }
    }

    pub fn leader_check(&self, bucket: Bucket) -> Result<(), Error> {
        self.leader(bucket).map(drop)
    }

    fn leader(&self, bucket: Bucket) -> Result<Arc<Hosted>, Error> {
        if let Some(hosted) = self.node.registry().get(bucket) {
            return Ok(hosted);
        }
        if self.node.registry().is_pending(bucket) {
            return Err(Error::Unavailable(bucket));
        }

        let view = self.view();
        match view.state.catalog.buckets.get(&bucket) {
            None => Err(Error::BucketNotExist(bucket)),
            Some(row) if row.leader == self.node.node_id() => Err(Error::Unavailable(bucket)),
            Some(row) => Err(Error::NotLeader {
                bucket,
                leader: (row.leader >= 0).then_some(row.leader),
            }),
        }
    }

    fn kv(&self, bucket: Bucket) -> Result<KvHosted, Error> {
        let hosted = self.leader(bucket)?;
        if hosted.kv.is_none() {
            return Err(Error::NotKvTable(bucket));
        }

        Ok(KvHosted(hosted))
    }
}

struct KvHosted(Arc<Hosted>);

impl Deref for KvHosted {
    type Target = Tablet;

    fn deref(&self) -> &Tablet {
        self.0.kv().expect("checked by Service::kv")
    }
}

#[derive(Debug, Clone)]
pub enum LimitScan {
    Rows(Vec<Bytes>),
    Batches(Vec<Bytes>),
}
