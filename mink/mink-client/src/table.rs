//! A table handle: schema, buckets, partition resolution, and the entry points to writers and readers.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use futures::StreamExt;
use futures::stream::BoxStream;
use mink_common::sync::{read, write};
use mink_table::{
    Bucket, BucketId, Descriptor, PartitionId, PartitionName, PartitionSpec, Path, SchemaId,
};

use crate::scan::{Batch, Snapshot, union_batches};
use crate::tail::Tail;
use crate::write::{Append, Upsert};
use crate::{Admin, Cluster, Error, Lookup, proto};

#[derive(Clone)]
pub struct Table {
    inner: Arc<Inner>,
}

struct Inner {
    cluster: Cluster,
    path: Path,
    info: Arc<proto::TableInfo>,
    partitions: RwLock<HashMap<PartitionName, PartitionId>>,
}

impl Table {
    pub(crate) async fn open(cluster: Cluster, path: &Path) -> Result<Self, Error> {
        let info = cluster.table_info(path).await?;
        Ok(Table {
            inner: Arc::new(Inner {
                cluster,
                path: path.clone(),
                info,
                partitions: RwLock::new(HashMap::new()),
            }),
        })
    }

    pub fn cluster(&self) -> &Cluster {
        &self.inner.cluster
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn info(&self) -> &proto::TableInfo {
        &self.inner.info
    }

    pub fn descriptor(&self) -> &Descriptor {
        &self.inner.info.descriptor
    }

    pub fn schema_id(&self) -> SchemaId {
        SchemaId((self.inner.info.schemas.len() - 1) as u32)
    }

    pub fn schema(&self) -> &mink_table::Schema {
        &self.inner.info.schemas[self.schema_id().0 as usize]
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        Arc::new(Schema::from(self.schema().fields()))
    }

    pub fn buckets(&self) -> impl Iterator<Item = Bucket> + '_ {
        self.inner.info.buckets.iter().map(|b| b.bucket)
    }

    pub async fn append_writer(&self) -> Result<Append, Error> {
        Append::new(self.clone()).await
    }

    pub async fn upsert_writer(&self) -> Result<Upsert, Error> {
        Upsert::new(self.clone(), None).await
    }

    pub async fn partial_update_writer(&self, columns: Vec<usize>) -> Result<Upsert, Error> {
        Upsert::new(self.clone(), Some(columns)).await
    }

    pub fn lookuper(&self) -> Result<Lookup, Error> {
        Lookup::new(self.clone())
    }

    pub async fn offsets(&self, bucket: Bucket) -> Result<(i64, i64), Error> {
        self.inner
            .cluster
            .with_leader(&self.inner.path, bucket, |connection| async move {
                connection.offsets(bucket).await
            })
            .await
    }

    pub async fn list_offset(&self, bucket: Bucket, spec: proto::OffsetSpec) -> Result<i64, Error> {
        self.inner
            .cluster
            .with_leader(&self.inner.path, bucket, |connection| async move {
                connection.list_offset(bucket, spec).await
            })
            .await
    }

    pub async fn scan(
        &self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
    ) -> Result<BoxStream<'static, Result<Batch, Error>>, Error> {
        if from >= to {
            return Ok(Box::pin(futures::stream::empty()));
        }
        let opening = columns.clone();
        let (leader, opened) = self
            .inner
            .cluster
            .with_leader(&self.inner.path, bucket, |connection| {
                let columns = opening.clone();
                async move {
                    let opened = connection.open_scan(bucket, from, columns).await?;
                    Ok((connection, opened))
                }
            })
            .await?;

        Ok(leader.scan_opened(bucket, from, to, columns, Some(opened)))
    }

    pub async fn snapshot(
        &self,
        bucket: Bucket,
        batch_rows: Option<usize>,
    ) -> Result<Snapshot, Error> {
        self.inner
            .cluster
            .with_leader(&self.inner.path, bucket, |connection| async move {
                connection.snapshot(bucket, batch_rows).await
            })
            .await
    }

    pub async fn union(
        &self,
        bucket: Bucket,
        columns: Option<Vec<usize>>,
    ) -> Result<BoxStream<'static, Result<RecordBatch, Error>>, Error> {
        let opened = self
            .inner
            .cluster
            .with_leader(&self.inner.path, bucket, |connection| {
                let columns = columns.clone();
                async move { connection.open_union(bucket, columns).await }
            })
            .await?;
        Ok(union_batches(opened))
    }

    pub async fn union_all(
        &self,
        columns: Option<Vec<usize>>,
    ) -> Result<BoxStream<'static, Result<RecordBatch, Error>>, Error> {
        let cluster = self.inner.cluster.clone();
        let info = cluster.any()?.flight_info(&self.inner.path).await?;
        let mut streams = Vec::with_capacity(info.endpoint.len());
        for endpoint in info.endpoint {
            let ticket = endpoint
                .ticket
                .ok_or_else(|| Error::Protocol("flight endpoint without a ticket".into()))?;
            let mut read: proto::Read = serde_json::from_slice(&ticket.ticket)?;
            read.project(columns.clone());
            let connection = match endpoint.location.first() {
                Some(location) => cluster.connection(&location.uri)?,
                None => cluster.any()?,
            };
            streams.push(union_batches(connection.get(&read).await?));
        }

        Ok(Box::pin(futures::stream::iter(streams).flatten()))
    }

    pub async fn tail(
        &self,
        bucket: Bucket,
        offset: i64,
        columns: Option<Vec<usize>>,
    ) -> Result<Tail, Error> {
        Tail::open(self.clone(), bucket, offset, columns).await
    }

    pub(crate) async fn resolve(
        &self,
        partition: Option<&PartitionName>,
        bucket: BucketId,
    ) -> Result<Bucket, Error> {
        let id = self.inner.info.table_id;
        let Some(name) = partition else {
            return Ok(Bucket::new(id, bucket));
        };
        if let Some(partition) = read(&self.inner.partitions).get(name) {
            return Ok(Bucket::partitioned(id, *partition, bucket));
        }
        let admin = self.inner.cluster.admin();
        let mut partition_id = self.load(&admin).await?.get(name).copied();
        if partition_id.is_none() {
            let spec = PartitionSpec::from_name(self.descriptor().partition_keys(), name)?;
            admin
                .create_partition(&self.inner.path, &spec, true)
                .await?;
            partition_id = self.load(&admin).await?.get(name).copied();
        }

        let partition_id = partition_id.ok_or_else(|| {
            Error::Protocol(format!(
                "partition {name} of {} did not appear",
                self.inner.path
            ))
        })?;

        Ok(Bucket::partitioned(id, partition_id, bucket))
    }

    async fn load(&self, admin: &Admin) -> Result<HashMap<PartitionName, PartitionId>, Error> {
        let partitions: HashMap<PartitionName, PartitionId> = admin
            .list_partitions(&self.inner.path)
            .await?
            .into_iter()
            .map(|p| (p.name, p.partition_id))
            .collect();
        *write(&self.inner.partitions) = partitions.clone();

        Ok(partitions)
    }
}
