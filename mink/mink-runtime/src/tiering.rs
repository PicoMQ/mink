//! Bucket source for tiering that reads local buckets directly and remote ones through the client.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use mink_client::Connection;
use mink_common::sync::lock;
use mink_lake::{BucketSource, Error, LogSource, Result, SnapshotRead, TableInfo, TieredBatch};
use mink_record::Changes;
use mink_server::{Owner, Service};
use mink_table::{Bucket, Path};

const SNAPSHOT_ROWS: usize = 4096;

pub(crate) struct Source {
    service: Service,
    local: mink_server::Source,
    connections: Mutex<HashMap<String, Connection>>,
}

impl Source {
    pub(crate) fn new(service: Service) -> Self {
        Source {
            local: mink_server::Source::new(service.clone()),
            service,
            connections: Mutex::new(HashMap::new()),
        }
    }

    fn remote(&self, bucket: Bucket) -> Result<Option<Connection>> {
        match self.service.owner(bucket) {
            Owner::Local => Ok(None),
            Owner::Remote { address, .. } => {
                let mut connections = lock(&self.connections);
                if let Some(connection) = connections.get(&address) {
                    return Ok(Some(connection.clone()));
                }

                let connection = Connection::new(&address).map_err(Error::other)?;
                connections.insert(address, connection.clone());

                Ok(Some(connection))
            }
            Owner::Unknown => Err(Error::Other(format!("{bucket:?} has no leader"))),
        }
    }
}

#[async_trait]
impl LogSource for Source {
    async fn offsets(&self, bucket: Bucket) -> Result<(i64, i64)> {
        match self.remote(bucket)? {
            None => self.local.offsets(bucket).await,
            Some(connection) => connection.offsets(bucket).await.map_err(Error::other),
        }
    }

    fn log(
        &self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
    ) -> BoxStream<'static, Result<TieredBatch>> {
        match self.remote(bucket) {
            Err(e) => Box::pin(futures::stream::once(async { Err(e) })),
            Ok(None) => self.local.log(bucket, from, to, columns),
            Ok(Some(connection)) => {
                Box::pin(connection.scan(bucket, from, to, columns).map(|batch| {
                    let batch = batch.map_err(Error::other)?;
                    let changes = match batch.meta.changes {
                        Some(bytes) => Changes::Vector(bytes.into()),
                        None => Changes::AppendOnly(batch.rows.num_rows()),
                    };
                    Ok(TieredBatch {
                        rows: batch.rows,
                        changes,
                        base_offset: batch.meta.base_offset,
                        timestamp_ms: batch.meta.commit_timestamp,
                    })
                }))
            }
        }
    }
}

#[async_trait]
impl BucketSource for Source {
    fn table(&self, path: &Path) -> Result<Option<TableInfo>> {
        self.local.table(path)
    }

    async fn snapshot(&self, bucket: Bucket) -> Result<SnapshotRead> {
        match self.remote(bucket)? {
            None => self.local.snapshot(bucket).await,
            Some(connection) => {
                let stream = connection
                    .snapshot(bucket, Some(SNAPSHOT_ROWS))
                    .await
                    .map_err(Error::other)?;
                Ok(SnapshotRead {
                    log_offset: stream.log_offset,
                    batches: Box::pin(
                        stream
                            .batches
                            .map(|rows| rows.map(TieredBatch::snapshot).map_err(Error::other)),
                    ),
                })
            }
        }
    }
}
