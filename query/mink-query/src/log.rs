//! The log tail of a bucket as the union reader wants it, fetched from the owning node over Flight.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use mink_client::{Batch, Table};
use mink_lake::{LogSource, TieredBatch};
use mink_record::Changes;
use mink_table::Bucket;

pub struct Log {
    table: Table,
}

impl Log {
    pub fn new(table: Table) -> Self {
        Log { table }
    }
}

#[async_trait]
impl LogSource for Log {
    async fn offsets(&self, bucket: Bucket) -> mink_lake::Result<(i64, i64)> {
        self.table
            .offsets(bucket)
            .await
            .map_err(mink_lake::Error::other)
    }

    fn log(
        &self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
    ) -> BoxStream<'static, mink_lake::Result<TieredBatch>> {
        let table = self.table.clone();
        let opened = async move {
            let batches = table
                .scan(bucket, from, to, columns)
                .await
                .map_err(mink_lake::Error::other)?;
            Ok::<_, mink_lake::Error>(
                batches.map(|batch| tiered(batch.map_err(mink_lake::Error::other)?)),
            )
        };
        Box::pin(futures::stream::once(opened).try_flatten())
    }
}

fn tiered(batch: Batch) -> mink_lake::Result<TieredBatch> {
    let changes = match batch.meta.changes {
        Some(vector) => Changes::vector(Bytes::from(vector))?,
        None => Changes::AppendOnly(batch.rows.num_rows()),
    };
    Ok(TieredBatch {
        rows: batch.rows,
        changes,
        base_offset: batch.meta.base_offset,
        timestamp_ms: batch.meta.commit_timestamp,
    })
}
