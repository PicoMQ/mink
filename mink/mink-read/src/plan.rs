//! Plans the lake position and log range of a union read and runs it as a batch stream.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use mink_lake::{LakeSplit, LogSource, Predicate, Reader, ScanOptions, TieredBatch, take_rows};
use mink_table::{Bucket, PartitionName, Path, Schema};

use crate::error::{Error, Result};
use crate::merge;
use crate::scan::{Columns, Options};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LakePosition {
    pub snapshot_id: i64,
    pub log_end_offset: i64,
}

/// One bucket's read: the lake files planned for it, where the lake ends, and the log range after it.
#[derive(Debug, Clone)]
pub struct Plan {
    pub bucket: Bucket,
    pub partition: Option<PartitionName>,
    pub lake: Option<LakePosition>,
    pub split: Option<LakeSplit>,
    pub log_from: i64,
    pub log_to: i64,
}

impl Plan {
    pub fn log_rows(&self) -> u64 {
        (self.log_to - self.log_from).max(0) as u64
    }
}

pub struct Read {
    lake: Option<Arc<dyn Reader>>,
    log: Arc<dyn LogSource>,
}

impl Read {
    pub fn new(lake: Option<Arc<dyn Reader>>, log: Arc<dyn LogSource>) -> Self {
        Read { lake, log }
    }

    fn lake(&self) -> Result<&dyn Reader> {
        self.lake.as_deref().ok_or(Error::NoLake)
    }

    pub async fn plan_lake(
        &self,
        path: &Path,
        snapshot_id: i64,
        filter: Option<&Predicate>,
    ) -> Result<Vec<LakeSplit>> {
        Ok(self.lake()?.plan(path, snapshot_id, filter).await?)
    }

    pub async fn plan(
        &self,
        bucket: Bucket,
        partition: Option<PartitionName>,
        lake: Option<LakePosition>,
        split: Option<LakeSplit>,
    ) -> Result<Plan> {
        let (log_start, high_watermark) = self.log.offsets(bucket).await?;
        let log_from = match lake {
            Some(position) => position.log_end_offset.max(log_start),
            None => log_start,
        };

        Ok(Plan {
            bucket,
            partition,
            lake,
            split,
            log_from,
            log_to: high_watermark.max(log_from),
        })
    }

    pub fn output_schema(schema: &Schema, options: &Options) -> Result<SchemaRef> {
        Ok(Columns::new(schema, options)?.output_schema)
    }

    pub async fn read_lake(
        &self,
        schema: &Schema,
        split: LakeSplit,
        options: &Options,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let columns = Columns::new(schema, options)?;
        let lake = self
            .lake()?
            .read(
                split,
                ScanOptions {
                    projection: Some(columns.read.clone()),
                    limit: options.limit,
                },
            )
            .await?;

        Ok(skip_empty(lake.map(move |batch| {
            let batch = columns.normalize(&batch?)?;
            columns.finish(batch)
        })))
    }

    pub async fn read(
        &self,
        schema: &Schema,
        plan: Plan,
        options: &Options,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let columns = Columns::new(schema, options)?;
        let keyed = schema.primary_key().is_some();
        let lake: BoxStream<'static, Result<RecordBatch>> = match plan.split {
            Some(split) => {
                let normalize = columns.clone();
                let rows = self
                    .lake()?
                    .read(
                        split,
                        ScanOptions {
                            projection: Some(columns.read.clone()),
                            limit: options.limit.filter(|_| !keyed),
                        },
                    )
                    .await?;
                Box::pin(rows.map(move |batch| normalize.normalize(&batch?)))
            }
            None => Box::pin(futures::stream::empty()),
        };
        let log = self.log.log(
            plan.bucket,
            plan.log_from,
            plan.log_to,
            Some(columns.read.clone()),
        );

        let rows = if keyed {
            let tail: Vec<TieredBatch> = log.try_collect().await?;
            merge::merged(lake, tail, columns)?
        } else {
            let normalize = columns.clone();
            let log = log.map(move |batch| normalize.normalize(&batch?.rows));
            Box::pin(lake.chain(log).map(move |batch| columns.finish(batch?)))
        };

        Ok(skip_empty(limited(rows, options.limit)))
    }
}

fn limited(
    rows: BoxStream<'static, Result<RecordBatch>>,
    limit: Option<usize>,
) -> BoxStream<'static, Result<RecordBatch>> {
    match limit {
        Some(limit) => Box::pin(take_rows(rows, limit)),
        None => rows,
    }
}

pub(crate) fn skip_empty<S>(stream: S) -> BoxStream<'static, Result<RecordBatch>>
where
    S: Stream<Item = Result<RecordBatch>> + Send + 'static,
{
    Box::pin(
        stream.filter(|b| {
            futures::future::ready(b.as_ref().map(|b| b.num_rows() > 0).unwrap_or(true))
        }),
    )
}
