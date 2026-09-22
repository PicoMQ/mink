//! The bucket source the tiering worker reads from: table layout, log ranges as tiered batches, and snapshots.

use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use futures::stream::BoxStream;
use mink_lake::{BucketSource, LogSource, Result, SnapshotRead, TableInfo, TieredBatch};
use mink_metadata::TableRow;
use mink_record::{Batch, Remap, codec as log_codec};
use mink_table::{Bucket, Path};
use mink_tablet::{Fixed, Schemas, values_to_arrow};

use crate::error::Error;
use crate::service::{OffsetSpec, Service};

const FETCH_BYTES: usize = 8 << 20;
const SNAPSHOT_ROWS: usize = 4096;

pub struct Source {
    service: Service,
}

impl Source {
    pub fn new(service: Service) -> Self {
        Source { service }
    }

    fn row(&self, bucket: Bucket) -> Result<TableRow> {
        self.service
            .view()
            .state
            .catalog
            .table_by_id(bucket.table())
            .cloned()
            .ok_or_else(|| mink_lake::Error::other(Error::BucketNotExist(bucket)))
    }
}

pub fn table(service: &Service, path: &Path) -> Option<TableInfo> {
    let view = service.view();
    let catalog = &view.state.catalog;
    let row = catalog.tables.get(path)?;
    let buckets = if row.descriptor.is_partitioned() {
        catalog
            .partitions_of(row.table_id)
            .flat_map(|partition| {
                catalog
                    .buckets_of(row.table_id, Some(partition.partition_id))
                    .map(move |(bucket, _)| (*bucket, Some(partition.name.clone())))
            })
            .collect()
    } else {
        catalog
            .buckets_of(row.table_id, None)
            .map(|(bucket, _)| (*bucket, None))
            .collect()
    };

    Some(TableInfo {
        table_id: row.table_id,
        path: path.clone(),
        descriptor: Arc::new(row.descriptor.clone()),
        buckets,
    })
}

#[async_trait]
impl LogSource for Source {
    async fn offsets(&self, bucket: Bucket) -> Result<(i64, i64)> {
        let start = self
            .service
            .list_offset(bucket, OffsetSpec::Earliest)
            .await
            .map_err(mink_lake::Error::other)?;
        let end = self
            .service
            .list_offset(bucket, OffsetSpec::Latest)
            .await
            .map_err(mink_lake::Error::other)?;

        Ok((start, end))
    }

    fn log(
        &self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
    ) -> BoxStream<'static, Result<TieredBatch>> {
        let table = match self.row(bucket) {
            Ok(table) => table,
            Err(e) => return Box::pin(futures::stream::once(async { Err(e) })),
        };
        let service = self.service.clone();
        Box::pin(async_stream::try_stream! {
            let schemas = Fixed::new(table.schemas.clone());
            let (_, latest) = schemas.latest();
            let codec = log_codec(table.descriptor.options().log_format, Default::default());
            let mut next = from;
            while next < to {
                let fetched = service
                    .fetch(bucket, next, FETCH_BYTES, None)
                    .await
                    .map_err(mink_lake::Error::other)?;
                if fetched.batches.is_empty() {
                    break;
                }
                for bytes in fetched.batches {
                    let batch = Batch::parse(bytes)?;
                    let header = batch.header();
                    let last = header.base_offset + i64::from(header.last_offset_delta);
                    if last >= to {
                        return;
                    }
                    next = last + 1;
                    let schema = schemas
                        .get(header.schema_id)
                        .ok_or(mink_tablet::Error::SchemaNotExist(header.schema_id))
                        .map_err(mink_lake::Error::other)?;
                    let arrow = Arc::new(Schema::from(schema.fields()));
                    let remap = Remap::new(schema.fields(), latest.fields());
                    let remap = match &columns {
                        Some(columns) => remap.select(columns)?,
                        None => remap,
                    };
                    let decode = remap.decode_columns();
                    let records = batch.records(codec.as_ref(), arrow, decode.as_deref())?;
                    yield TieredBatch {
                        rows: remap.batch(records.batch)?,
                        changes: records.changes,
                        base_offset: header.base_offset,
                        timestamp_ms: header.commit_timestamp,
                    };
                }
            }
        })
    }
}

#[async_trait]
impl BucketSource for Source {
    fn table(&self, path: &Path) -> Result<Option<TableInfo>> {
        Ok(table(&self.service, path))
    }

    async fn snapshot(&self, bucket: Bucket) -> Result<SnapshotRead> {
        let table = self.row(bucket)?;
        let scan = self
            .service
            .snapshot_scan(bucket)
            .await
            .map_err(mink_lake::Error::other)?;
        let log_offset = scan.log_offset;
        let batches = Box::pin(async_stream::try_stream! {
            let schemas = Fixed::new(table.schemas.clone());
            let (_, latest) = schemas.latest();
            let format = table.descriptor.options().kv_format;
            let mut rows = scan.rows;
            loop {
                let (page, cursor) = tokio::task::spawn_blocking(move || {
                    let page = rows.next_page(SNAPSHOT_ROWS);
                    (page, rows)
                })
                .await
                .map_err(mink_lake::Error::other)?;
                rows = cursor;
                let page = page.map_err(mink_tablet::Error::from).map_err(mink_lake::Error::other)?;
                if page.is_empty() {
                    break;
                }
                let values = page.into_iter().map(|(_, v)| v);
                let batch = values_to_arrow(values, latest.fields(), format, |id| schemas.get(id));
                yield TieredBatch::snapshot(batch.map_err(mink_lake::Error::other)?);
            }
        });

        Ok(SnapshotRead {
            log_offset,
            batches,
        })
    }
}
