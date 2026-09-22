//! The read paths served over DoGet and DoExchange: bounded scans, key-value snapshots, union reads,
//! limit scans and long-poll tails, each streamed as schema frame then batch frames.

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::FlightData;
use bytes::Bytes;
use futures::StreamExt;
use mink_read::{LakePosition, Options, Read};
use mink_record::Header;
use mink_server::{LimitScan, OffsetSpec};
use mink_table::{Bucket, PartitionName, Path};
use tonic::Streaming;

use crate::codec::{self, LogFrames, Schemas};
use crate::error::Error;
use crate::proto::{self, Tail};
use crate::service::{BoxStream, Flight, decode, table_at};

impl Flight {
    pub(crate) fn scan(
        &self,
        bucket: Bucket,
        offset: i64,
        max_bytes: Option<usize>,
        columns: Option<Vec<usize>>,
    ) -> Result<BoxStream<FlightData>, Error> {
        let view = self.inner.service.view();
        let table = self.table(&view, bucket)?;
        let budget = max_bytes.unwrap_or(self.inner.config.default_scan_bytes);

        Ok(self.spawn_stream(move |this, tx| async move {
            let service = &this.inner.service;
            let end = service.list_offset(bucket, OffsetSpec::Latest).await?;
            let frames = LogFrames {
                schemas: Schemas::new(&table.schemas),
                format: table.descriptor.options().log_format,
                columns: columns.as_deref(),
                high_watermark: end,
            };
            if tx
                .send(Ok(codec::schema_frame(&frames.schema()?)))
                .await
                .is_err()
            {
                return Ok(());
            }
            let mut next = offset;
            let mut remaining = budget;
            while next < end && remaining > 0 {
                let fetched = service.fetch(bucket, next, remaining, None).await?;
                if fetched.batches.is_empty() {
                    break;
                }
                for bytes in fetched.batches {
                    remaining = remaining.saturating_sub(bytes.len());
                    let header = Header::read(&bytes)?;
                    next = header.next_offset();
                    if tx.send(Ok(frames.frame(bytes)?)).await.is_err() {
                        return Ok(());
                    }
                }
            }

            Ok(())
        }))
    }

    pub(crate) async fn snapshot(
        &self,
        bucket: Bucket,
        columns: Option<Vec<usize>>,
        batch_rows: Option<usize>,
    ) -> Result<BoxStream<FlightData>, Error> {
        let view = self.inner.service.view();
        let table = self.table(&view, bucket)?;
        let scan = self.inner.service.snapshot_scan(bucket).await?;
        let page = batch_rows
            .unwrap_or(self.inner.config.default_snapshot_rows)
            .max(1);
        Ok(self.spawn_stream(move |_, tx| async move {
            let schemas = Schemas::new(&table.schemas);
            let format = table.descriptor.options().kv_format;
            let schema = codec::projected(&codec::arrow(schemas.latest().1), columns.as_deref())?;
            let metadata = Bytes::from(serde_json::to_vec(&proto::SnapshotBatch {
                log_offset: scan.log_offset,
            })?);
            let mut first = codec::schema_frame(&schema);
            first.app_metadata = metadata.clone();
            if tx.send(Ok(first)).await.is_err() {
                return Ok(());
            }
            let mut rows = scan.rows;
            loop {
                let (page_rows, cursor) = tokio::task::spawn_blocking(move || {
                    let page = rows.next_page(page);
                    (page, rows)
                })
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
                rows = cursor;
                let page_rows = page_rows.map_err(mink_tablet::Error::from)?;
                if page_rows.is_empty() {
                    return Ok(());
                }
                let values: Vec<Bytes> = page_rows.into_iter().map(|(_, v)| v).collect();
                let batch = codec::rows(&values, &schemas, format, columns.as_deref())?;
                if tx
                    .send(Ok(codec::batch_frame(&batch, metadata.clone())?))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
        }))
    }

    pub(crate) async fn union(
        &self,
        bucket: Bucket,
        columns: Option<Vec<usize>>,
    ) -> Result<BoxStream<FlightData>, Error> {
        let union = self
            .inner
            .union
            .clone()
            .ok_or_else(|| Error::Request("this cluster has no lake to union with".into()))?;
        self.inner.service.leader_check(bucket)?;
        let view = self.inner.service.view();
        let table = self.table(&view, bucket)?;
        if table.descriptor.bucket_keys().is_empty() {
            return Err(Error::Request(
                "keyless log tables are not bucketed in the lake; read the lake per partition \
                 with Read::Lake and the log tail with Read::Scan, as GetFlightInfo lays out"
                    .into(),
            ));
        }
        let path = view
            .state
            .catalog
            .table_paths
            .get(&table.table_id)
            .cloned()
            .ok_or(Error::Server(mink_server::Error::BucketNotExist(bucket)))?;
        let partition = bucket
            .partition()
            .and_then(|id| view.state.catalog.partition_names.get(&id))
            .map(|(_, name)| name.clone());
        let lake = view
            .state
            .catalog
            .lake
            .get(&table.table_id)
            .map(|row| LakePosition {
                snapshot_id: row.snapshot_id,
                log_end_offset: row.bucket_log_end_offset.get(&bucket).copied().unwrap_or(0),
            });
        drop(view);
        let schema = table.descriptor.schema().clone();
        let options = Options {
            projection: columns,
            limit: None,
        };
        let output = Read::output_schema(&schema, &options)?;
        let split = match lake {
            Some(position) => union
                .plan_lake(&path, position.snapshot_id, None)
                .await?
                .into_iter()
                .find(|s| s.bucket == Some(bucket.bucket()) && s.partition == partition),
            None => None,
        };
        let plan = union.plan(bucket, partition, lake, split).await?;
        let rows = union.read(&schema, plan, &options).await?;
        let this = self.clone();
        let frames = rows.map(move |batch| match batch {
            Ok(batch) => codec::batch_frame(&batch, Bytes::new()).map_err(|e| this.status(e)),
            Err(e) => Err(this.status(e.into())),
        });

        Ok(Box::pin(
            futures::stream::once(async move { Ok(codec::schema_frame(&output)) }).chain(frames),
        ))
    }

    pub(crate) async fn lake(
        &self,
        path: Path,
        partition: Option<PartitionName>,
        snapshot_id: i64,
        columns: Option<Vec<usize>>,
    ) -> Result<BoxStream<FlightData>, Error> {
        let union = self
            .inner
            .union
            .clone()
            .ok_or_else(|| Error::Request("this cluster has no lake to read".into()))?;
        let schema = {
            let view = self.inner.service.view();
            table_at(&view, &path)?.descriptor.schema().clone()
        };
        let options = Options {
            projection: columns,
            limit: None,
        };
        let output = Read::output_schema(&schema, &options)?;
        let splits = union
            .plan_lake(&path, snapshot_id, None)
            .await?
            .into_iter()
            .filter(|s| s.partition == partition);
        let mut streams = Vec::new();
        for split in splits {
            streams.push(union.read_lake(&schema, split, &options).await?);
        }
        let rows = futures::stream::iter(streams).flatten();
        let this = self.clone();
        let frames = rows.map(move |batch| match batch {
            Ok(batch) => codec::batch_frame(&batch, Bytes::new()).map_err(|e| this.status(e)),
            Err(e) => Err(this.status(e.into())),
        });

        Ok(Box::pin(
            futures::stream::once(async move { Ok(codec::schema_frame(&output)) }).chain(frames),
        ))
    }

    pub(crate) async fn limit_scan(
        &self,
        bucket: Bucket,
        limit: usize,
        columns: Option<Vec<usize>>,
    ) -> Result<BoxStream<FlightData>, Error> {
        let view = self.inner.service.view();
        let table = self.table(&view, bucket)?;
        let schemas = Schemas::new(&table.schemas);
        let mut frames = Vec::new();
        match self.inner.service.limit_scan(bucket, limit).await? {
            LimitScan::Rows(values) => {
                let batch = codec::rows(
                    &values,
                    &schemas,
                    table.descriptor.options().kv_format,
                    columns.as_deref(),
                )?;
                frames.push(codec::schema_frame(batch.schema_ref()));
                frames.push(codec::batch_frame(&batch, Bytes::new())?);
            }
            LimitScan::Batches(batches) => {
                let log = LogFrames {
                    schemas,
                    format: table.descriptor.options().log_format,
                    columns: columns.as_deref(),
                    high_watermark: self
                        .inner
                        .service
                        .list_offset(bucket, OffsetSpec::Latest)
                        .await?,
                };
                frames.push(codec::schema_frame(&log.schema()?));
                let mut rows = Vec::with_capacity(batches.len());
                for bytes in batches {
                    let header = Header::read(&bytes)?;
                    rows.push((header.record_count as usize, bytes));
                }
                let mut total: usize = rows.iter().map(|(n, _)| n).sum();
                let mut skip = 0;
                while total > limit && skip < rows.len() && total - rows[skip].0 >= limit {
                    total -= rows[skip].0;
                    skip += 1;
                }
                for (_, bytes) in rows.into_iter().skip(skip) {
                    frames.push(log.frame(bytes)?);
                }
            }
        }

        Ok(Box::pin(futures::stream::iter(frames.into_iter().map(Ok))))
    }

    pub(crate) fn tail(&self, mut stream: Streaming<FlightData>) -> BoxStream<FlightData> {
        self.spawn_stream(move |this, tx| async move {
            let first = match stream.message().await {
                Ok(Some(first)) => first,
                Ok(None) => return Ok(()),
                Err(status) => return Err(Error::Request(status.to_string())),
            };
            let descriptor = first
                .flight_descriptor
                .as_ref()
                .ok_or_else(|| Error::Request("DoExchange needs a descriptor".into()))?;
            let tail: Tail = decode(&descriptor.cmd)?;
            let view = this.inner.service.view();
            let table = this.table(&view, tail.bucket)?;
            drop(view);
            this.inner.service.leader_check(tail.bucket)?;
            let max_wait = Duration::from_millis(tail.max_wait_ms.unwrap_or(500));
            let min_bytes = tail.min_bytes.unwrap_or(1);
            let service = &this.inner.service;

            let mut frames = LogFrames {
                schemas: Schemas::new(&table.schemas),
                format: table.descriptor.options().log_format,
                columns: tail.columns.as_deref(),
                high_watermark: 0,
            };
            let schema = frames.schema()?;
            if tx.send(Ok(codec::schema_frame(&schema))).await.is_err() {
                return Ok(());
            }
            let mut next = tail.offset;
            loop {
                let fetched = tokio::select! {
                    closed = stream.message() => match closed {
                        Ok(Some(_)) => continue,
                        Ok(None) | Err(_) => return Ok(()),
                    },
                    fetched = service.fetch_wait(
                        tail.bucket,
                        next,
                        this.inner.config.default_scan_bytes,
                        min_bytes,
                        max_wait,
                        None,
                    ) => fetched?,
                };
                frames.high_watermark = fetched.high_watermark;
                if fetched.batches.is_empty() {
                    let meta = proto::ScanBatch {
                        base_offset: next,
                        last_offset: next - 1,
                        commit_timestamp: -1,
                        schema_id: frames.schemas.latest().0,
                        changes: None,
                        high_watermark: fetched.high_watermark,
                    };
                    let empty = RecordBatch::new_empty(schema.clone());
                    let frame = codec::batch_frame(&empty, serde_json::to_vec(&meta)?.into())?;
                    if tx.send(Ok(frame)).await.is_err() {
                        return Ok(());
                    }
                    continue;
                }
                for bytes in fetched.batches {
                    let header = Header::read(&bytes)?;
                    next = header.next_offset();
                    if tx.send(Ok(frames.frame(bytes)?)).await.is_err() {
                        return Ok(());
                    }
                }
            }
        })
    }
}
