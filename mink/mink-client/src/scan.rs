//! Bounded scans, snapshot reads and union reads of one bucket as decoded batch streams.

use arrow_array::RecordBatch;
use arrow_flight::FlightData;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use mink_table::Bucket;
use tonic::Streaming;

use crate::proto::{self, Read, action};
use crate::{Connection, Error};

#[derive(Debug, Clone)]
pub struct Batch {
    pub rows: RecordBatch,
    pub meta: proto::ScanBatch,
}

impl Batch {
    pub fn last_offset(&self) -> i64 {
        self.meta.last_offset
    }
}

pub struct Snapshot {
    pub log_offset: i64,
    pub batches: BoxStream<'static, Result<RecordBatch, Error>>,
}

fn clip(rows: RecordBatch, mut meta: proto::ScanBatch, from: i64, to: i64) -> Result<Batch, Error> {
    let first = meta.base_offset.max(from);
    let last = meta.last_offset.min(to - 1);
    if first == meta.base_offset && last == meta.last_offset {
        return Ok(Batch { rows, meta });
    }
    let span = (meta.last_offset - meta.base_offset + 1) as usize;
    if rows.num_rows() != span {
        return Err(Error::Protocol(format!(
            "batch {}..={} carries {} rows",
            meta.base_offset,
            meta.last_offset,
            rows.num_rows()
        )));
    }
    if last < first {
        meta.base_offset = first;
        meta.last_offset = first - 1;
        meta.changes = meta.changes.map(|_| Vec::new());
        return Ok(Batch {
            rows: rows.slice(0, 0),
            meta,
        });
    }
    let skip = (first - meta.base_offset) as usize;
    let take = (last - first + 1) as usize;
    let rows = rows.slice(skip, take);
    if let Some(changes) = meta.changes.as_mut() {
        *changes = changes[skip..skip + take].to_vec();
    }
    meta.base_offset = first;
    meta.last_offset = last;

    Ok(Batch { rows, meta })
}

impl Connection {
    pub async fn list_offset(&self, bucket: Bucket, spec: proto::OffsetSpec) -> Result<i64, Error> {
        let offset: proto::Offset = self
            .action_one(action::LIST_OFFSETS, &proto::ListOffsets { bucket, spec })
            .await?;

        Ok(offset.offset)
    }

    pub async fn offsets(&self, bucket: Bucket) -> Result<(i64, i64), Error> {
        Ok((
            self.list_offset(bucket, proto::OffsetSpec::Earliest)
                .await?,
            self.list_offset(bucket, proto::OffsetSpec::Latest).await?,
        ))
    }

    pub fn scan(
        self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
    ) -> BoxStream<'static, Result<Batch, Error>> {
        self.scan_opened(bucket, from, to, columns, None)
    }

    pub(crate) async fn open_scan(
        &self,
        bucket: Bucket,
        offset: i64,
        columns: Option<Vec<usize>>,
    ) -> Result<Streaming<FlightData>, Error> {
        self.get(&Read::Scan {
            bucket,
            offset,
            max_bytes: None,
            columns,
        })
        .await
    }

    pub(crate) fn scan_opened(
        self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
        opened: Option<Streaming<FlightData>>,
    ) -> BoxStream<'static, Result<Batch, Error>> {
        Box::pin(async_stream::try_stream! {
            let mut next = from;
            let mut opened = opened;
            while next < to {
                let response = match opened.take() {
                    Some(response) => response,
                    None => self.open_scan(bucket, next, columns.clone()).await?,
                };
                let mut frames = decode(response);
                let mut progressed = false;
                while let Some(frame) = frames.next().await {
                    let (rows, meta) = frame?;
                    let Some(rows) = rows else { continue };
                    let meta: proto::ScanBatch = serde_json::from_slice(&meta)?;
                    if meta.last_offset < from {
                        continue;
                    }
                    let done = meta.last_offset >= to;
                    let batch = clip(rows, meta, from, to)?;
                    next = batch.meta.last_offset + 1;
                    progressed = true;
                    if batch.rows.num_rows() > 0 {
                        yield batch;
                    }
                    if done {
                        return;
                    }
                }
                if !progressed {
                    break;
                }
            }
        })
    }

    pub async fn snapshot(
        &self,
        bucket: Bucket,
        batch_rows: Option<usize>,
    ) -> Result<Snapshot, Error> {
        let read = Read::Snapshot {
            bucket,
            columns: None,
            batch_rows,
        };
        let mut frames = decode(self.get(&read).await?);
        let (_, meta) = frames
            .next()
            .await
            .ok_or_else(|| Error::Protocol("snapshot stream sent no schema".into()))??;
        let meta: proto::SnapshotBatch = serde_json::from_slice(&meta)?;

        Ok(Snapshot {
            log_offset: meta.log_offset,
            batches: Box::pin(frames.filter_map(|frame| async move {
                match frame {
                    Ok((Some(rows), _)) => Some(Ok(rows)),
                    Ok((None, _)) => None,
                    Err(e) => Some(Err(e)),
                }
            })),
        })
    }

    pub fn union(
        self,
        bucket: Bucket,
        columns: Option<Vec<usize>>,
    ) -> BoxStream<'static, Result<RecordBatch, Error>> {
        Box::pin(async_stream::try_stream! {
            let response = self.open_union(bucket, columns).await?;
            for await batch in union_batches(response) {
                yield batch?;
            }
        })
    }

    pub(crate) async fn open_union(
        &self,
        bucket: Bucket,
        columns: Option<Vec<usize>>,
    ) -> Result<Streaming<FlightData>, Error> {
        self.get(&Read::Union { bucket, columns }).await
    }
}

pub(crate) fn union_batches(
    response: Streaming<FlightData>,
) -> BoxStream<'static, Result<RecordBatch, Error>> {
    Box::pin(async_stream::try_stream! {
        let mut frames = decode(response);
        while let Some(frame) = frames.next().await {
            if let (Some(rows), _) = frame? {
                yield rows;
            }
        }
    })
}

pub(crate) fn decode(
    stream: Streaming<FlightData>,
) -> BoxStream<'static, Result<(Option<RecordBatch>, Bytes), Error>> {
    let decoder =
        FlightDataDecoder::new(stream.map(|r| r.map_err(|s| FlightError::Tonic(Box::new(s)))));
    Box::pin(decoder.filter_map(|frame| async move {
        match frame {
            Ok(frame) => match frame.payload {
                DecodedPayload::RecordBatch(batch) => {
                    Some(Ok((Some(batch), frame.inner.app_metadata)))
                }
                DecodedPayload::Schema(_) => Some(Ok((None, frame.inner.app_metadata))),
                DecodedPayload::None => None,
            },
            Err(FlightError::Tonic(status)) => Some(Err(Error::Status(*status))),
            Err(e) => Some(Err(Error::Flight(e))),
        }
    }))
}
