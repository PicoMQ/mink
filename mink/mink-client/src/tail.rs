//! A long-poll tail of one bucket that follows the leader across interruptions.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use mink_table::Bucket;

use crate::connection::Handle;
use crate::scan::{Batch, decode};
use crate::{Connection, Error, Table, proto};

pub struct Tail {
    frames: BoxStream<'static, Result<Batch, Error>>,
    next_offset: Arc<AtomicI64>,
}

struct Open {
    _handle: Handle,
    frames: BoxStream<'static, Result<(Option<RecordBatch>, Bytes), Error>>,
}

impl Open {
    async fn open(
        connection: Connection,
        bucket: Bucket,
        offset: i64,
        columns: Option<Vec<usize>>,
    ) -> Result<Self, Error> {
        let (handle, frames) = connection
            .exchange(&proto::Tail {
                bucket,
                offset,
                columns,
                max_wait_ms: None,
                min_bytes: None,
            })
            .await?;
        let mut frames = decode(frames);
        match frames.next().await {
            Some(Ok((None, _))) => {}
            Some(Ok((Some(_), _))) => {
                return Err(Error::Protocol("tail sent rows before its schema".into()));
            }
            Some(Err(e)) => return Err(e),
            None => {
                return Err(Error::Protocol("tail closed before its schema".into()));
            }
        }

        Ok(Open {
            _handle: handle,
            frames,
        })
    }
}

impl Tail {
    pub(crate) async fn open(
        table: Table,
        bucket: Bucket,
        offset: i64,
        columns: Option<Vec<usize>>,
    ) -> Result<Self, Error> {
        let open = Self::connect(&table, bucket, offset, columns.clone()).await?;
        let next_offset = Arc::new(AtomicI64::new(offset));
        let position = next_offset.clone();
        let frames = async_stream::try_stream! {
            let mut open = open;
            loop {
                let error = loop {
                    match open.frames.next().await {
                        Some(Ok((Some(rows), meta))) => {
                            let meta: proto::ScanBatch = serde_json::from_slice(&meta)?;
                            if rows.num_rows() > 0 {
                                position.store(meta.last_offset + 1, Ordering::Release);
                            }
                            yield Batch { rows, meta };
                        }
                        Some(Ok((None, _))) => {}
                        Some(Err(e)) if e.is_retriable() => break Some(e),
                        Some(Err(e)) => Err(e)?,
                        // A clean hang-up means the node is going away; follow the bucket.
                        None => break None,
                    }
                };
                if let Some(error) = &error {
                    tracing::debug!(?bucket, %error, "tail interrupted; following the leader");
                }
                let from = position.load(Ordering::Acquire);
                open = Self::connect(&table, bucket, from, columns.clone()).await?;
            }
        };

        Ok(Tail {
            frames: Box::pin(frames),
            next_offset,
        })
    }

    async fn connect(
        table: &Table,
        bucket: Bucket,
        offset: i64,
        columns: Option<Vec<usize>>,
    ) -> Result<Open, Error> {
        table
            .cluster()
            .with_leader(table.path(), bucket, |connection| {
                let columns = columns.clone();
                async move { Open::open(connection, bucket, offset, columns).await }
            })
            .await
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset.load(Ordering::Acquire)
    }
}

impl Stream for Tail {
    type Item = Result<Batch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.frames.as_mut().poll_next(cx)
    }
}
