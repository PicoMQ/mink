//! One bucket's log: assigns offsets, deduplicates idempotent writers, appends to the stream,
//! serves reads up to the chosen isolation, and finds offsets by timestamp.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use mink_common::Clock;
use mink_common::sync::lock;
use mink_record::header::{self, NO_WRITER_ID};
use mink_record::{Batch, Header, Projection};
use mink_table::Bucket;
use s3stream::{AppendContext, FetchContext, PendingAppend, RecordBatch, Stream};
use tokio::sync::watch;

use crate::Error;
use crate::append::{AppendInfo, analyze};
use crate::fetch::{FetchInfo, FetchIsolation};
use crate::offset::OffsetSnapshot;
use crate::snapshot::{Snapshot, SnapshotStore};
use crate::writer::{Append, Writers};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub writer_expiration: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            writer_expiration: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

impl Config {
    fn writer_expiration_ms(&self) -> i64 {
        i64::try_from(self.writer_expiration.as_millis()).unwrap_or(i64::MAX)
    }
}

const REPLAY_CHUNK: usize = 8 << 20;

struct State {
    writers: Writers,
    max_timestamp: i64,
}

pub struct Tablet {
    bucket: Bucket,
    stream: Arc<dyn Stream>,
    snapshots: Arc<dyn SnapshotStore>,
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
    high_watermark: watch::Sender<i64>,
}

impl Tablet {
    pub async fn open(
        bucket: Bucket,
        stream: Arc<dyn Stream>,
        snapshots: Arc<dyn SnapshotStore>,
        clock: Arc<dyn Clock>,
        config: Config,
    ) -> Result<Self, Error> {
        let now = clock.millis();
        let mut writers = Writers::new(config.writer_expiration_ms());
        let mut max_timestamp = -1;
        let mut replay_from = offset(stream.start_offset());
        if let Some(snapshot) = snapshots.load(&bucket).await? {
            snapshot.restore(&mut writers, now);
            replay_from = replay_from.max(snapshot.offset);
            max_timestamp = writers
                .active_writers()
                .map(|w| w.last_batch_timestamp())
                .fold(max_timestamp, i64::max);
        }

        let end = offset(stream.next_offset());
        let mut next = replay_from;
        while next < end {
            let fetched = stream
                .fetch(
                    FetchContext::default(),
                    unsigned(next),
                    unsigned(end),
                    REPLAY_CHUNK,
                )
                .await?;
            if fetched.records.is_empty() {
                break;
            }
            for record in &fetched.records {
                let header = Header::read(&record.payload)?;
                max_timestamp = max_timestamp.max(header.commit_timestamp);
                if header.writer_id != NO_WRITER_ID {
                    let mut append = writers.prepare_update(header.writer_id);
                    append.append(&header, true)?;
                    writers.update(append);
                }
                next = offset(record.last_offset);
            }
        }
        writers.set_map_end_offset(end);

        let high_watermark = watch::Sender::new(offset(stream.confirm_offset()));

        Ok(Tablet {
            bucket,
            stream,
            snapshots,
            clock,
            state: Mutex::new(State {
                writers,
                max_timestamp,
            }),
            high_watermark,
        })
    }

    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    pub fn offsets(&self) -> OffsetSnapshot {
        OffsetSnapshot {
            log_start: offset(self.stream.start_offset()),
            high_watermark: offset(self.stream.confirm_offset()),
            log_end: offset(self.stream.next_offset()),
        }
    }

    pub fn high_watermark(&self) -> i64 {
        offset(self.stream.confirm_offset())
    }

    pub fn log_end_offset(&self) -> i64 {
        offset(self.stream.next_offset())
    }

    pub fn log_start_offset(&self) -> i64 {
        offset(self.stream.start_offset())
    }

    pub async fn append(&self, records: Bytes) -> Result<AppendInfo, Error> {
        let batches = analyze(records)?;
        if batches.is_empty() {
            return Ok(AppendInfo::empty());
        }

        let (info, pending) = self.submit(batches)?;
        for append in pending {
            append.durable().await?;
        }

        let durable = self.high_watermark();
        // Concurrent appends complete in any order; the watermark only advances.
        self.high_watermark.send_if_modified(|hw| {
            if durable > *hw {
                *hw = durable;
                true
            } else {
                false
            }
        });

        Ok(info)
    }

    pub async fn wait_past(&self, offset: i64, timeout: Duration) -> i64 {
        let mut rx = self.high_watermark.subscribe();
        let current = *rx.borrow_and_update();
        if current > offset {
            return current;
        }

        let waited = tokio::time::timeout(timeout, rx.wait_for(|hw| *hw > offset)).await;
        match waited {
            Ok(Ok(hw)) => *hw,
            _ => self.high_watermark(),
        }
    }

    fn submit(&self, batches: Vec<Batch>) -> Result<(AppendInfo, Vec<PendingAppend>), Error> {
        let mut state = self.state();
        let now = self.clock.millis();
        let timestamp = state.max_timestamp.max(now);
        let first_offset = offset(self.stream.next_offset());

        let mut next = first_offset;
        let mut assigned = Vec::with_capacity(batches.len());
        for batch in batches {
            let mut bytes = batch.into_bytes().to_vec();
            header::set_base_offset(&mut bytes, next);
            header::set_commit_timestamp(&mut bytes, timestamp);
            let header = Header::read(&bytes)?;
            next = header.next_offset();
            assigned.push((header, bytes));
        }

        let mut updates: HashMap<i64, Append> = HashMap::new();
        for (header, _) in &assigned {
            if header.writer_id == NO_WRITER_ID {
                continue;
            }
            let duplicate = state
                .writers
                .last_entry(header.writer_id)
                .and_then(|entry| entry.find_duplicate(header.batch_sequence));
            if let Some(duplicate) = duplicate {
                return Ok((
                    AppendInfo {
                        first_offset: duplicate.first_offset(),
                        last_offset: duplicate.last_offset,
                        max_timestamp: duplicate.timestamp,
                        batch_count: assigned.len(),
                        duplicated: true,
                    },
                    Vec::new(),
                ));
            }
            let expired = state.writers.is_batch_expired(now, header);
            updates
                .entry(header.writer_id)
                .or_insert_with(|| state.writers.prepare_update(header.writer_id))
                .append(header, expired)?;
        }

        let mut pending = Vec::with_capacity(assigned.len());
        for (header, bytes) in assigned {
            let count = u32::try_from(header.record_count).map_err(|_| Error::EmptyBatch)?;
            let batch = RecordBatch::new(count, timestamp, Bytes::from(bytes));
            let append = Arc::clone(&self.stream).submit_append(AppendContext::default(), batch)?;
            if offset(append.base_offset()) != header.base_offset {
                return Err(Error::OffsetMismatch {
                    expected: header.base_offset,
                    assigned: offset(append.base_offset()),
                });
            }
            pending.push(append);
        }

        for update in updates.into_values() {
            state.writers.update(update);
        }
        state.writers.set_map_end_offset(next);
        state.max_timestamp = timestamp;

        Ok((
            AppendInfo {
                first_offset,
                last_offset: next - 1,
                max_timestamp: timestamp,
                batch_count: pending.len(),
                duplicated: false,
            },
            pending,
        ))
    }

    pub async fn read(
        &self,
        fetch_offset: i64,
        max_bytes: usize,
        isolation: FetchIsolation,
        projection: Option<&Projection>,
    ) -> Result<FetchInfo, Error> {
        let offsets = self.offsets();
        if fetch_offset < offsets.log_start || fetch_offset > offsets.log_end {
            return Err(Error::OutOfRange {
                offset: fetch_offset,
                start: offsets.log_start,
                end: offsets.log_end,
            });
        }

        let bound = match isolation {
            FetchIsolation::HighWatermark => offsets.high_watermark,
            FetchIsolation::LogEnd => offsets.log_end,
        };
        let mut info = FetchInfo {
            fetch_offset,
            high_watermark: offsets.high_watermark,
            log_end_offset: offsets.log_end,
            batches: Vec::new(),
        };
        if fetch_offset >= bound {
            return Ok(info);
        }

        let fetched = self
            .stream
            .fetch(
                FetchContext::default(),
                unsigned(fetch_offset),
                unsigned(bound),
                max_bytes,
            )
            .await?;
        for record in fetched.records {
            let batch = Batch::parse(record.payload)?;
            if batch.header().base_offset != offset(record.base_offset) {
                return Err(Error::OffsetMismatch {
                    expected: offset(record.base_offset),
                    assigned: batch.header().base_offset,
                });
            }
            info.batches.push(match projection {
                Some(projection) => Bytes::from(projection.apply(&batch)?),
                None => batch.into_bytes(),
            });
        }

        Ok(info)
    }

    pub async fn offset_for_timestamp(&self, timestamp: i64) -> Result<i64, Error> {
        self.offset_for_timestamp_from(timestamp, self.log_start_offset())
            .await
    }

    pub async fn offset_for_timestamp_from(&self, timestamp: i64, from: i64) -> Result<i64, Error> {
        let now = self.clock.millis();
        if timestamp > now {
            return Err(Error::InvalidTimestamp { timestamp, now });
        }

        let (offsets, max_timestamp) = {
            let state = self.state();
            (self.offsets(), state.max_timestamp)
        };
        if timestamp > max_timestamp {
            return Ok(offsets.high_watermark);
        }

        let mut lo = from.clamp(offsets.log_start, offsets.high_watermark);
        let mut hi = offsets.high_watermark;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let Some(header) = self.header_at(mid).await? else {
                break;
            };
            if header.commit_timestamp >= timestamp {
                hi = header.base_offset;
            } else {
                lo = header.next_offset();
            }
        }

        Ok(lo)
    }

    pub async fn commit_timestamp_at(&self, offset: i64) -> Result<Option<i64>, Error> {
        let offsets = self.offsets();
        if offset < offsets.log_start || offset >= offsets.high_watermark {
            return Ok(None);
        }

        Ok(self.header_at(offset).await?.map(|h| h.commit_timestamp))
    }

    async fn header_at(&self, at: i64) -> Result<Option<Header>, Error> {
        let fetched = self
            .stream
            .fetch(FetchContext::default(), unsigned(at), unsigned(at + 1), 1)
            .await?;

        match fetched.records.first() {
            Some(record) => Ok(Some(Header::read(&record.payload)?)),
            None => Ok(None),
        }
    }

    pub fn remove_expired_writers(&self) {
        let now = self.clock.millis();
        self.state().writers.remove_expired(now);
    }

    pub fn writer_count(&self) -> usize {
        self.state().writers.writer_count()
    }

    pub async fn snapshot(&self) -> Result<Snapshot, Error> {
        let snapshot = Snapshot::capture(&self.state().writers);
        self.snapshots.store(&self.bucket, &snapshot).await?;

        Ok(snapshot)
    }

    pub async fn trim(&self, new_start_offset: i64) -> Result<(), Error> {
        self.stream.trim(unsigned(new_start_offset)).await?;

        Ok(())
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.snapshot().await?;
        self.stream.close().await?;

        Ok(())
    }

    pub async fn destroy(&self) -> Result<(), Error> {
        self.stream.destroy().await?;
        self.snapshots.remove(&self.bucket).await?;
        self.state().writers.truncate_fully_and_start_at(0);

        Ok(())
    }

    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }
}

fn offset(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unsigned(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
