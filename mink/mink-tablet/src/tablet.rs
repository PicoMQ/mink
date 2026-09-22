//! One bucket's primary-key tablet: puts, lookups, scans, checkpoints and recovery over a log and a store.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use bytes::Bytes;
use mink_kv::{Snapshot, Store};
use mink_log::{AppendInfo, FetchIsolation};
use mink_record::{ChangeType, Codec, Compression, Row, Scalar, codec};
use mink_table::{Bucket, ChangelogImage, DeleteBehavior, Descriptor};
use tokio::sync::Mutex;

use crate::Error;
use crate::autoinc::{AutoIncrement, Sequence, Tracker};
use crate::changelog::Changelog;
use crate::merger::{Decoded, Merged, Merger, RowMerger};
use crate::prewrite::{Buffer, TruncateReason};
use crate::put::{Op, Put};
use crate::recover::{RecoverPoint, Replay};
use crate::schema::{Schemas, Version, Versions};
use crate::value::Value;
use crate::value::values_to_arrow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub recover_fetch_bytes: usize,
    pub write_batch_bytes: usize,
    pub auto_increment_cache: u64,
    pub compression: Compression,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            recover_fetch_bytes: 16 << 20,
            write_batch_bytes: 2 << 20,
            auto_increment_cache: 100_000,
            compression: Compression::default(),
        }
    }
}

#[derive(Debug)]
pub struct Checkpoint {
    pub files: mink_kv::Checkpoint,
    pub recover_point: RecoverPoint,
}

pub struct Scan {
    pub rows: Box<dyn Snapshot>,
    pub log_offset: i64,
}

struct Writer {
    prewrite: Buffer,
    flushed_log_offset: i64,
    row_count: i64,
    auto_increment: Option<AutoIncrement>,
}

pub struct Tablet {
    bucket: Bucket,
    log: Arc<mink_log::Tablet>,
    store: Box<dyn Store>,
    versions: Versions,
    merger: Merger,
    codec: Box<dyn Codec>,
    changelog_image: ChangelogImage,
    config: Config,
    writer: Mutex<Writer>,
    closed: AtomicBool,
}

impl Tablet {
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        bucket: Bucket,
        descriptor: &Descriptor,
        schemas: Arc<dyn Schemas>,
        sequence: Arc<dyn Sequence>,
        log: Arc<mink_log::Tablet>,
        store: Box<dyn Store>,
        recover_point: RecoverPoint,
        config: Config,
    ) -> Result<Self, Error> {
        let options = descriptor.options();
        let versions = Versions::new(schemas, options.kv_format, descriptor.bucketing());
        let auto_increment = AutoIncrement::new(
            &versions.latest()?.schema,
            sequence,
            config.auto_increment_cache,
        )?;
        let tablet = Tablet {
            bucket,
            log,
            store,
            versions,
            merger: Merger::new(descriptor)?,
            codec: codec(options.log_format, config.compression),
            changelog_image: options.changelog_image,
            config,
            writer: Mutex::new(Writer {
                prewrite: Buffer::new(),
                flushed_log_offset: recover_point.log_offset,
                row_count: recover_point.row_count,
                auto_increment,
            }),
            closed: AtomicBool::new(false),
        };
        tablet.recover(recover_point).await?;

        Ok(tablet)
    }

    async fn recover(&self, point: RecoverPoint) -> Result<(), Error> {
        let mut writer = self.writer.lock().await;
        let latest = self.versions.latest()?;
        let auto_increment = writer
            .auto_increment
            .as_ref()
            .and_then(|_| latest.auto_increment);
        let mut tracker = match (&writer.auto_increment, point.auto_increment) {
            (Some(generator), Some(range)) => Some(Tracker::new(range, generator.cache_size())),
            _ => None,
        };

        let mut row_count = writer.row_count;
        let mut batch = mink_kv::Writer::new(self.store.as_ref(), self.config.write_batch_bytes);
        let mut replay = Replay::new(
            &self.log,
            &self.versions,
            self.codec.as_ref(),
            writer.flushed_log_offset,
            FetchIsolation::HighWatermark,
            self.config.recover_fetch_bytes,
            auto_increment,
        )?;
        while let Some(records) = replay.next_batch().await? {
            for replayed in records {
                match replayed.change {
                    ChangeType::Insert => row_count += 1,
                    ChangeType::Delete => row_count -= 1,
                    _ => {}
                }
                if let (Some(tracker), Some(id)) = (&mut tracker, replayed.auto_increment) {
                    tracker.inserted(id);
                }
                match replayed.value {
                    Some(value) => batch.put(replayed.key, value).await?,
                    None => batch.delete(replayed.key).await?,
                }
            }
        }
        let flushed = replay.offset();
        batch.close().await?;
        writer.flushed_log_offset = flushed;
        writer.row_count = row_count;

        let prewrite = &mut writer.prewrite;
        let mut replay = Replay::new(
            &self.log,
            &self.versions,
            self.codec.as_ref(),
            flushed,
            FetchIsolation::LogEnd,
            self.config.recover_fetch_bytes,
            auto_increment,
        )?;
        while let Some(records) = replay.next_batch().await? {
            for replayed in records {
                if let (Some(tracker), Some(id)) = (&mut tracker, replayed.auto_increment) {
                    tracker.inserted(id);
                }
                match (replayed.change, replayed.value) {
                    (ChangeType::Delete, None) => prewrite.delete(replayed.key, replayed.offset),
                    (ChangeType::Insert, Some(value)) => {
                        prewrite.insert(replayed.key, value, replayed.offset)
                    }
                    (_, Some(value)) => prewrite.update(replayed.key, value, replayed.offset),
                    (change, None) => unreachable!("{change:?} without a value"),
                }?;
            }
        }

        if let (Some(generator), Some(tracker)) = (&mut writer.auto_increment, tracker) {
            generator.update(tracker.range())?;
        }

        Ok(())
    }

    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    pub fn log(&self) -> &Arc<mink_log::Tablet> {
        &self.log
    }

    pub fn latest(&self) -> Result<Arc<Version>, Error> {
        self.versions.latest()
    }

    pub async fn put(&self, put: Put) -> Result<AppendInfo, Error> {
        let mut writer = self.writer.lock().await;
        self.ensure_open()?;
        let latest = self.versions.latest()?;
        if put.schema_id.0 > latest.id.0 {
            return Err(Error::SchemaNotExist(put.schema_id));
        }
        if put.ops.len() != put.rows.num_rows() {
            return Err(Error::OpCount {
                ops: put.ops.len(),
                rows: put.rows.num_rows(),
            });
        }
        let put = self.rebase(put, &latest)?;
        let version = Arc::clone(&latest);
        let merger = self
            .merger
            .configure(put.target_columns.as_deref(), &latest)?;
        if let Some(generator) = &mut writer.auto_increment {
            generator.validate_targets(put.target_columns.as_deref(), &latest)?;
            let upserts = put.ops.iter().filter(|op| **op == Op::Upsert).count();
            generator.reserve(upserts).await?;
        }

        let start = self.log.log_end_offset();
        let mut context = Context {
            version: &version,
            merger: merger.as_ref(),
            changelog: Changelog::new(Arc::clone(&latest), put.rows.num_rows() * 2)?,
            next_offset: start,
        };
        if let Err(error) = self.stage(&mut writer, &mut context, &put) {
            writer.prewrite.truncate_to(start, TruncateReason::Error);
            return Err(error);
        }
        if context.changelog.is_empty() {
            return Ok(AppendInfo::empty());
        }

        let bytes =
            context
                .changelog
                .build(put.writer_id, put.batch_sequence, self.codec.as_ref())?;
        let info = match self.log.append(Bytes::from(bytes)).await {
            Ok(info) => info,
            Err(error) => {
                writer.prewrite.truncate_to(start, TruncateReason::Error);
                return Err(error.into());
            }
        };
        if info.duplicated {
            writer
                .prewrite
                .truncate_to(start, TruncateReason::Duplicated);
            return Ok(info);
        }
        if info.first_offset != start {
            writer.prewrite.truncate_to(start, TruncateReason::Error);
            return Err(Error::OffsetMismatch {
                expected: start,
                actual: info.first_offset,
            });
        }

        self.flush(&mut writer, info.next_offset()).await?;

        Ok(info)
    }

    fn rebase(&self, put: Put, latest: &Version) -> Result<Put, Error> {
        if put.schema_id == latest.id {
            return Ok(put);
        }
        let version = self.versions.get(put.schema_id)?;
        let remap = self.versions.remap(&version, latest);
        let target_columns = match put.target_columns {
            None => None,
            Some(columns) => Some(
                columns
                    .into_iter()
                    .map(|column| {
                        remap
                            .position(column)
                            .ok_or(Error::TargetColumnDropped(column))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        };

        Ok(Put {
            schema_id: latest.id,
            rows: remap.batch(put.rows)?,
            target_columns,
            ..put
        })
    }

    fn stage(
        &self,
        writer: &mut Writer,
        context: &mut Context<'_>,
        put: &Put,
    ) -> Result<(), Error> {
        let keys = context.version.keys.bind(&put.rows)?;
        let readers = context.version.readers(&put.rows)?;
        for (index, op) in put.ops.iter().enumerate() {
            let key = Bytes::from(keys.encode_vec(index)?);
            match op {
                Op::Delete => self.stage_delete(writer, context, key)?,
                Op::Upsert => {
                    let row: Row<'_> = readers.iter().map(|r| r.get(index)).collect();
                    self.stage_upsert(writer, context, key, row)?;
                }
            }
        }

        Ok(())
    }

    fn stage_delete(
        &self,
        writer: &mut Writer,
        context: &mut Context<'_>,
        key: Bytes,
    ) -> Result<(), Error> {
        match context.merger.delete_behavior() {
            DeleteBehavior::Ignore => return Ok(()),
            DeleteBehavior::Disable => return Err(Error::DeleteDisabled),
            DeleteBehavior::Allow => {}
        }

        let Some(current) = self.current(writer, &key)? else {
            return Ok(());
        };
        let old_value = Value::decode(current)?;
        let old = self.versions.decode(&old_value)?;

        match context.merger.delete(&old)? {
            None => {
                context.changelog.append(ChangeType::Delete, &old.row)?;
                writer.prewrite.delete(key, context.next_offset)?;
                context.next_offset += 1;
                Ok(())
            }
            Some(value) => {
                let row = self.versions.decode(&value)?.row;
                self.stage_update(writer, context, key, &old.row, &value, &row)
            }
        }
    }

    fn stage_upsert(
        &self,
        writer: &mut Writer,
        context: &mut Context<'_>,
        key: Bytes,
        row: Row<'_>,
    ) -> Result<(), Error> {
        let mut new = Decoded {
            schema_id: context.version.id,
            row,
        };
        // Last-write-wins WAL can skip the read; auto-increment cannot, because
        // the assigned value depends on whether the key already exists.
        if self.changelog_image == ChangelogImage::Wal
            && context.merger.is_default()
            && writer.auto_increment.is_none()
        {
            let value = context.version.encode(&new.row)?;
            return self.stage_update(writer, context, key, &[], &value, &new.row);
        }

        let Some(current) = self.current(writer, &key)? else {
            if let Some(generator) = &mut writer.auto_increment {
                generator.fill(&mut new.row, context.version)?;
            }
            let value = context.version.encode(&new.row)?;
            context.changelog.append(ChangeType::Insert, &new.row)?;
            writer
                .prewrite
                .insert(key, value.encode(), context.next_offset)?;
            context.next_offset += 1;
            return Ok(());
        };

        let value = context.version.encode(&new.row)?;
        let old_value = Value::decode(current)?;
        let old = self.versions.decode(&old_value)?;

        match context.merger.merge(&old, &new)? {
            Merged::Old => Ok(()),
            Merged::New => self.stage_update(writer, context, key, &old.row, &value, &new.row),
            Merged::Row(merged) => {
                let row = self.versions.decode(&merged)?.row;
                self.stage_update(writer, context, key, &old.row, &merged, &row)
            }
        }
    }

    fn stage_update(
        &self,
        writer: &mut Writer,
        context: &mut Context<'_>,
        key: Bytes,
        old_row: &[Option<Scalar<'_>>],
        value: &Value,
        row: &[Option<Scalar<'_>>],
    ) -> Result<(), Error> {
        if self.changelog_image == ChangelogImage::Full {
            context
                .changelog
                .append(ChangeType::UpdateBefore, old_row)?;
            context.next_offset += 1;
        }
        context.changelog.append(ChangeType::UpdateAfter, row)?;
        writer
            .prewrite
            .update(key, value.encode(), context.next_offset)?;
        context.next_offset += 1;

        Ok(())
    }

    fn current(&self, writer: &Writer, key: &[u8]) -> Result<Option<Bytes>, Error> {
        match writer.prewrite.get(key) {
            Some(buffered) => Ok(buffered.cloned()),
            None => Ok(self.store.get(key)?),
        }
    }

    async fn flush(&self, writer: &mut Writer, offset: i64) -> Result<(), Error> {
        let mut batch = mink_kv::Writer::new(self.store.as_ref(), self.config.write_batch_bytes);
        let delta = writer.prewrite.flush(offset, &mut batch).await?;
        batch.close().await?;
        writer.flushed_log_offset = offset;
        writer.row_count += delta;

        Ok(())
    }

    pub fn lookup(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        self.ensure_open()?;
        Ok(self.store.get(key)?)
    }

    pub fn multi_lookup(&self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>, Error> {
        self.ensure_open()?;
        Ok(self.store.multi_get(keys)?)
    }

    pub fn prefix_lookup(&self, prefix: &[u8]) -> Result<Vec<Bytes>, Error> {
        self.ensure_open()?;
        Ok(self.store.prefix_lookup(prefix)?)
    }

    pub fn limit_scan(&self, limit: usize) -> Result<Vec<Bytes>, Error> {
        self.ensure_open()?;
        Ok(self.store.limit_scan(limit)?)
    }

    pub async fn snapshot_scan(&self) -> Result<Scan, Error> {
        let writer = self.writer.lock().await;
        self.ensure_open()?;

        Ok(Scan {
            rows: self.store.snapshot()?,
            log_offset: writer.flushed_log_offset,
        })
    }

    pub fn to_batch<I>(&self, values: I) -> Result<RecordBatch, Error>
    where
        I: IntoIterator<Item = Bytes>,
    {
        let latest = self.versions.latest()?;

        values_to_arrow(
            values,
            latest.schema.fields(),
            self.versions.kv_format(),
            |id| self.versions.get(id).ok().map(|v| Arc::clone(&v.schema)),
        )
    }

    pub async fn row_count(&self) -> i64 {
        self.writer.lock().await.row_count
    }

    pub async fn flushed_log_offset(&self) -> i64 {
        self.writer.lock().await.flushed_log_offset
    }

    pub async fn pending_rows(&self) -> usize {
        self.writer.lock().await.prewrite.len()
    }

    pub async fn checkpoint(&self, dir: &Path) -> Result<Checkpoint, Error> {
        let writer = self.writer.lock().await;
        self.ensure_open()?;
        let files = self.store.checkpoint(dir)?;

        Ok(Checkpoint {
            files,
            recover_point: RecoverPoint {
                log_offset: writer.flushed_log_offset,
                row_count: writer.row_count,
                auto_increment: writer.auto_increment.as_ref().map(AutoIncrement::range),
            },
        })
    }

    pub async fn close(&self) -> Result<(), Error> {
        let _writer = self.writer.lock().await;
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        self.store.close().await?;

        Ok(())
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        Ok(())
    }
}

struct Context<'a> {
    version: &'a Version,
    merger: &'a dyn RowMerger,
    changelog: Changelog,
    next_offset: i64,
}
