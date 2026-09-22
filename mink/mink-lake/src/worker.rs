//! The tiering worker: takes a table from the coordinator, writes each bucket's new log range or snapshot
//! to the lake, commits once, and records the tiered offsets.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::TryStreamExt;
use mink_coordinator::tiering::Table;
use mink_metadata::LakeSnapshotRow;
use mink_table::{Bucket, Id, PartitionName, Path};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::committer::{BucketOffset, CommitterContext, SNAPSHOT_OFFSETS_PROPERTY};
use crate::error::{Error, Result};
use crate::tiering::{BucketSource, Config, Coordinator, TableInfo};
use crate::writer::{Factory, Writer, WriterContext};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundReport {
    pub table_id: Id,
    pub path: Path,
    pub snapshot_id: Option<i64>,
    pub tiered: BTreeMap<Bucket, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Split {
    Log { from: i64, to: i64 },
    Snapshot,
}

pub struct Worker<F: Factory> {
    factory: Arc<F>,
    coordinator: Arc<dyn Coordinator>,
    source: Arc<dyn BucketSource>,
    config: Config,
}

impl<F: Factory + 'static> Worker<F> {
    pub fn new(
        factory: Arc<F>,
        coordinator: Arc<dyn Coordinator>,
        source: Arc<dyn BucketSource>,
        config: Config,
    ) -> Self {
        Worker {
            factory,
            coordinator,
            source,
            config,
        }
    }

    pub fn spawn(self: Arc<Self>, mut leadership: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if !*leadership.borrow() {
                    if leadership.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                let idle = match self.run_once().await {
                    Ok(Some(report)) => {
                        tracing::info!(
                            table = %report.path,
                            snapshot = ?report.snapshot_id,
                            buckets = report.tiered.len(),
                            "tiering round done"
                        );
                        false
                    }
                    Ok(None) => true,
                    Err(e) => {
                        tracing::warn!(error = %e, "tiering round failed");
                        true
                    }
                };
                if idle {
                    tokio::select! {
                        changed = leadership.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                        _ = tokio::time::sleep(self.config.poll_interval) => {}
                    }
                }
            }
        })
    }

    pub async fn run_once(&self) -> Result<Option<RoundReport>> {
        match self.coordinator.request_table().await? {
            Some(table) => self.tier(&table).await.map(Some),
            None => Ok(None),
        }
    }

    pub async fn tier(&self, table: &Table) -> Result<RoundReport> {
        let fenced = Arc::new(AtomicBool::new(false));
        let heartbeat = Heartbeat::spawn(
            self.coordinator.clone(),
            table.clone(),
            self.config.heartbeat_interval,
            fenced.clone(),
        );
        let outcome = self.round(table, &fenced).await;
        drop(heartbeat);

        match outcome {
            Ok(report) => {
                self.coordinator.finish(table.table_id, table.epoch).await?;
                Ok(report)
            }
            Err(e) if e.is_fenced() || fenced.load(Ordering::Acquire) => Err(e),
            Err(e) => {
                if let Err(report) = self.coordinator.fail(table.table_id, table.epoch).await {
                    tracing::warn!(table = %table.path, error = %report, "could not report tiering failure");
                }

                Err(e)
            }
        }
    }

    async fn round(&self, table: &Table, fenced: &AtomicBool) -> Result<RoundReport> {
        let info = self
            .source
            .table(&table.path)?
            .filter(|info| info.table_id == table.table_id)
            .ok_or_else(|| {
                Error::Other(format!(
                    "table {} ({:?}) was dropped or recreated during tiering",
                    table.path, table.table_id
                ))
            })?;

        let known = self.coordinator.lake_snapshot(table.table_id);
        let splits = self.plan(&info, known.as_ref()).await?;
        let mut report = RoundReport {
            table_id: table.table_id,
            path: table.path.clone(),
            snapshot_id: None,
            tiered: BTreeMap::new(),
        };
        if splits.is_empty() {
            return Ok(report);
        }

        let mut results = Vec::with_capacity(splits.len());
        for (bucket, partition, split) in splits {
            if fenced.load(Ordering::Acquire) {
                return Err(Error::Other(format!(
                    "tiering of {} was fenced by the coordinator",
                    table.path
                )));
            }
            let context = WriterContext {
                path: table.path.clone(),
                bucket,
                partition,
                descriptor: info.descriptor.clone(),
            };
            let writer = self.factory.create_writer(context).await?;
            let (result, end) = self.write_split(writer, bucket, split).await?;
            results.push(result);
            report.tiered.insert(bucket, end);
        }

        let mut committer = self
            .factory
            .create_committer(CommitterContext {
                path: table.path.clone(),
                descriptor: info.descriptor.clone(),
            })
            .await?;
        let committable = committer.to_committable(results).await?;
        if let Some(missing) = committer
            .missing_snapshot(known.as_ref().map(|k| k.snapshot_id))
            .await?
        {
            let offsets = missing.properties.get(SNAPSHOT_OFFSETS_PROPERTY).ok_or(
                Error::SnapshotProperty(missing.snapshot_id, SNAPSHOT_OFFSETS_PROPERTY),
            )?;
            let offsets = BucketOffset::decode(missing.snapshot_id, offsets)?;
            self.record(table.table_id, missing.snapshot_id, offsets)
                .await?;
            committer.abort(committable).await?;

            return Err(Error::Other(format!(
                "recorded lake snapshot {:?} is behind the lake's {} for table {}; recorded it, \
                 retiering next round",
                known.map(|k| k.snapshot_id),
                missing.snapshot_id,
                table.path
            )));
        }

        let known_id = known.as_ref().map(|k| k.snapshot_id);
        let mut offsets = known.map(|k| k.bucket_log_end_offset).unwrap_or_default();
        offsets.extend(report.tiered.iter().map(|(b, o)| (*b, *o)));
        if committer.is_empty(&committable) {
            // Offsets advance even when the lake is unchanged, or the range would be re-read forever.
            let Some(snapshot_id) = known_id else {
                return Ok(report);
            };
            self.record(table.table_id, snapshot_id, offsets).await?;
            report.snapshot_id = Some(snapshot_id);

            return Ok(report);
        }

        let property = BucketOffset::encode(&offsets)?;
        let committed = committer
            .commit(
                committable,
                BTreeMap::from([(SNAPSHOT_OFFSETS_PROPERTY.to_string(), property)]),
            )
            .await?;
        self.record(table.table_id, committed.committed_snapshot_id, offsets)
            .await?;
        report.snapshot_id = Some(committed.committed_snapshot_id);

        Ok(report)
    }

    async fn record(
        &self,
        table_id: Id,
        snapshot_id: i64,
        bucket_log_end_offset: BTreeMap<Bucket, i64>,
    ) -> Result<()> {
        self.coordinator
            .commit_lake_snapshot(
                table_id,
                LakeSnapshotRow {
                    snapshot_id,
                    bucket_log_end_offset,
                },
            )
            .await
    }

    async fn plan(
        &self,
        info: &TableInfo,
        known: Option<&LakeSnapshotRow>,
    ) -> Result<Vec<(Bucket, Option<PartitionName>, Split)>> {
        let has_primary_key = info.descriptor.schema().primary_key().is_some();
        let mut splits = Vec::new();
        for (bucket, partition) in &info.buckets {
            let (earliest, latest) = self.source.offsets(*bucket).await?;
            if latest <= 0 {
                continue;
            }
            let committed = known.and_then(|k| k.bucket_log_end_offset.get(bucket).copied());
            let split = match committed {
                None if has_primary_key && known.is_none() => Split::Snapshot,
                None => Split::Log {
                    from: earliest,
                    to: latest,
                },
                Some(committed) if committed >= latest => continue,
                Some(committed) if committed < earliest => {
                    return Err(Error::Other(format!(
                        "bucket {bucket:?} is tiered to {committed} but its log starts at {earliest}"
                    )));
                }
                Some(committed) => Split::Log {
                    from: committed,
                    to: latest,
                },
            };
            splits.push((*bucket, partition.clone(), split));
        }

        Ok(splits)
    }

    async fn write_split(
        &self,
        mut writer: Box<dyn Writer<F::WriteResult>>,
        bucket: Bucket,
        split: Split,
    ) -> Result<(F::WriteResult, i64)> {
        let end = match split {
            Split::Log { from, to } => {
                let mut batches = self.source.log(bucket, from, to, None);
                while let Some(batch) = batches.try_next().await? {
                    if batch.last_offset() >= to {
                        return Err(Error::Other(format!(
                            "bucket {bucket:?} returned offset {} past the split end {to}",
                            batch.last_offset()
                        )));
                    }
                    writer.write(&batch).await?;
                }
                to
            }
            Split::Snapshot => {
                let mut read = self.source.snapshot(bucket).await?;
                while let Some(batch) = read.batches.try_next().await? {
                    writer.write(&batch).await?;
                }
                read.log_offset
            }
        };

        Ok((writer.complete().await?, end))
    }
}

struct Heartbeat(JoinHandle<()>);

impl Heartbeat {
    fn spawn(
        coordinator: Arc<dyn Coordinator>,
        table: Table,
        interval: Duration,
        fenced: Arc<AtomicBool>,
    ) -> Self {
        Heartbeat(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = coordinator.heartbeat(table.table_id, table.epoch).await {
                    tracing::warn!(table = %table.path, error = %e, "tiering heartbeat rejected");
                    fenced.store(true, Ordering::Release);
                    return;
                }
            }
        }))
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}
