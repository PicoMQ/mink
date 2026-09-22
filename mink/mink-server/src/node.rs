//! The node: reconciles hosted buckets against the catalog, opens logs and key-value tablets with recovery,
//! and runs the sync, snapshot and retention loops.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mink_common::Clock;
use mink_coordinator::Membership;
use mink_kv::Engine;
use mink_metadata::{BucketRow, Handle, View};
use mink_table::{Bucket, Descriptor};
use mink_tablet::{CompletedSnapshot, RecoverPoint, Tablet, Uploader, download};
use object_store::ObjectStore;
use object_store::path::Path;
use s3stream::{Client, OpenStreamOptions, StreamState};
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::error::Error;
use crate::failover;
use crate::registry::{Hosted, Kv, Registry, SnapshotState};
use crate::schemas::Schemas;
use crate::sequence;

#[derive(Debug, Clone)]
pub struct Config {
    pub cluster_id: String,
    pub wal_config: String,
    pub data_dir: PathBuf,
    pub kv_snapshot_interval: Duration,
    pub sync_interval: Duration,
    pub log_retention_interval: Duration,
    pub log: mink_log::Config,
    pub kv: mink_tablet::Config,
    pub kv_options: mink_kv::Options,
}

impl Config {
    pub fn new(
        cluster_id: impl Into<String>,
        wal_config: impl Into<String>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        Config {
            cluster_id: cluster_id.into(),
            wal_config: wal_config.into(),
            data_dir: data_dir.into(),
            kv_snapshot_interval: Duration::from_secs(10 * 60),
            sync_interval: Duration::from_secs(5),
            log_retention_interval: Duration::from_secs(5 * 60),
            log: mink_log::Config::default(),
            kv: mink_tablet::Config::default(),
            kv_options: mink_kv::Options::default(),
        }
    }
}

pub struct Node {
    metadata: Handle,
    engine: Arc<dyn Client>,
    storage: Arc<dyn ObjectStore>,
    kv_engine: Arc<dyn Engine>,
    membership: Arc<dyn Membership>,
    writer_snapshots: Arc<mink_log::Kv>,
    clock: Arc<dyn Clock>,
    config: Config,
    registry: Registry,
    sync: AsyncMutex<()>,
}

impl Node {
    pub fn new(
        metadata: Handle,
        engine: Arc<dyn Client>,
        storage: Arc<dyn ObjectStore>,
        kv_engine: Arc<dyn Engine>,
        membership: Arc<dyn Membership>,
        clock: Arc<dyn Clock>,
        config: Config,
    ) -> Self {
        let writer_snapshots = Arc::new(mink_log::Kv::new(engine.kv_client()));
        Node {
            metadata,
            engine,
            storage,
            kv_engine,
            membership,
            writer_snapshots,
            clock,
            config,
            registry: Registry::default(),
            sync: AsyncMutex::new(()),
        }
    }

    pub fn node_id(&self) -> i32 {
        self.metadata.node_id()
    }

    pub fn metadata(&self) -> &Handle {
        &self.metadata
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn sync(&self) -> Report {
        let _pass = self.sync.lock().await;
        let view = self.metadata.views().load();
        let me = self.node_id();
        let mut report = Report::default();

        for hosted in self.registry.all() {
            let bucket = hosted.bucket;
            let action = match view.state.catalog.buckets.get(&bucket) {
                None => Action::Destroy,
                Some(row) if row.leader != me || row.leader_epoch != hosted.leader_epoch => {
                    Action::Close
                }
                Some(_) => Action::Keep,
            };
            let result = match action {
                Action::Keep => continue,
                Action::Destroy => self.destroy(hosted).await,
                Action::Close => self.close(hosted).await,
            };
            match (action, result) {
                (Action::Destroy, Ok(())) => report.destroyed.push(bucket),
                (_, Ok(())) => report.closed.push(bucket),
                (_, Err(e)) => {
                    tracing::warn!(?bucket, %e, ?action, "release failed");
                    report.failed.push((bucket, e));
                }
            }
        }

        let wanted: Vec<Bucket> = view
            .state
            .catalog
            .buckets
            .iter()
            .filter(|(bucket, row)| row.leader == me && !self.registry.contains(**bucket))
            .map(|(bucket, _)| *bucket)
            .collect();
        for bucket in wanted {
            match self.open(view.clone(), bucket).await {
                Ok(()) => report.opened.push(bucket),
                Err(e) => {
                    tracing::warn!(?bucket, %e, "open failed");
                    report.failed.push((bucket, e));
                }
            }
        }

        report
    }

    async fn open(&self, view: Arc<View>, bucket: Bucket) -> Result<(), Error> {
        let row = view
            .state
            .catalog
            .buckets
            .get(&bucket)
            .ok_or(Error::BucketNotExist(bucket))?;
        let table = view
            .state
            .catalog
            .table_by_id(bucket.table())
            .ok_or(Error::BucketNotExist(bucket))?;
        let path = view
            .state
            .catalog
            .table_paths
            .get(&bucket.table())
            .cloned()
            .ok_or(Error::BucketNotExist(bucket))?;

        self.registry.begin(bucket);
        let result = self.open_inner(&view, bucket, row, &table.descriptor).await;
        match result {
            Ok((log, kv, leader_epoch)) => {
                self.registry.insert(Arc::new(Hosted {
                    bucket,
                    path,
                    descriptor: Arc::new(table.descriptor.clone()),
                    stream_id: row.stream_id,
                    leader_epoch,
                    log,
                    kv,
                    retention: Mutex::new(None),
                }));
                self.registry.finish(bucket);
                tracing::info!(?bucket, leader_epoch, "bucket opened");

                Ok(())
            }
            Err(e) => {
                self.registry.finish(bucket);

                Err(e)
            }
        }
    }

    async fn open_inner(
        &self,
        view: &View,
        bucket: Bucket,
        row: &BucketRow,
        descriptor: &Descriptor,
    ) -> Result<(Arc<mink_log::Tablet>, Option<Kv>, i32), Error> {
        let stream_id = row.stream_id;
        let mut leader_epoch = row.leader_epoch;
        let stream = view.state.streams.get(&stream_id);
        if let Some(stream) = stream
            && stream.state == StreamState::Opened
            && stream.node_id != self.node_id()
        {
            let live = self
                .membership
                .live_nodes()
                .await
                .map_err(|e| Error::Membership(e.to_string()))?;
            if live.contains(&stream.node_id) {
                return Err(Error::HeldBy {
                    bucket,
                    node_id: stream.node_id,
                });
            }
            tracing::info!(
                ?bucket,
                dead = stream.node_id,
                "taking over from dead leader"
            );
            failover::take_over(
                &self.metadata,
                self.engine.as_ref(),
                &self.config.wal_config,
                stream.node_id,
            )
            .await?;
        }
        if let Some(stream) = stream
            && stream.state == StreamState::Closed
            && stream.epoch == i64::from(leader_epoch)
        {
            leader_epoch = self
                .metadata
                .lead_bucket(bucket, row.coordinator_epoch)
                .await?;
            tracing::info!(?bucket, leader_epoch, "new term after restart");
        }

        let stream = self
            .engine
            .stream_client()
            .open_stream(
                stream_id,
                OpenStreamOptions {
                    epoch: leader_epoch as u64,
                    ..Default::default()
                },
            )
            .await?;
        let log = Arc::new(
            mink_log::Tablet::open(
                bucket,
                stream,
                self.writer_snapshots.clone(),
                self.clock.clone(),
                self.config.log,
            )
            .await?,
        );

        if descriptor.schema().primary_key().is_none() {
            return Ok((log, None, leader_epoch));
        }

        let kv = self.open_kv(view, bucket, descriptor, log.clone()).await?;

        Ok((log, Some(kv), leader_epoch))
    }

    async fn open_kv(
        &self,
        view: &View,
        bucket: Bucket,
        descriptor: &Descriptor,
        log: Arc<mink_log::Tablet>,
    ) -> Result<Kv, Error> {
        let dir = self.kv_dir(bucket);
        if dir.exists() {
            tokio::fs::remove_dir_all(&dir).await?;
        }
        tokio::fs::create_dir_all(&dir).await?;
        let store_dir = dir.join("store");

        let restored = match view.state.catalog.latest_kv_snapshot(bucket) {
            Some(row) => Some(CompletedSnapshot::load(self.storage.as_ref(), &row.path).await?),
            None => None,
        };
        let (store, recover_point) = match &restored {
            Some(snapshot) => {
                let restore_dir = dir.join("restore");
                download(self.storage.as_ref(), snapshot, &restore_dir).await?;
                let store =
                    self.kv_engine
                        .restore(&store_dir, &restore_dir, self.config.kv_options)?;
                tokio::fs::remove_dir_all(&restore_dir).await?;
                (store, snapshot.recover_point())
            }
            None => (
                self.kv_engine.open(&store_dir, self.config.kv_options)?,
                RecoverPoint {
                    log_offset: log.log_start_offset(),
                    row_count: 0,
                    auto_increment: None,
                },
            ),
        };

        let schemas = Schemas::new(self.metadata.views().clone(), bucket.table())
            .ok_or(Error::BucketNotExist(bucket))?;
        let sequence =
            sequence::for_table(self.metadata.clone(), bucket.table(), descriptor.schema());
        let tablet = Tablet::open(
            bucket,
            descriptor,
            Arc::new(schemas),
            sequence,
            log,
            store,
            recover_point,
            self.config.kv,
        )
        .await?;

        let uploader = Uploader::new(
            self.storage.clone(),
            remote_kv_dir(bucket),
            restored.as_ref(),
        );

        Ok(Kv {
            tablet,
            dir,
            snapshots: AsyncMutex::new(SnapshotState {
                uploader,
                log_offset: restored.as_ref().map(|s| s.log_offset).unwrap_or(-1),
            }),
        })
    }

    async fn close(&self, hosted: Arc<Hosted>) -> Result<(), Error> {
        self.release(&hosted, Release::Close).await?;
        tracing::info!(bucket = ?hosted.bucket, "bucket closed");

        Ok(())
    }

    async fn destroy(&self, hosted: Arc<Hosted>) -> Result<(), Error> {
        self.release(&hosted, Release::Destroy).await?;
        tracing::info!(bucket = ?hosted.bucket, "bucket destroyed");

        Ok(())
    }

    async fn release(&self, hosted: &Hosted, release: Release) -> Result<(), Error> {
        let bucket = hosted.bucket;
        self.registry.begin(bucket);
        self.registry.remove(bucket);

        let mut result = Ok(());
        if let Some(kv) = &hosted.kv {
            result = kv.tablet.close().await.map_err(Error::from);
        }
        if result.is_ok() {
            result = match release {
                Release::Destroy => hosted.log.destroy().await,
                Release::Close => hosted.log.close().await,
            }
            .map_err(Error::from);
        }
        if let Some(kv) = &hosted.kv
            && kv.dir.exists()
        {
            let _ = tokio::fs::remove_dir_all(&kv.dir).await;
        }
        self.registry.finish(bucket);

        result
    }

    pub fn spawn(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move { self.run(shutdown).await })
    }

    async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let mut published = self.metadata.views().subscribe();
        published.mark_unchanged();
        let mut snapshots = tokio::time::interval(self.config.kv_snapshot_interval);
        snapshots.set_missed_tick_behavior(MissedTickBehavior::Skip);
        snapshots.tick().await;
        let mut retention = tokio::time::interval(self.config.log_retention_interval);
        retention.set_missed_tick_behavior(MissedTickBehavior::Skip);
        retention.tick().await;
        self.sync().await;

        loop {
            enum Wake {
                Tick,
                Snapshot,
                Retention,
                Stop,
            }
            let wake = tokio::select! {
                _ = published.changed() => Wake::Tick,
                _ = tokio::time::sleep(self.config.sync_interval) => Wake::Tick,
                _ = snapshots.tick() => Wake::Snapshot,
                _ = retention.tick() => Wake::Retention,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { Wake::Stop } else { Wake::Tick }
                }
            };
            match wake {
                Wake::Tick => {
                    self.sync().await;
                }
                Wake::Snapshot => {
                    for (bucket, result) in self.snapshot_all().await {
                        if let Err(e) = result {
                            tracing::warn!(?bucket, %e, "kv snapshot failed");
                        }
                    }
                }
                Wake::Retention => {
                    for (bucket, result) in self.retain_all().await {
                        if let Err(e) = result {
                            tracing::warn!(?bucket, %e, "log retention failed");
                        }
                    }
                }
                Wake::Stop => break,
            }
        }

        self.shutdown().await;
    }

    pub async fn shutdown(&self) {
        let _pass = self.sync.lock().await;
        for hosted in self.registry.all() {
            let bucket = hosted.bucket;
            if let Err(e) = self.close(hosted).await {
                tracing::warn!(?bucket, %e, "close on shutdown failed");
            }
        }
    }

    fn kv_dir(&self, bucket: Bucket) -> PathBuf {
        self.config
            .data_dir
            .join("kv")
            .join(bucket_dir_name(bucket))
    }
}

#[derive(Debug, Clone, Copy)]
enum Action {
    Keep,
    Close,
    Destroy,
}

#[derive(Debug, Clone, Copy)]
enum Release {
    Close,
    Destroy,
}

#[derive(Debug, Default)]
pub struct Report {
    pub opened: Vec<Bucket>,
    pub closed: Vec<Bucket>,
    pub destroyed: Vec<Bucket>,
    pub failed: Vec<(Bucket, Error)>,
}

impl Report {
    pub fn is_quiet(&self) -> bool {
        self.opened.is_empty()
            && self.closed.is_empty()
            && self.destroyed.is_empty()
            && self.failed.is_empty()
    }
}

fn bucket_dir_name(bucket: Bucket) -> String {
    match bucket.partition() {
        Some(partition) => format!(
            "t{}-p{}-b{}",
            bucket.table().0,
            partition.0,
            bucket.bucket().0
        ),
        None => format!("t{}-b{}", bucket.table().0, bucket.bucket().0),
    }
}

fn remote_kv_dir(bucket: Bucket) -> Path {
    Path::from(format!("kv/{}", bucket_dir_name(bucket)))
}
