//! The coordinator itself: leadership, catalog commands with ignore-if flags, reconciliation and the run loop.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mink_common::Clock;
use mink_common::sync::lock;
use mink_metadata::{
    Command, CommandSink, Error as MetadataError, LakeSnapshotRow, OffsetsRow, Outcome, State,
    TableRow, View, ViewPublisher,
};
use mink_table::{
    Bucket, BucketId, Change, Descriptor, Id, PartitionId, PartitionName, PartitionSpec, Path,
    SchemaId,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::assign::{assign, elect, leader_load, orphaned};
use crate::lake::{self, LakeCatalog, NoLakeCatalog};
use crate::membership::Membership;
use crate::snapshots::{SnapshotCleaner, excess};
use crate::tiering::{self, Manager, Table};
use crate::{Error, partition, rebalance};

#[derive(Debug, Clone)]
pub struct Config {
    pub default_bucket_count: u32,
    pub snapshots_retained: usize,
    pub auto_partition_interval: Duration,
    pub tiering_timeout: Duration,
    pub producer_offsets_ttl: Duration,
    pub producer_offsets_cleanup_interval: Duration,
    pub tick: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            default_bucket_count: 1,
            snapshots_retained: 2,
            auto_partition_interval: Duration::from_secs(10 * 60),
            tiering_timeout: tiering::DEFAULT_TIMEOUT,
            producer_offsets_ttl: Duration::from_secs(24 * 60 * 60),
            producer_offsets_cleanup_interval: Duration::from_secs(60 * 60),
            tick: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    pub buckets_reled: usize,
    pub snapshots_dropped: usize,
    pub partitions_created: usize,
    pub partitions_dropped: usize,
    pub producer_offsets_expired: usize,
    pub group_offsets_expired: usize,
}

pub struct Coordinator {
    node_id: i32,
    address: String,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
    membership: Arc<dyn Membership>,
    cleaner: Arc<dyn SnapshotCleaner>,
    lake: Arc<dyn LakeCatalog>,
    clock: Arc<dyn Clock>,
    config: Config,
    epoch: AtomicI32,
    tiering: Mutex<Manager>,
    last_auto_partition_ms: Mutex<Option<i64>>,
    last_producer_offsets_sweep_ms: Mutex<Option<i64>>,
}

impl Coordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        address: impl Into<String>,
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        membership: Arc<dyn Membership>,
        cleaner: Arc<dyn SnapshotCleaner>,
        clock: Arc<dyn Clock>,
        config: Config,
    ) -> Self {
        Coordinator {
            node_id,
            address: address.into(),
            sink,
            views,
            membership,
            cleaner,
            lake: Arc::new(NoLakeCatalog),
            clock,
            tiering: Mutex::new(Manager::new(config.tiering_timeout)),
            config,
            epoch: AtomicI32::new(0),
            last_auto_partition_ms: Mutex::new(None),
            last_producer_offsets_sweep_ms: Mutex::new(None),
        }
    }

    pub fn with_lake_catalog(mut self, lake: Arc<dyn LakeCatalog>) -> Self {
        self.lake = lake;
        self
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn is_leader(&self) -> bool {
        self.epoch.load(Ordering::Acquire) > 0
    }

    pub async fn become_leader(&self) -> Result<i32, Error> {
        let view = self.views.load();
        let epoch = view
            .state
            .catalog
            .coordinator
            .as_ref()
            .map_or(1, |row| row.epoch + 1);
        self.propose(Command::RegisterCoordinator {
            node_id: self.node_id,
            epoch,
            address: self.address.clone(),
        })
        .await?;
        self.epoch.store(epoch, Ordering::Release);

        let now = self.clock.millis();
        let mut tiering = lock(&self.tiering);
        *tiering = Manager::new(self.config.tiering_timeout);
        for (path, table) in view.state.catalog.tables.iter() {
            if table.descriptor.options().lake.is_some() {
                tiering.restore(
                    table.table_id,
                    path.clone(),
                    table.descriptor.options().lake_freshness,
                    now,
                    now,
                );
            }
        }

        Ok(epoch)
    }

    pub fn resign(&self) {
        self.epoch.store(0, Ordering::Release);
    }

    fn registered_epoch(&self, view: &View) -> Result<i32, Error> {
        let registered = view
            .state
            .catalog
            .coordinator
            .as_ref()
            .ok_or(Error::NoCoordinator)?;
        let own = self.epoch.load(Ordering::Acquire);
        if own > 0 && own != registered.epoch {
            return Err(MetadataError::CoordinatorFenced {
                current: registered.epoch,
                given: own,
            }
            .into());
        }

        Ok(registered.epoch)
    }

    pub async fn create_database(
        &self,
        name: &str,
        comment: Option<String>,
        custom: BTreeMap<String, String>,
        ignore_if_exists: bool,
    ) -> Result<(), Error> {
        let result = self
            .propose(Command::CreateDatabase {
                name: name.to_owned(),
                comment,
                custom,
                now_ms: self.clock.millis(),
            })
            .await
            .map(drop);

        or_default(result, ignore_if_exists, |e| {
            matches!(e, MetadataError::DatabaseExists { .. })
        })
    }

    pub async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<Vec<u64>, Error> {
        let mut streams = Vec::new();
        if cascade {
            let view = self.views.load();
            let tables: Vec<Path> = view
                .state
                .catalog
                .tables
                .keys()
                .filter(|path| path.database().as_str() == name)
                .cloned()
                .collect();
            for path in tables {
                streams.extend(self.drop_table(&path, true).await?);
            }
        }

        let result = self
            .propose(Command::DropDatabase {
                name: name.to_owned(),
            })
            .await
            .map(drop);
        or_default(result, ignore_if_not_exists, |e| {
            matches!(e, MetadataError::DatabaseNotExist { .. })
        })?;

        Ok(streams)
    }

    pub async fn create_table(
        &self,
        path: &Path,
        descriptor: &Descriptor,
        ignore_if_exists: bool,
    ) -> Result<Option<Id>, Error> {
        let descriptor = match descriptor.bucket_count() {
            Some(_) => descriptor.clone(),
            None => descriptor.with_bucket_count(self.config.default_bucket_count),
        };
        let view = self.views.load();
        let coordinator_epoch = self.registered_epoch(&view)?;
        if ignore_if_exists && view.state.catalog.table(path).is_ok() {
            return Ok(None);
        }

        let leaders = if descriptor.is_partitioned() {
            BTreeMap::new()
        } else {
            self.place(&view.state, descriptor.bucket_count().expect("defaulted"))
                .await?
        };
        let created = if descriptor.options().lake.is_some() {
            self.lake.create_table(path, &descriptor).await?
        } else if descriptor.options().lake_attach {
            return Err(Error::Table(mink_table::Error::AttachWithoutLake));
        } else {
            lake::Created::FRESH
        };
        let descriptor = created.descriptor.unwrap_or(descriptor);

        let result = self
            .propose_id(Command::CreateTable {
                path: path.clone(),
                descriptor: descriptor.clone(),
                leaders,
                coordinator_epoch,
                now_ms: self.clock.millis(),
            })
            .await
            .map(|id| Some(Id(id)));
        let id = or_default(result, ignore_if_exists, |e| {
            matches!(e, MetadataError::TableExists { .. })
        })?;

        if let Some(id) = id
            && descriptor.options().lake.is_some()
        {
            if let Some(snapshot_id) = created.baseline_snapshot_id {
                self.commit_lake_snapshot(
                    id,
                    LakeSnapshotRow {
                        snapshot_id,
                        bucket_log_end_offset: BTreeMap::new(),
                    },
                )
                .await?;
                tracing::info!(table = %path, snapshot_id, "attached to existing lake table");
            }
            lock(&self.tiering).add(
                id,
                path.clone(),
                descriptor.options().lake_freshness,
                self.clock.millis(),
            );
        }

        Ok(id)
    }

    pub async fn drop_table(
        &self,
        path: &Path,
        ignore_if_not_exists: bool,
    ) -> Result<Vec<u64>, Error> {
        let id = self
            .views
            .load()
            .state
            .catalog
            .table(path)
            .ok()
            .map(|row| row.table_id);

        let result = self
            .propose_ids(Command::DropTable { path: path.clone() })
            .await;
        let streams = or_default(result, ignore_if_not_exists, |e| {
            matches!(e, MetadataError::TableNotExist { .. })
        })?;

        if let Some(id) = id {
            lock(&self.tiering).remove(id);
        }

        Ok(streams)
    }

    pub async fn alter_table(
        &self,
        path: &Path,
        changes: &[Change],
        ignore_if_not_exists: bool,
    ) -> Result<Option<SchemaId>, Error> {
        let view = self.views.load();
        let table = match view.state.catalog.table(path) {
            Ok(table) => table,
            Err(MetadataError::TableNotExist { .. }) if ignore_if_not_exists => {
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        let current = &table.descriptor;
        let altered = mink_table::alter_table(current, changes, self.lake.format())?;
        if altered == *current {
            tracing::info!(table = %path, "alter table changed nothing");
            return Ok(Some(table.latest_schema_id()));
        }

        match (current.options().lake, altered.options().lake) {
            (None, Some(_)) => {
                self.lake.create_table(path, &altered).await?;
            }
            (Some(_), Some(_)) => self.lake.alter_table(path, current, &altered).await?,
            _ => {}
        }

        let schema_id = self
            .propose_id(Command::AlterTable {
                path: path.clone(),
                descriptor: altered.clone(),
                now_ms: self.clock.millis(),
            })
            .await?;

        let now = self.clock.millis();
        let mut tiering = lock(&self.tiering);
        match (current.options().lake, altered.options().lake) {
            (None, Some(_)) => tiering.add(
                table.table_id,
                path.clone(),
                altered.options().lake_freshness,
                now,
            ),
            (Some(_), None) => tiering.remove(table.table_id),
            (Some(_), Some(_))
                if current.options().lake_freshness != altered.options().lake_freshness =>
            {
                tiering.update_freshness(table.table_id, altered.options().lake_freshness)
            }
            _ => {}
        }

        Ok(Some(SchemaId(schema_id as u32)))
    }

    pub async fn create_partition(
        &self,
        path: &Path,
        spec: &PartitionSpec,
        ignore_if_exists: bool,
    ) -> Result<Option<PartitionId>, Error> {
        let view = self.views.load();
        let (table, name) = partition_of(&view, path, spec)?;
        let bucket_count = table
            .descriptor
            .bucket_count()
            .expect("resolved at creation");

        self.create_named_partition(&view, path, name, bucket_count, ignore_if_exists)
            .await
    }

    async fn create_named_partition(
        &self,
        view: &View,
        path: &Path,
        name: PartitionName,
        bucket_count: u32,
        ignore_if_exists: bool,
    ) -> Result<Option<PartitionId>, Error> {
        let coordinator_epoch = self.registered_epoch(view)?;
        let leaders = self.place(&view.state, bucket_count).await?;
        let result = self
            .propose_id(Command::CreatePartition {
                path: path.clone(),
                name,
                leaders,
                coordinator_epoch,
                now_ms: self.clock.millis(),
            })
            .await
            .map(|id| Some(PartitionId(id)));

        or_default(result, ignore_if_exists, |e| {
            matches!(e, MetadataError::PartitionExists { .. })
        })
    }

    pub async fn drop_partition(
        &self,
        path: &Path,
        spec: &PartitionSpec,
        ignore_if_not_exists: bool,
    ) -> Result<Vec<u64>, Error> {
        let view = self.views.load();
        let (_, name) = partition_of(&view, path, spec)?;

        self.drop_named_partition(path, name, ignore_if_not_exists)
            .await
    }

    async fn drop_named_partition(
        &self,
        path: &Path,
        name: PartitionName,
        ignore_if_not_exists: bool,
    ) -> Result<Vec<u64>, Error> {
        let result = self
            .propose_ids(Command::DropPartition {
                path: path.clone(),
                name,
            })
            .await;

        or_default(result, ignore_if_not_exists, |e| {
            matches!(e, MetadataError::PartitionNotExist { .. })
        })
    }

    async fn place(
        &self,
        state: &State,
        bucket_count: u32,
    ) -> Result<BTreeMap<BucketId, i32>, Error> {
        let live = self.live(state).await?;
        if live.is_empty() {
            return Err(Error::NoLiveNodes);
        }

        let nodes: Vec<i32> = live.into_iter().collect();
        let start = (self.clock.millis().unsigned_abs() as usize) % nodes.len();

        Ok(assign(bucket_count, &nodes, start))
    }

    async fn live(&self, state: &State) -> Result<BTreeSet<i32>, Error> {
        let live = self.membership.live_nodes().await?;

        Ok(live
            .into_iter()
            .filter(|node| state.nodes.contains_key(node))
            .collect())
    }

    pub async fn reconcile(&self) -> Result<Report, Error> {
        let view = self.views.load();
        let epoch = self.registered_epoch(&view)?;
        if !self.is_leader() {
            return Err(Error::NoCoordinator);
        }

        let now = self.clock.millis();
        let live = self.live(&view.state).await?;
        let mut report = Report {
            buckets_reled: self.relead_orphans(&view.state, &live, epoch).await?,
            snapshots_dropped: self.prune_snapshots(&view.state).await?,
            ..Report::default()
        };

        let due = (*lock(&self.last_auto_partition_ms)).is_none_or(|last| {
            now - last >= self.config.auto_partition_interval.as_millis() as i64
        });
        if due {
            let (created, dropped) = self.auto_partition(&view, now, false).await?;
            report.partitions_created = created;
            report.partitions_dropped = dropped;
            *lock(&self.last_auto_partition_ms) = Some(now);
        }

        let due = (*lock(&self.last_producer_offsets_sweep_ms)).is_none_or(|last| {
            now - last >= self.config.producer_offsets_cleanup_interval.as_millis() as i64
        });
        if due {
            report.producer_offsets_expired =
                self.expire_producer_offsets(&view.state, now).await?;
            report.group_offsets_expired = self.expire_group_offsets(&view.state, now).await?;
            *lock(&self.last_producer_offsets_sweep_ms) = Some(now);
        }

        self.sync_tiering(&view.state, now);

        Ok(report)
    }

    async fn expire_producer_offsets(&self, state: &State, now_ms: i64) -> Result<usize, Error> {
        if state
            .catalog
            .expired_producer_offsets(now_ms)
            .next()
            .is_none()
        {
            return Ok(0);
        }

        match self
            .propose(Command::ExpireProducerOffsets { now_ms })
            .await?
        {
            Outcome::Count(count) => Ok(count as usize),
            other => Err(unexpected(other)),
        }
    }

    async fn expire_group_offsets(&self, state: &State, now_ms: i64) -> Result<usize, Error> {
        if state.catalog.expired_group_offsets(now_ms).next().is_none() {
            return Ok(0);
        }

        match self.propose(Command::ExpireGroupOffsets { now_ms }).await? {
            Outcome::Count(count) => Ok(count as usize),
            other => Err(unexpected(other)),
        }
    }

    async fn relead_orphans(
        &self,
        state: &State,
        live: &BTreeSet<i32>,
        coordinator_epoch: i32,
    ) -> Result<usize, Error> {
        let orphans = orphaned(state, live);
        if orphans.is_empty() {
            return Ok(0);
        }

        let mut load = leader_load(state, live);
        let mut moved = 0;
        for (bucket, from) in orphans {
            let Some(to) = elect(&load) else {
                tracing::warn!(?bucket, from, "no live node to lead bucket");
                break;
            };
            self.lead(bucket, to, coordinator_epoch).await?;
            *load.get_mut(&to).expect("elected from load") += 1;
            moved += 1;
        }

        Ok(moved)
    }

    async fn lead(
        &self,
        bucket: Bucket,
        node_id: i32,
        coordinator_epoch: i32,
    ) -> Result<i32, Error> {
        let epoch = self
            .propose_id(Command::LeadBucket {
                bucket,
                node_id,
                coordinator_epoch,
            })
            .await?;

        Ok(epoch as i32)
    }

    pub async fn rebalance(&self) -> Result<Vec<rebalance::Move>, Error> {
        let view = self.views.load();
        let epoch = self.registered_epoch(&view)?;
        let live = self.live(&view.state).await?;
        let moves = rebalance::plan(&view.state, &live);
        for m in &moves {
            self.lead(m.bucket, m.to, epoch).await?;
        }

        Ok(moves)
    }

    async fn prune_snapshots(&self, state: &State) -> Result<usize, Error> {
        let mut dropped = 0;
        for (bucket, (drop, keep)) in excess(state, self.config.snapshots_retained) {
            for snapshot in drop {
                self.propose(Command::DropKvSnapshot {
                    bucket,
                    snapshot_id: snapshot.snapshot_id,
                })
                .await?;
                self.cleaner.discard(bucket, &snapshot, &keep).await?;
                dropped += 1;
            }
        }

        Ok(dropped)
    }

    pub async fn auto_partition(
        &self,
        view: &View,
        now_ms: i64,
        forced: bool,
    ) -> Result<(usize, usize), Error> {
        let (mut created, mut dropped) = (0, 0);
        for (path, table) in view.state.catalog.tables.iter() {
            if table.descriptor.options().auto_partition.is_none() {
                continue;
            }
            let existing: Vec<PartitionName> = view
                .state
                .catalog
                .partitions
                .iter()
                .filter(|((id, _), _)| *id == table.table_id)
                .map(|(_, row)| row.name.clone())
                .collect();
            let plan =
                partition::plan(table.table_id, &table.descriptor, &existing, now_ms, forced)?;

            for name in plan.drop {
                match self.drop_named_partition(path, name.clone(), true).await {
                    Ok(_) => dropped += 1,
                    Err(e) => tracing::warn!(%path, %name, %e, "auto partition drop failed"),
                }
            }
            let bucket_count = table
                .descriptor
                .bucket_count()
                .expect("resolved at creation");
            for name in plan.create {
                match self
                    .create_named_partition(view, path, name.clone(), bucket_count, true)
                    .await
                {
                    Ok(_) => created += 1,
                    Err(e) => tracing::warn!(%path, %name, %e, "auto partition create failed"),
                }
            }
        }

        Ok((created, dropped))
    }

    fn sync_tiering(&self, state: &State, now_ms: i64) {
        let mut tiering = lock(&self.tiering);
        let mut present = BTreeSet::new();
        for (path, table) in state.catalog.tables.iter() {
            let options = table.descriptor.options();
            if options.lake.is_none() {
                continue;
            }
            present.insert(table.table_id);
            if tiering.contains(table.table_id) {
                tiering.update_freshness(table.table_id, options.lake_freshness);
            } else {
                tiering.add(table.table_id, path.clone(), options.lake_freshness, now_ms);
            }
        }

        let gone: Vec<Id> = tiering
            .table_ids()
            .filter(|id| !present.contains(id))
            .collect();
        for id in gone {
            tiering.remove(id);
        }

        tiering.tick(now_ms);
    }

    pub fn request_tiering(&self) -> Option<Table> {
        lock(&self.tiering).request_table(self.clock.millis())
    }

    pub fn tiering_heartbeat(&self, table_id: Id, epoch: u64) -> Result<(), Error> {
        lock(&self.tiering).heartbeat(table_id, epoch, self.clock.millis())
    }

    pub fn finish_tiering(&self, table_id: Id, epoch: u64, forced: bool) -> Result<(), Error> {
        lock(&self.tiering).finish(table_id, epoch, forced, self.clock.millis())
    }

    pub fn fail_tiering(&self, table_id: Id, epoch: u64) -> Result<(), Error> {
        lock(&self.tiering).fail(table_id, epoch, self.clock.millis())
    }

    pub fn tiering_state(&self, table_id: Id) -> Option<tiering::State> {
        lock(&self.tiering).state(table_id)
    }

    pub fn tiering_status(&self) -> Vec<tiering::Status> {
        lock(&self.tiering).status()
    }

    pub async fn live_nodes(&self) -> Result<BTreeSet<i32>, Error> {
        let view = self.views.load();
        self.live(&view.state).await
    }

    pub async fn commit_lake_snapshot(
        &self,
        table_id: Id,
        snapshot: LakeSnapshotRow,
    ) -> Result<(), Error> {
        self.propose(Command::CommitLakeSnapshot { table_id, snapshot })
            .await?;

        Ok(())
    }

    pub fn lake_snapshot(&self, table_id: Id) -> Option<LakeSnapshotRow> {
        self.views.load().state.catalog.lake.get(&table_id).cloned()
    }

    pub async fn register_producer_offsets(
        &self,
        producer_id: &str,
        offsets: BTreeMap<Bucket, i64>,
        ttl: Option<Duration>,
    ) -> Result<bool, Error> {
        let now_ms = self.clock.millis();
        let ttl = ttl.unwrap_or(self.config.producer_offsets_ttl);
        let expires_ms = now_ms.saturating_add(i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX));

        match self
            .propose(Command::RegisterProducerOffsets {
                producer_id: producer_id.to_owned(),
                offsets,
                expires_ms,
                now_ms,
            })
            .await?
        {
            Outcome::Bool(created) => Ok(created),
            other => Err(unexpected(other)),
        }
    }

    pub fn producer_offsets(&self, producer_id: &str) -> Option<OffsetsRow> {
        self.views
            .load()
            .state
            .catalog
            .producer_offsets(producer_id, self.clock.millis())
            .cloned()
    }

    pub async fn delete_producer_offsets(&self, producer_id: &str) -> Result<(), Error> {
        self.propose(Command::DeleteProducerOffsets {
            producer_id: producer_id.to_owned(),
        })
        .await?;

        Ok(())
    }

    pub async fn commit_group_offsets(
        &self,
        group: &str,
        offsets: BTreeMap<Bucket, i64>,
        ttl: Duration,
    ) -> Result<(), Error> {
        let now_ms = self.clock.millis();
        let expires_ms = now_ms.saturating_add(i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX));
        self.propose(Command::CommitGroupOffsets {
            group: group.to_owned(),
            offsets,
            expires_ms,
        })
        .await?;

        Ok(())
    }

    pub fn group_offsets(&self, group: &str) -> Option<OffsetsRow> {
        self.views
            .load()
            .state
            .catalog
            .group_offsets(group, self.clock.millis())
            .cloned()
    }

    pub async fn delete_group_offsets(&self, group: &str) -> Result<(), Error> {
        self.propose(Command::DeleteGroupOffsets {
            group: group.to_owned(),
        })
        .await?;

        Ok(())
    }

    pub fn spawn(self: Arc<Self>, mut leadership: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if *leadership.borrow() {
                    if !self.is_leader() {
                        match self.become_leader().await {
                            Ok(epoch) => tracing::info!(epoch, "coordinator leading"),
                            Err(e) => tracing::warn!(%e, "could not register as coordinator"),
                        }
                    }
                    if self.is_leader()
                        && let Err(e) = self.reconcile().await
                    {
                        tracing::warn!(%e, "coordinator reconcile failed");
                        if matches!(e, Error::Metadata(MetadataError::CoordinatorFenced { .. })) {
                            self.resign();
                        }
                    }
                } else if self.is_leader() {
                    self.resign();
                    tracing::info!("coordinator resigned");
                }

                tokio::select! {
                    changed = leadership.changed() => {
                        if changed.is_err() {
                            self.resign();
                            return;
                        }
                    }
                    _ = tokio::time::sleep(self.config.tick) => {}
                }
            }
        })
    }

    async fn propose(&self, command: Command) -> Result<Outcome, Error> {
        Ok(self.sink.propose(command).await?.result)
    }

    async fn propose_id(&self, command: Command) -> Result<u64, Error> {
        match self.propose(command).await? {
            Outcome::Id(id) => Ok(id),
            other => Err(unexpected(other)),
        }
    }

    async fn propose_ids(&self, command: Command) -> Result<Vec<u64>, Error> {
        match self.propose(command).await? {
            Outcome::Ids(ids) => Ok(ids),
            other => Err(unexpected(other)),
        }
    }
}

fn partition_of<'a>(
    view: &'a View,
    path: &Path,
    spec: &PartitionSpec,
) -> Result<(&'a TableRow, PartitionName), Error> {
    let table = view.state.catalog.table(path)?;
    if !table.descriptor.is_partitioned() {
        return Err(Error::NotPartitioned(path.clone()));
    }
    let name = spec.resolve(table.descriptor.partition_keys())?.name();

    Ok((table, name))
}

fn or_default<T: Default>(
    result: Result<T, Error>,
    ignore: bool,
    ignored: impl Fn(&MetadataError) -> bool,
) -> Result<T, Error> {
    match result {
        Err(Error::Metadata(e)) if ignore && ignored(&e) => Ok(T::default()),
        other => other,
    }
}

fn unexpected(result: Outcome) -> Error {
    MetadataError::Unexpected {
        message: format!("unexpected result {result:?}"),
    }
    .into()
}
