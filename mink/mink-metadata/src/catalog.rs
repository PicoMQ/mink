//! The table catalog rows and the handlers that create, alter, drop and lease databases, tables, partitions,
//! buckets, snapshots, counters and producer offsets.

use std::collections::BTreeMap;

use im::OrdMap;
use mink_table::{
    Bucket, BucketId, Descriptor, Id, PartitionId, PartitionName, Path, Schema, SchemaId,
};

use crate::Error;
use crate::command::Outcome;
use crate::state::State;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseRow {
    pub comment: Option<String>,
    pub custom: BTreeMap<String, String>,
    pub created_ms: i64,
    pub modified_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub table_id: Id,
    pub descriptor: Descriptor,
    pub schemas: Vec<Schema>,
    pub created_ms: i64,
    pub modified_ms: i64,
}

impl TableRow {
    pub fn latest_schema_id(&self) -> SchemaId {
        SchemaId((self.schemas.len() - 1) as u32)
    }

    pub fn schema(&self, id: SchemaId) -> Option<&Schema> {
        self.schemas.get(id.0 as usize)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRow {
    pub partition_id: PartitionId,
    pub table_id: Id,
    pub name: PartitionName,
    pub created_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketRow {
    pub stream_id: u64,
    pub leader: i32,
    pub leader_epoch: i32,
    pub coordinator_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvSnapshotRow {
    pub snapshot_id: u64,
    pub log_offset: i64,
    pub row_count: i64,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeSnapshotRow {
    pub snapshot_id: i64,
    pub bucket_log_end_offset: BTreeMap<Bucket, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetsRow {
    pub expires_ms: i64,
    pub offsets: BTreeMap<Bucket, i64>,
}

impl OffsetsRow {
    pub fn is_expired(&self, now_ms: i64) -> bool {
        now_ms > self.expires_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorRow {
    pub node_id: i32,
    pub epoch: i32,
    pub address: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Counter {
    SnapshotId(Bucket),
    AutoIncrement { table_id: Id, column_id: u32 },
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Catalog {
    pub next_table_id: u64,
    pub next_partition_id: u64,
    pub databases: OrdMap<String, DatabaseRow>,
    pub tables: OrdMap<Path, TableRow>,
    pub table_paths: OrdMap<Id, Path>,
    pub partitions: OrdMap<(Id, PartitionName), PartitionRow>,
    pub partition_names: OrdMap<PartitionId, (Id, PartitionName)>,
    pub buckets: OrdMap<Bucket, BucketRow>,
    pub kv_snapshots: OrdMap<(Bucket, u64), KvSnapshotRow>,
    pub lake: OrdMap<Id, LakeSnapshotRow>,
    pub counters: OrdMap<Counter, u64>,
    pub coordinator: Option<CoordinatorRow>,
    pub producer_offsets: OrdMap<String, OffsetsRow>,
    pub producer_offsets_by_expiry: OrdMap<(i64, String), ()>,
    pub group_offsets: OrdMap<String, OffsetsRow>,
    pub group_offsets_by_expiry: OrdMap<(i64, String), ()>,
}

impl Catalog {
    pub fn table(&self, path: &Path) -> Result<&TableRow, Error> {
        self.tables.get(path).ok_or_else(|| Error::TableNotExist {
            path: path.to_string(),
        })
    }

    pub fn table_by_id(&self, table_id: Id) -> Option<&TableRow> {
        self.table_paths
            .get(&table_id)
            .and_then(|path| self.tables.get(path))
    }

    pub fn partitions_of(&self, table_id: Id) -> impl Iterator<Item = &PartitionRow> {
        self.partitions
            .iter()
            .filter(move |(key, _)| key.0 == table_id)
            .map(|(_, row)| row)
    }

    pub fn buckets_of(
        &self,
        table_id: Id,
        partition_id: Option<PartitionId>,
    ) -> impl Iterator<Item = (&Bucket, &BucketRow)> {
        let low = Bucket::of(table_id, partition_id, BucketId(0));
        let high = Bucket::of(table_id, partition_id, BucketId(u32::MAX));
        self.buckets.range(low..=high)
    }

    pub fn kv_snapshots_of(&self, bucket: Bucket) -> impl Iterator<Item = &KvSnapshotRow> {
        self.kv_snapshots
            .range((bucket, 0)..=(bucket, u64::MAX))
            .map(|(_, row)| row)
    }

    pub fn latest_kv_snapshot(&self, bucket: Bucket) -> Option<&KvSnapshotRow> {
        self.kv_snapshots_of(bucket).last()
    }

    pub fn producer_offsets(&self, producer_id: &str, now_ms: i64) -> Option<&OffsetsRow> {
        self.producer_offsets
            .get(producer_id)
            .filter(|row| !row.is_expired(now_ms))
    }

    pub fn expired_producer_offsets(&self, now_ms: i64) -> impl Iterator<Item = &str> {
        expired(&self.producer_offsets_by_expiry, now_ms)
    }

    pub fn group_offsets(&self, group: &str, now_ms: i64) -> Option<&OffsetsRow> {
        self.group_offsets
            .get(group)
            .filter(|row| !row.is_expired(now_ms))
    }

    pub fn expired_group_offsets(&self, now_ms: i64) -> impl Iterator<Item = &str> {
        expired(&self.group_offsets_by_expiry, now_ms)
    }
}

fn expired(by_expiry: &OrdMap<(i64, String), ()>, now_ms: i64) -> impl Iterator<Item = &str> {
    by_expiry
        .range(..(now_ms, String::new()))
        .map(|((_, id), ())| id.as_str())
}

pub(crate) fn create_database(
    state: &mut State,
    name: &str,
    comment: Option<&str>,
    custom: &BTreeMap<String, String>,
    now_ms: i64,
) -> Result<Outcome, Error> {
    let catalog = &mut state.catalog;
    if catalog.databases.contains_key(name) {
        return Err(Error::DatabaseExists {
            name: name.to_owned(),
        });
    }

    catalog.databases.insert(
        name.to_owned(),
        DatabaseRow {
            comment: comment.map(str::to_owned),
            custom: custom.clone(),
            created_ms: now_ms,
            modified_ms: now_ms,
        },
    );

    Ok(Outcome::Unit)
}

pub(crate) fn drop_database(state: &mut State, name: &str) -> Result<Outcome, Error> {
    let catalog = &mut state.catalog;
    if !catalog.databases.contains_key(name) {
        return Err(Error::DatabaseNotExist {
            name: name.to_owned(),
        });
    }
    if catalog
        .tables
        .keys()
        .any(|path| path.database().as_str() == name)
    {
        return Err(Error::DatabaseNotEmpty {
            name: name.to_owned(),
        });
    }

    catalog.databases.remove(name);

    Ok(Outcome::Unit)
}

pub(crate) fn create_table(
    state: &mut State,
    path: &Path,
    descriptor: &Descriptor,
    leaders: &BTreeMap<BucketId, i32>,
    coordinator_epoch: i32,
    now_ms: i64,
) -> Result<Outcome, Error> {
    if !state
        .catalog
        .databases
        .contains_key(path.database().as_str())
    {
        return Err(Error::DatabaseNotExist {
            name: path.database().to_string(),
        });
    }
    if state.catalog.tables.contains_key(path) {
        return Err(Error::TableExists {
            path: path.to_string(),
        });
    }
    if descriptor.is_partitioned() {
        if !leaders.is_empty() {
            return Err(Error::Unexpected {
                message: format!("partitioned table {path} takes its buckets per partition"),
            });
        }
    } else {
        check_leaders(path, descriptor, leaders)?;
    }

    let id = Id(state.catalog.next_table_id);
    state.catalog.next_table_id += 1;
    state.catalog.tables.insert(
        path.clone(),
        TableRow {
            table_id: id,
            descriptor: descriptor.clone(),
            schemas: vec![descriptor.schema().clone()],
            created_ms: now_ms,
            modified_ms: now_ms,
        },
    );
    state.catalog.table_paths.insert(id, path.clone());
    create_buckets(state, id, None, leaders, coordinator_epoch);

    Ok(Outcome::Id(id.0))
}

pub(crate) fn drop_table(state: &mut State, path: &Path) -> Result<Outcome, Error> {
    let id = state.catalog.table(path)?.table_id;
    let partitions: Vec<(Id, PartitionName)> = state
        .catalog
        .partitions_of(id)
        .map(|row| (id, row.name.clone()))
        .collect();

    let mut streams = Vec::new();
    for key in partitions {
        let row = state.catalog.partitions.remove(&key).expect("listed");
        state.catalog.partition_names.remove(&row.partition_id);
        streams.extend(drop_buckets(state, id, Some(row.partition_id)));
    }
    streams.extend(drop_buckets(state, id, None));

    state.catalog.lake.remove(&id);
    state.catalog.table_paths.remove(&id);
    state.catalog.tables.remove(path);

    Ok(Outcome::Ids(streams))
}

pub(crate) fn alter_table(
    state: &mut State,
    path: &Path,
    descriptor: &Descriptor,
    now_ms: i64,
) -> Result<Outcome, Error> {
    let row = state
        .catalog
        .tables
        .get_mut(path)
        .ok_or_else(|| Error::TableNotExist {
            path: path.to_string(),
        })?;
    let current = &row.descriptor;
    let fixed = |what: &str| Error::Unexpected {
        message: format!("alter table {path}: the {what} cannot change"),
    };
    if descriptor.schema().primary_key() != current.schema().primary_key() {
        return Err(fixed("primary key"));
    }
    if descriptor.partition_keys() != current.partition_keys() {
        return Err(fixed("partition keys"));
    }
    if descriptor.bucket_keys() != current.bucket_keys() {
        return Err(fixed("bucket keys"));
    }
    if descriptor.bucket_count() != current.bucket_count() {
        return Err(fixed("bucket count"));
    }

    let latest = row.schemas.last().expect("a table has at least one schema");
    if descriptor.schema() != latest {
        let schema = descriptor.schema();
        let ids_kept = schema.columns().iter().all(|column| match column.id() {
            Some(id) if id.0 < latest.next_field_id() => {
                latest.columns().iter().any(|c| c.id() == Some(id))
            }
            Some(_) => true,
            None => false,
        });
        if !ids_kept || schema.next_field_id() < latest.next_field_id() {
            return Err(Error::Unexpected {
                message: format!("alter table {path}: field ids must be kept and never reused"),
            });
        }
        row.schemas.push(schema.clone());
    }

    row.descriptor = descriptor.clone();
    row.modified_ms = now_ms;

    Ok(Outcome::Id(row.latest_schema_id().0 as u64))
}

pub(crate) fn create_partition(
    state: &mut State,
    path: &Path,
    name: &PartitionName,
    leaders: &BTreeMap<BucketId, i32>,
    coordinator_epoch: i32,
    now_ms: i64,
) -> Result<Outcome, Error> {
    let table = state.catalog.table(path)?;
    let id = table.table_id;
    if !table.descriptor.is_partitioned() {
        return Err(Error::Unexpected {
            message: format!("table {path} is not partitioned"),
        });
    }
    check_leaders(path, &table.descriptor, leaders)?;

    let key = (id, name.clone());
    if state.catalog.partitions.contains_key(&key) {
        return Err(Error::PartitionExists {
            path: path.to_string(),
            name: name.to_string(),
        });
    }
    let partition = PartitionId(state.catalog.next_partition_id);
    state.catalog.next_partition_id += 1;
    state.catalog.partitions.insert(
        key.clone(),
        PartitionRow {
            partition_id: partition,
            table_id: id,
            name: name.clone(),
            created_ms: now_ms,
        },
    );
    state.catalog.partition_names.insert(partition, key);
    create_buckets(state, id, Some(partition), leaders, coordinator_epoch);

    Ok(Outcome::Id(partition.0))
}

pub(crate) fn drop_partition(
    state: &mut State,
    path: &Path,
    name: &PartitionName,
) -> Result<Outcome, Error> {
    let id = state.catalog.table(path)?.table_id;
    let key = (id, name.clone());
    let row = state
        .catalog
        .partitions
        .remove(&key)
        .ok_or_else(|| Error::PartitionNotExist {
            path: path.to_string(),
            name: name.to_string(),
        })?;
    state.catalog.partition_names.remove(&row.partition_id);
    let streams = drop_buckets(state, id, Some(row.partition_id));

    Ok(Outcome::Ids(streams))
}

pub(crate) fn lead_bucket(
    state: &mut State,
    bucket: Bucket,
    node_id: i32,
    coordinator_epoch: i32,
) -> Result<Outcome, Error> {
    if !state.nodes.contains_key(&node_id) {
        return Err(Error::NodeEpochMismatch {
            node_id,
            message: format!("node {node_id} is not registered"),
        });
    }
    let row = state
        .catalog
        .buckets
        .get_mut(&bucket)
        .ok_or(Error::BucketNotExist { bucket })?;
    fence_coordinator(row.coordinator_epoch, coordinator_epoch)?;

    row.leader = node_id;
    row.leader_epoch += 1;
    row.coordinator_epoch = coordinator_epoch;

    Ok(Outcome::Id(row.leader_epoch as u64))
}

pub(crate) fn commit_kv_snapshot(
    state: &mut State,
    bucket: Bucket,
    snapshot: &KvSnapshotRow,
    leader_epoch: i32,
    coordinator_epoch: i32,
) -> Result<Outcome, Error> {
    let row = state
        .catalog
        .buckets
        .get(&bucket)
        .ok_or(Error::BucketNotExist { bucket })?;
    if leader_epoch != row.leader_epoch {
        return Err(Error::LeaderFenced {
            bucket,
            current: row.leader_epoch,
            given: leader_epoch,
        });
    }
    fence_coordinator(row.coordinator_epoch, coordinator_epoch)?;

    let key = (bucket, snapshot.snapshot_id);
    if state.catalog.kv_snapshots.contains_key(&key) {
        return Err(Error::Redundant {
            message: format!(
                "snapshot {} of {bucket:?} already committed",
                snapshot.snapshot_id
            ),
        });
    }

    state.catalog.kv_snapshots.insert(key, snapshot.clone());

    Ok(Outcome::Unit)
}

pub(crate) fn drop_kv_snapshot(
    state: &mut State,
    bucket: Bucket,
    snapshot_id: u64,
) -> Result<Outcome, Error> {
    state.catalog.kv_snapshots.remove(&(bucket, snapshot_id));

    Ok(Outcome::Unit)
}

pub(crate) fn commit_lake_snapshot(
    state: &mut State,
    table_id: Id,
    snapshot: &LakeSnapshotRow,
) -> Result<Outcome, Error> {
    if state.catalog.table_by_id(table_id).is_none() {
        return Err(Error::TableNotExist {
            path: format!("id {table_id}"),
        });
    }
    if let Some(current) = state.catalog.lake.get(&table_id) {
        // Lake snapshot ids are not monotonic (Iceberg's are random); only per-bucket offsets must not regress.
        if current == snapshot {
            return Err(Error::Redundant {
                message: format!(
                    "lake snapshot {} of table {table_id} is already recorded",
                    snapshot.snapshot_id
                ),
            });
        }
        for (bucket, offset) in &snapshot.bucket_log_end_offset {
            if let Some(recorded) = current.bucket_log_end_offset.get(bucket)
                && offset < recorded
            {
                return Err(Error::Unexpected {
                    message: format!(
                        "lake snapshot {} moves bucket {bucket:?} of table {table_id} back from \
                         offset {recorded} to {offset}",
                        snapshot.snapshot_id
                    ),
                });
            }
        }
    }

    state.catalog.lake.insert(table_id, snapshot.clone());

    Ok(Outcome::Unit)
}

pub(crate) fn allocate(state: &mut State, counter: Counter, count: u64) -> Result<Outcome, Error> {
    if count == 0 {
        return Err(Error::Unexpected {
            message: "allocate count must be positive".into(),
        });
    }

    let next = state.catalog.counters.get(&counter).copied().unwrap_or(0);
    state.catalog.counters.insert(counter, next + count);

    Ok(Outcome::Id(next))
}

pub(crate) fn register_coordinator(
    state: &mut State,
    node_id: i32,
    epoch: i32,
    address: &str,
) -> Result<Outcome, Error> {
    if let Some(current) = &state.catalog.coordinator {
        fence_coordinator(current.epoch, epoch)?;
    }

    state.catalog.coordinator = Some(CoordinatorRow {
        node_id,
        epoch,
        address: address.to_owned(),
    });

    Ok(Outcome::Unit)
}

pub(crate) fn register_producer_offsets(
    state: &mut State,
    producer_id: &str,
    offsets: &BTreeMap<Bucket, i64>,
    expires_ms: i64,
    now_ms: i64,
) -> Result<Outcome, Error> {
    mink_table::Name::new(producer_id).map_err(|e| Error::InvalidArgument {
        message: format!("invalid producer id `{producer_id}`: {e}"),
    })?;
    if expires_ms < now_ms {
        return Err(Error::InvalidArgument {
            message: format!(
                "producer `{producer_id}` offsets expire at {expires_ms}, before {now_ms}"
            ),
        });
    }
    check_offsets(state, producer_id, offsets)?;

    let catalog = &mut state.catalog;
    if let Some(current) = catalog.producer_offsets.get(producer_id) {
        if !current.is_expired(now_ms) {
            return Ok(Outcome::Bool(false));
        }
        catalog
            .producer_offsets_by_expiry
            .remove(&(current.expires_ms, producer_id.to_owned()));
    }

    catalog.producer_offsets.insert(
        producer_id.to_owned(),
        OffsetsRow {
            expires_ms,
            offsets: offsets.clone(),
        },
    );
    catalog
        .producer_offsets_by_expiry
        .insert((expires_ms, producer_id.to_owned()), ());

    Ok(Outcome::Bool(true))
}

pub(crate) fn delete_producer_offsets(
    state: &mut State,
    producer_id: &str,
) -> Result<Outcome, Error> {
    let catalog = &mut state.catalog;
    remove_offsets(
        &mut catalog.producer_offsets,
        &mut catalog.producer_offsets_by_expiry,
        producer_id,
    );

    Ok(Outcome::Unit)
}

pub(crate) fn expire_producer_offsets(state: &mut State, now_ms: i64) -> Result<Outcome, Error> {
    let expired: Vec<String> = state
        .catalog
        .expired_producer_offsets(now_ms)
        .map(str::to_owned)
        .collect();
    for producer_id in &expired {
        delete_producer_offsets(state, producer_id)?;
    }

    Ok(Outcome::Count(expired.len() as u64))
}

pub(crate) fn commit_group_offsets(
    state: &mut State,
    group: &str,
    offsets: &BTreeMap<Bucket, i64>,
    expires_ms: i64,
) -> Result<Outcome, Error> {
    if group.is_empty() {
        return Err(Error::InvalidArgument {
            message: "group id is empty".to_owned(),
        });
    }
    check_offsets(state, group, offsets)?;

    let catalog = &mut state.catalog;
    let mut merged = catalog
        .group_offsets
        .get(group)
        .map(|row| row.offsets.clone())
        .unwrap_or_default();
    merged.extend(offsets.iter().map(|(bucket, offset)| (*bucket, *offset)));
    remove_offsets(
        &mut catalog.group_offsets,
        &mut catalog.group_offsets_by_expiry,
        group,
    );
    catalog.group_offsets.insert(
        group.to_owned(),
        OffsetsRow {
            expires_ms,
            offsets: merged,
        },
    );
    catalog
        .group_offsets_by_expiry
        .insert((expires_ms, group.to_owned()), ());

    Ok(Outcome::Unit)
}

pub(crate) fn delete_group_offsets(state: &mut State, group: &str) -> Result<Outcome, Error> {
    let catalog = &mut state.catalog;
    remove_offsets(
        &mut catalog.group_offsets,
        &mut catalog.group_offsets_by_expiry,
        group,
    );

    Ok(Outcome::Unit)
}

pub(crate) fn expire_group_offsets(state: &mut State, now_ms: i64) -> Result<Outcome, Error> {
    let expired: Vec<String> = state
        .catalog
        .expired_group_offsets(now_ms)
        .map(str::to_owned)
        .collect();
    for group in &expired {
        delete_group_offsets(state, group)?;
    }

    Ok(Outcome::Count(expired.len() as u64))
}

fn check_offsets(state: &State, owner: &str, offsets: &BTreeMap<Bucket, i64>) -> Result<(), Error> {
    for (bucket, offset) in offsets {
        if !state.catalog.buckets.contains_key(bucket) {
            return Err(Error::BucketNotExist { bucket: *bucket });
        }
        if *offset < 0 {
            return Err(Error::InvalidArgument {
                message: format!("`{owner}` offset {offset} for {bucket:?} is negative"),
            });
        }
    }

    Ok(())
}

fn remove_offsets(
    rows: &mut OrdMap<String, OffsetsRow>,
    by_expiry: &mut OrdMap<(i64, String), ()>,
    id: &str,
) {
    if let Some(row) = rows.remove(id) {
        by_expiry.remove(&(row.expires_ms, id.to_owned()));
    }
}

fn fence_coordinator(current: i32, given: i32) -> Result<(), Error> {
    if given < current {
        return Err(Error::CoordinatorFenced { current, given });
    }

    Ok(())
}

fn check_leaders(
    path: &Path,
    descriptor: &Descriptor,
    leaders: &BTreeMap<BucketId, i32>,
) -> Result<(), Error> {
    let expected = match descriptor.bucket_count() {
        Some(count) if count > 0 => count as usize,
        _ => {
            return Err(Error::Unexpected {
                message: format!("table {path} has no resolved bucket count"),
            });
        }
    };
    if leaders.len() != expected {
        return Err(Error::Unexpected {
            message: format!(
                "table {path} declares {expected} buckets, {} leaders given",
                leaders.len()
            ),
        });
    }

    Ok(())
}

fn create_buckets(
    state: &mut State,
    table_id: Id,
    partition_id: Option<PartitionId>,
    leaders: &BTreeMap<BucketId, i32>,
    coordinator_epoch: i32,
) {
    for (&id, &leader) in leaders {
        let stream_id = state.alloc_stream();
        state.catalog.buckets.insert(
            Bucket::of(table_id, partition_id, id),
            BucketRow {
                stream_id,
                leader,
                leader_epoch: 0,
                coordinator_epoch,
            },
        );
    }
}

fn drop_buckets(state: &mut State, table_id: Id, partition_id: Option<PartitionId>) -> Vec<u64> {
    let keys: Vec<Bucket> = state
        .catalog
        .buckets_of(table_id, partition_id)
        .map(|(key, _)| *key)
        .collect();

    let mut streams = Vec::with_capacity(keys.len());
    for key in keys {
        let row = state.catalog.buckets.remove(&key).expect("listed");
        streams.push(row.stream_id);

        let snapshots: Vec<(Bucket, u64)> = state
            .catalog
            .kv_snapshots
            .range((key, 0)..=(key, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        for snapshot in snapshots {
            state.catalog.kv_snapshots.remove(&snapshot);
        }
        state.catalog.counters.remove(&Counter::SnapshotId(key));
    }

    streams
}
