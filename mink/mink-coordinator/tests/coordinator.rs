//! Coordinator behavior end to end on the local sink: catalog, leadership, snapshots, partitions and tiering.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use mink_common::{Clock, ManualClock};
use mink_coordinator::lake;
use mink_coordinator::{
    Config, Coordinator, Error, MemoryLakeCatalog, Move, NoLakeCatalog, NoopCleaner,
    SnapshotCleaner, StaticMembership, TieringState,
};
use mink_metadata::{Command, CommandSink, KvSnapshotRow, LocalSink, ViewPublisher};
use mink_table::{
    AutoPartition, Bucket, BucketId, Change, Column, Descriptor, Id, LakeFormat, Options,
    PartitionSpec, Path, PrimaryKey, Schema, SchemaId, TimeUnit,
};
use mink_types::DataType;
use tokio::time::Instant;

const NOW_MS: i64 = 1_718_452_800_000;

struct Cluster {
    sink: Arc<LocalSink>,
    views: Arc<ViewPublisher>,
    membership: Arc<StaticMembership>,
    clock: Arc<ManualClock>,
}

impl Cluster {
    async fn new(nodes: &[i32]) -> Self {
        let (sink, views) = LocalSink::new();
        let sink = Arc::new(sink);
        for node in nodes {
            sink.propose(Command::RegisterNode {
                node_id: *node,
                node_epoch: 1,
                http_address: format!("node{node}:9000"),
                slots: 1,
                protocol_addresses: BTreeMap::new(),
            })
            .await
            .unwrap();
        }
        Cluster {
            sink,
            views,
            membership: Arc::new(StaticMembership::new(nodes.iter().copied())),
            clock: Arc::new(ManualClock::new(NOW_MS)),
        }
    }

    fn coordinator(&self, node_id: i32, config: Config) -> Coordinator {
        self.coordinator_with(node_id, config, Arc::new(NoopCleaner))
    }

    fn coordinator_with(
        &self,
        node_id: i32,
        config: Config,
        cleaner: Arc<dyn SnapshotCleaner>,
    ) -> Coordinator {
        Coordinator::new(
            node_id,
            format!("node{node_id}:9000"),
            self.sink.clone(),
            self.views.clone(),
            self.membership.clone(),
            cleaner,
            self.clock.clone(),
            config,
        )
        .with_lake_catalog(Arc::new(MemoryLakeCatalog::default()))
    }

    fn leaders(&self, table_id: Id) -> BTreeMap<Bucket, i32> {
        self.views
            .load()
            .state
            .catalog
            .buckets
            .iter()
            .filter(|(bucket, _)| bucket.table() == table_id)
            .map(|(bucket, row)| (*bucket, row.leader))
            .collect()
    }

    fn partitions(&self, table_id: Id) -> Vec<String> {
        self.views
            .load()
            .state
            .catalog
            .partitions
            .iter()
            .filter(|((id, _), _)| *id == table_id)
            .map(|(_, row)| row.name.to_string())
            .collect()
    }
}

fn path(s: &str) -> Path {
    s.parse().unwrap()
}

fn schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .column(Column::new("dt", DataType::string().with_nullable(false)).unwrap())
        .primary_key(PrimaryKey::new(vec!["k".into(), "dt".into()]).unwrap())
        .build()
        .unwrap()
}

fn table(buckets: Option<u32>, options: Options) -> Descriptor {
    let mut builder = Descriptor::builder(schema()).options(options);
    if let Some(buckets) = buckets {
        builder = builder.bucket_count(buckets);
    }
    builder.build().unwrap()
}

fn partitioned(buckets: u32, options: Options) -> Descriptor {
    Descriptor::builder(schema())
        .partitioned_by(["dt"])
        .bucket_count(buckets)
        .options(options)
        .build()
        .unwrap()
}

#[tokio::test]
async fn leadership_registers_with_increasing_epochs_and_fences_the_old_one() {
    let cluster = Cluster::new(&[1, 2]).await;
    let first = cluster.coordinator(1, Config::default());
    let second = cluster.coordinator(2, Config::default());

    assert!(matches!(first.reconcile().await, Err(Error::NoCoordinator)));
    assert_eq!(first.become_leader().await.unwrap(), 1);
    first.reconcile().await.unwrap();
    assert_eq!(second.become_leader().await.unwrap(), 2);

    assert!(matches!(
        first.reconcile().await,
        Err(Error::Metadata(mink_metadata::Error::CoordinatorFenced {
            current: 2,
            given: 1
        }))
    ));
    second.reconcile().await.unwrap();
    let row = cluster
        .views
        .load()
        .state
        .catalog
        .coordinator
        .clone()
        .unwrap();
    assert_eq!(
        (row.node_id, row.epoch, row.address.as_str()),
        (2, 2, "node2:9000")
    );
}

#[tokio::test]
async fn create_table_defaults_buckets_and_spreads_leaders_over_live_nodes() {
    let cluster = Cluster::new(&[1, 2, 3]).await;
    let coordinator = cluster.coordinator(
        1,
        Config {
            default_bucket_count: 6,
            ..Config::default()
        },
    );
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), true)
        .await
        .unwrap();
    assert!(matches!(
        coordinator
            .create_database("db", None, BTreeMap::new(), false)
            .await,
        Err(Error::Metadata(mink_metadata::Error::DatabaseExists { .. }))
    ));

    cluster.membership.remove(3);
    let table_id = coordinator
        .create_table(&path("db.t"), &table(None, Options::default()), false)
        .await
        .unwrap()
        .unwrap();
    let leaders = cluster.leaders(table_id);
    assert_eq!(leaders.len(), 6, "default bucket count applied");
    let mut per_node: BTreeMap<i32, usize> = BTreeMap::new();
    for leader in leaders.values() {
        *per_node.entry(*leader).or_default() += 1;
    }
    assert_eq!(per_node, BTreeMap::from([(1, 3), (2, 3)]));

    assert!(
        coordinator
            .create_table(&path("db.t"), &table(None, Options::default()), true)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        coordinator
            .create_table(&path("db.t"), &table(None, Options::default()), false)
            .await,
        Err(Error::Metadata(mink_metadata::Error::TableExists { .. }))
    ));

    cluster.membership.set([]);
    assert!(matches!(
        coordinator
            .create_table(&path("db.u"), &table(Some(1), Options::default()), false)
            .await,
        Err(Error::NoLiveNodes)
    ));
    assert!(
        !cluster
            .views
            .load()
            .state
            .catalog
            .tables
            .contains_key(&path("db.u"))
    );

    cluster.membership.set([1, 2]);
    let streams = coordinator.drop_table(&path("db.t"), false).await.unwrap();
    assert_eq!(streams.len(), 6);
    assert!(
        coordinator
            .drop_table(&path("db.t"), true)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        coordinator.drop_table(&path("db.t"), false).await,
        Err(Error::Metadata(mink_metadata::Error::TableNotExist { .. }))
    ));
}

#[tokio::test]
async fn drop_database_cascade_takes_its_tables_along() {
    let cluster = Cluster::new(&[1]).await;
    let coordinator = cluster.coordinator(1, Config::default());
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    for name in ["db.a", "db.b"] {
        coordinator
            .create_table(&path(name), &table(Some(2), Options::default()), false)
            .await
            .unwrap();
    }
    assert!(matches!(
        coordinator.drop_database("db", false, false).await,
        Err(Error::Metadata(
            mink_metadata::Error::DatabaseNotEmpty { .. }
        ))
    ));
    let streams = coordinator.drop_database("db", false, true).await.unwrap();
    assert_eq!(streams.len(), 4);
    assert!(cluster.views.load().state.catalog.tables.is_empty());
    assert!(
        coordinator
            .drop_database("db", true, false)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn partitions_are_created_by_spec_and_dropped_by_spec() {
    let cluster = Cluster::new(&[1, 2]).await;
    let coordinator = cluster.coordinator(1, Config::default());
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table_id = coordinator
        .create_table(&path("db.p"), &partitioned(2, Options::default()), false)
        .await
        .unwrap()
        .unwrap();
    assert!(
        cluster.leaders(table_id).is_empty(),
        "partitioned tables have no buckets yet"
    );

    let spec = PartitionSpec::new(vec![("dt".into(), "20240615".parse().unwrap())]).unwrap();
    let partition_id = coordinator
        .create_partition(&path("db.p"), &spec, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cluster.partitions(table_id), vec!["20240615"]);
    let leaders = cluster.leaders(table_id);
    assert_eq!(leaders.len(), 2);
    assert!(leaders.keys().all(|b| b.partition() == Some(partition_id)));
    assert_eq!(
        leaders.values().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2])
    );

    assert!(
        coordinator
            .create_partition(&path("db.p"), &spec, true)
            .await
            .unwrap()
            .is_none()
    );
    let flat = coordinator
        .create_table(&path("db.flat"), &table(Some(1), Options::default()), false)
        .await
        .unwrap();
    assert!(flat.is_some());
    assert!(matches!(
        coordinator
            .create_partition(&path("db.flat"), &spec, false)
            .await,
        Err(Error::NotPartitioned(_))
    ));

    let streams = coordinator
        .drop_partition(&path("db.p"), &spec, false)
        .await
        .unwrap();
    assert_eq!(streams.len(), 2);
    assert!(cluster.partitions(table_id).is_empty());
    assert!(
        coordinator
            .drop_partition(&path("db.p"), &spec, true)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn dead_leaders_are_replaced_by_the_least_loaded_live_node() {
    let cluster = Cluster::new(&[1, 2, 3]).await;
    let coordinator = cluster.coordinator(1, Config::default());
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table_id = coordinator
        .create_table(&path("db.t"), &table(Some(6), Options::default()), false)
        .await
        .unwrap()
        .unwrap();
    let before = cluster.leaders(table_id);
    let epochs_before: BTreeMap<Bucket, i32> = cluster
        .views
        .load()
        .state
        .catalog
        .buckets
        .iter()
        .map(|(b, row)| (*b, row.leader_epoch))
        .collect();

    cluster.membership.remove(3);
    let report = coordinator.reconcile().await.unwrap();
    assert_eq!(report.buckets_reled, 2);
    let after = cluster.leaders(table_id);
    let view = cluster.views.load();
    for (bucket, leader) in &after {
        assert_ne!(*leader, 3);
        let row = view.state.catalog.buckets[bucket];
        if before[bucket] == 3 {
            assert_eq!(
                row.leader_epoch,
                epochs_before[bucket] + 1,
                "re-led bucket bumps epoch"
            );
        } else {
            assert_eq!(row.leader_epoch, epochs_before[bucket], "others untouched");
        }
    }
    let mut per_node: BTreeMap<i32, usize> = BTreeMap::new();
    for leader in after.values() {
        *per_node.entry(*leader).or_default() += 1;
    }
    assert_eq!(per_node, BTreeMap::from([(1, 3), (2, 3)]));

    assert_eq!(coordinator.reconcile().await.unwrap().buckets_reled, 0);
}

#[tokio::test]
async fn rebalance_moves_leaders_off_overloaded_nodes() {
    let cluster = Cluster::new(&[1]).await;
    let coordinator = cluster.coordinator(1, Config::default());
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table_id = coordinator
        .create_table(&path("db.t"), &table(Some(9), Options::default()), false)
        .await
        .unwrap()
        .unwrap();
    assert!(cluster.leaders(table_id).values().all(|l| *l == 1));

    for node in [2, 3] {
        cluster
            .sink
            .propose(Command::RegisterNode {
                node_id: node,
                node_epoch: 1,
                http_address: format!("node{node}:9000"),
                slots: 1,
                protocol_addresses: BTreeMap::new(),
            })
            .await
            .unwrap();
        cluster.membership.add(node);
    }
    assert_eq!(coordinator.reconcile().await.unwrap().buckets_reled, 0);
    let moves: Vec<Move> = coordinator.rebalance().await.unwrap();
    assert!(!moves.is_empty());
    let mut per_node: BTreeMap<i32, usize> = BTreeMap::new();
    for leader in cluster.leaders(table_id).values() {
        *per_node.entry(*leader).or_default() += 1;
    }
    let (lower, upper) = mink_coordinator::limits(9, 3);
    assert!(
        per_node.values().all(|n| *n >= lower && *n <= upper),
        "{per_node:?}"
    );
    assert!(coordinator.rebalance().await.unwrap().is_empty());
}

struct RecordingCleaner(Mutex<Vec<(Bucket, u64, Vec<u64>)>>);

#[async_trait]
impl SnapshotCleaner for RecordingCleaner {
    async fn discard(
        &self,
        bucket: Bucket,
        snapshot: &KvSnapshotRow,
        retained: &[KvSnapshotRow],
    ) -> Result<(), Error> {
        self.0.lock().unwrap().push((
            bucket,
            snapshot.snapshot_id,
            retained.iter().map(|r| r.snapshot_id).collect(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn snapshots_beyond_retention_are_dropped_and_handed_to_the_cleaner() {
    let cluster = Cluster::new(&[1]).await;
    let cleaner = Arc::new(RecordingCleaner(Mutex::new(Vec::new())));
    let coordinator = cluster.coordinator_with(
        1,
        Config {
            snapshots_retained: 2,
            ..Config::default()
        },
        cleaner.clone(),
    );
    let epoch = coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table_id = coordinator
        .create_table(&path("db.t"), &table(Some(1), Options::default()), false)
        .await
        .unwrap()
        .unwrap();
    let bucket = Bucket::new(table_id, BucketId(0));
    let leader_epoch = cluster.views.load().state.catalog.buckets[&bucket].leader_epoch;
    for id in 0..4 {
        cluster
            .sink
            .propose(Command::CommitKvSnapshot {
                bucket,
                snapshot: KvSnapshotRow {
                    snapshot_id: id,
                    log_offset: id as i64 * 10,
                    row_count: 1,
                    path: format!("snap-{id}"),
                },
                leader_epoch,
                coordinator_epoch: epoch,
            })
            .await
            .unwrap();
    }

    let report = coordinator.reconcile().await.unwrap();
    assert_eq!(report.snapshots_dropped, 2);
    let remaining: Vec<u64> = cluster
        .views
        .load()
        .state
        .catalog
        .kv_snapshots
        .keys()
        .map(|(_, id)| *id)
        .collect();
    assert_eq!(remaining, vec![2, 3]);
    assert_eq!(
        *cleaner.0.lock().unwrap(),
        vec![(bucket, 0, vec![2, 3]), (bucket, 1, vec![2, 3])]
    );
    assert_eq!(coordinator.reconcile().await.unwrap().snapshots_dropped, 0);
}

#[tokio::test]
async fn auto_partitioning_creates_ahead_drops_behind_and_respects_the_interval() {
    let cluster = Cluster::new(&[1, 2]).await;
    let coordinator = cluster.coordinator(
        1,
        Config {
            auto_partition_interval: Duration::from_secs(600),
            ..Config::default()
        },
    );
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let options = Options {
        auto_partition: Some(AutoPartition {
            time_unit: TimeUnit::Day,
            num_precreate: Some(2),
            num_retention: Some(1),
            ..AutoPartition::default()
        }),
        ..Options::default()
    };
    let table_id = coordinator
        .create_table(&path("db.p"), &partitioned(2, options), false)
        .await
        .unwrap()
        .unwrap();
    let old = PartitionSpec::new(vec![("dt".into(), "20240601".parse().unwrap())]).unwrap();
    coordinator
        .create_partition(&path("db.p"), &old, false)
        .await
        .unwrap();

    let view = cluster.views.load();
    let (created, dropped) = coordinator
        .auto_partition(&view, NOW_MS, true)
        .await
        .unwrap();
    assert_eq!((created, dropped), (2, 1));
    assert_eq!(cluster.partitions(table_id), vec!["20240615", "20240616"]);
    assert_eq!(cluster.leaders(table_id).len(), 4);

    let report = coordinator.reconcile().await.unwrap();
    assert_eq!(
        (report.partitions_created, report.partitions_dropped),
        (0, 0)
    );
    cluster.clock.advance(Duration::from_secs(60));
    let view = cluster.views.load();
    let plan = mink_coordinator::plan_partitions(
        table_id,
        &view.state.catalog.tables[&path("db.p")].descriptor,
        &[],
        cluster.clock.millis(),
        false,
    )
    .unwrap();
    assert_eq!(
        plan.create.len(),
        2,
        "unforced plan lags by the table delay"
    );
    let report = coordinator.reconcile().await.unwrap();
    assert_eq!(report.partitions_created, 0, "interval not elapsed");
    cluster.clock.advance(Duration::from_secs(600));
    let report = coordinator.reconcile().await.unwrap();
    assert!(report.partitions_created <= 1);
}

#[tokio::test]
async fn the_loop_follows_the_leadership_watch() {
    let cluster = Cluster::new(&[1, 2]).await;
    let coordinator = Arc::new(cluster.coordinator(
        1,
        Config {
            tick: Duration::from_millis(10),
            ..Config::default()
        },
    ));
    let (tx, rx) = tokio::sync::watch::channel(false);
    let task = coordinator.clone().spawn(rx);

    let registered = || {
        cluster
            .views
            .load()
            .state
            .catalog
            .coordinator
            .as_ref()
            .map(|row| (row.node_id, row.epoch))
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(registered(), None, "not leading until the watch says so");

    tx.send(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while registered() != Some((1, 1)) {
        assert!(Instant::now() < deadline, "never registered");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(coordinator.is_leader());

    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table_id = coordinator
        .create_table(&path("db.t"), &table(Some(4), Options::default()), false)
        .await
        .unwrap()
        .unwrap();
    cluster.membership.remove(2);
    let deadline = Instant::now() + Duration::from_secs(5);
    while cluster.leaders(table_id).values().any(|l| *l == 2) {
        assert!(Instant::now() < deadline, "buckets never re-led");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    tx.send(false).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while coordinator.is_leader() {
        assert!(Instant::now() < deadline, "never resigned");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(tx);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("loop did not exit")
        .unwrap();
}

#[tokio::test]
async fn lake_tables_are_scheduled_for_tiering_and_workers_are_fenced() {
    let cluster = Cluster::new(&[1]).await;
    let coordinator = cluster.coordinator(1, Config::default());
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let options = Options {
        lake: Some(LakeFormat::Paimon),
        lake_freshness: Duration::from_secs(60),
        ..Options::default()
    };
    let table_id = coordinator
        .create_table(&path("db.lake"), &table(Some(1), options), false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        coordinator.tiering_state(table_id),
        Some(TieringState::Scheduled)
    );
    assert!(coordinator.request_tiering().is_none());

    cluster.clock.advance(Duration::from_secs(60));
    coordinator.reconcile().await.unwrap();
    let handed = coordinator.request_tiering().unwrap();
    assert_eq!((handed.table_id, handed.epoch), (table_id, 1));
    assert_eq!(handed.path, path("db.lake"));
    coordinator.tiering_heartbeat(table_id, 1).unwrap();
    assert!(matches!(
        coordinator.tiering_heartbeat(table_id, 0),
        Err(Error::TieringFenced { .. })
    ));
    coordinator.finish_tiering(table_id, 1, false).unwrap();
    assert_eq!(
        coordinator.tiering_state(table_id),
        Some(TieringState::Scheduled)
    );

    let successor = cluster.coordinator(1, Config::default());
    successor.become_leader().await.unwrap();
    assert_eq!(
        successor.tiering_state(table_id),
        Some(TieringState::Scheduled)
    );
    successor.drop_table(&path("db.lake"), false).await.unwrap();
    assert_eq!(successor.tiering_state(table_id), None);
}

#[tokio::test]
async fn lake_twin_is_created_before_the_table_and_missing_catalogs_reject_lake_ddl() {
    let cluster = Cluster::new(&[1]).await;
    let lake = Arc::new(MemoryLakeCatalog::default());
    let coordinator = Coordinator::new(
        1,
        "node1:9000",
        cluster.sink.clone(),
        cluster.views.clone(),
        cluster.membership.clone(),
        Arc::new(NoopCleaner),
        cluster.clock.clone(),
        Config::default(),
    )
    .with_lake_catalog(lake.clone());
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let options = Options {
        lake: Some(LakeFormat::Iceberg),
        ..Options::default()
    };

    coordinator
        .create_table(
            &path("db.plain"),
            &table(Some(1), Options::default()),
            false,
        )
        .await
        .unwrap();
    assert!(lake.created().is_empty());

    coordinator
        .create_table(&path("db.lake"), &table(Some(1), options.clone()), false)
        .await
        .unwrap();
    assert_eq!(lake.created(), vec![path("db.lake")]);
    assert_eq!(
        coordinator
            .create_table(&path("db.lake"), &table(Some(1), options.clone()), true)
            .await
            .unwrap(),
        None
    );
    assert_eq!(lake.created().len(), 1);

    coordinator
        .drop_table(&path("db.lake"), false)
        .await
        .unwrap();
    let err = coordinator
        .create_table(&path("db.lake"), &table(Some(1), options.clone()), false)
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Lake(lake::Error::TableExists(_))),
        "{err}"
    );
    assert!(
        cluster
            .views
            .load()
            .state
            .catalog
            .table(&path("db.lake"))
            .is_err()
    );

    let bare = cluster
        .coordinator_with(2, Config::default(), Arc::new(NoopCleaner))
        .with_lake_catalog(Arc::new(NoLakeCatalog));
    let err = bare
        .create_table(&path("db.other"), &table(Some(1), options), false)
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Lake(lake::Error::NotConfigured(_))),
        "{err}"
    );
}

#[tokio::test]
async fn attaching_adopts_the_lake_schema_and_seeds_the_baseline_snapshot() {
    let cluster = Cluster::new(&[1]).await;
    let lake = Arc::new(MemoryLakeCatalog::default());
    let coordinator = Coordinator::new(
        1,
        "node1:9000",
        cluster.sink.clone(),
        cluster.views.clone(),
        cluster.membership.clone(),
        Arc::new(NoopCleaner),
        cluster.clock.clone(),
        Config::default(),
    )
    .with_lake_catalog(lake.clone());
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let existing = path("db.existing");
    lake.add_existing(&existing, schema(), 77);

    let attach = Options {
        lake: Some(LakeFormat::Iceberg),
        lake_attach: true,
        ..Options::default()
    };
    let bare = Descriptor::builder(Schema::builder().build().unwrap())
        .bucket_count(2)
        .options(attach.clone())
        .build()
        .unwrap();
    let id = coordinator
        .create_table(&existing, &bare, false)
        .await
        .unwrap()
        .unwrap();
    let row = cluster
        .views
        .load()
        .state
        .catalog
        .table(&existing)
        .unwrap()
        .clone();
    assert_eq!(row.descriptor.schema(), &schema());
    assert!(row.descriptor.options().lake_attach);
    let snapshot = coordinator.lake_snapshot(id).unwrap();
    assert_eq!(snapshot.snapshot_id, 77);
    assert!(snapshot.bucket_log_end_offset.is_empty());
    assert!(coordinator.tiering_state(id).is_some());

    let err = coordinator
        .create_table(&path("db.missing"), &bare, false)
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Lake(lake::Error::TableNotFound(_))),
        "{err}"
    );
    let err = coordinator
        .create_table(
            &path("db.nolake"),
            &Descriptor::builder(schema())
                .bucket_count(1)
                .options(Options {
                    lake_attach: true,
                    ..Options::default()
                })
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Table(mink_table::Error::AttachWithoutLake)),
        "{err}"
    );
}

#[tokio::test]
async fn alter_table_evolves_schema_and_switches_the_lake_on_and_off() {
    let cluster = Cluster::new(&[1]).await;
    let lake = Arc::new(MemoryLakeCatalog::default());
    let coordinator = Coordinator::new(
        1,
        "node1:9000",
        cluster.sink.clone(),
        cluster.views.clone(),
        cluster.membership.clone(),
        Arc::new(NoopCleaner),
        cluster.clock.clone(),
        Config::default(),
    )
    .with_lake_catalog(lake.clone());
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table_id = coordinator
        .create_table(&path("db.t"), &table(Some(1), Options::default()), false)
        .await
        .unwrap()
        .unwrap();

    let schema_id = coordinator
        .alter_table(
            &path("db.t"),
            &[Change::add_column("added", DataType::int())],
            false,
        )
        .await
        .unwrap();
    assert_eq!(schema_id, Some(SchemaId(1)));
    let row = cluster
        .views
        .load()
        .state
        .catalog
        .table(&path("db.t"))
        .unwrap()
        .clone();
    assert_eq!(row.schemas.len(), 2);
    assert_eq!(
        row.descriptor.schema().columns().last().unwrap().name(),
        "added"
    );
    assert!(lake.created().is_empty(), "no lake yet, nothing to sync");

    assert!(matches!(
        coordinator
            .alter_table(
                &path("db.t"),
                &[Change::add_column("added", DataType::int())],
                false
            )
            .await,
        Err(Error::Table(mink_table::Error::ColumnExists(_)))
    ));
    assert!(matches!(
        coordinator
            .alter_table(&path("db.t"), &[Change::set("table.log.ttl", "1h")], false)
            .await,
        Err(Error::Table(mink_table::Error::NotAlterable(_)))
    ));
    assert_eq!(
        coordinator
            .alter_table(&path("db.missing"), &[Change::set("k", "v")], true)
            .await
            .unwrap(),
        None
    );

    coordinator
        .alter_table(
            &path("db.t"),
            &[
                Change::set("table.datalake.enabled", "true"),
                Change::set("table.datalake.freshness", "30s"),
                Change::set("owner", "ann"),
            ],
            false,
        )
        .await
        .unwrap();
    assert_eq!(lake.created(), vec![path("db.t")]);
    assert_eq!(
        coordinator.tiering_state(table_id),
        Some(TieringState::Scheduled)
    );
    let row = cluster
        .views
        .load()
        .state
        .catalog
        .table(&path("db.t"))
        .unwrap()
        .clone();
    assert_eq!(row.descriptor.options().lake, Some(LakeFormat::Iceberg));
    assert_eq!(
        row.descriptor.options().lake_freshness,
        Duration::from_secs(30)
    );
    assert_eq!(
        row.descriptor.custom().get("owner").map(String::as_str),
        Some("ann")
    );
    assert_eq!(row.schemas.len(), 2, "options only: no new schema version");

    coordinator
        .alter_table(
            &path("db.t"),
            &[Change::add_column("more", DataType::string())],
            false,
        )
        .await
        .unwrap();
    let synced = lake.altered(&path("db.t")).unwrap();
    assert_eq!(synced.schema().columns().last().unwrap().name(), "more");
    assert!(matches!(
        coordinator
            .alter_table(
                &path("db.t"),
                &[Change::set("iceberg.write.format.default", "orc")],
                false
            )
            .await,
        Err(Error::Table(mink_table::Error::LakeProperty(_)))
    ));

    coordinator
        .alter_table(
            &path("db.t"),
            &[Change::reset("table.datalake.enabled")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(coordinator.tiering_state(table_id), None);
    let row = cluster
        .views
        .load()
        .state
        .catalog
        .table(&path("db.t"))
        .unwrap()
        .clone();
    assert_eq!(row.descriptor.options().lake, None);
}

#[tokio::test]
async fn producer_offsets_register_once_expire_by_ttl_and_are_swept_on_the_interval() {
    let cluster = Cluster::new(&[1]).await;
    let coordinator = cluster.coordinator(
        1,
        Config {
            producer_offsets_ttl: Duration::from_secs(60),
            producer_offsets_cleanup_interval: Duration::from_secs(600),
            ..Config::default()
        },
    );
    coordinator.become_leader().await.unwrap();
    coordinator
        .create_database("db", None, BTreeMap::new(), false)
        .await
        .unwrap();
    let table_id = coordinator
        .create_table(&path("db.t"), &table(Some(2), Options::default()), false)
        .await
        .unwrap()
        .unwrap();
    let b0 = Bucket::new(table_id, BucketId(0));
    let b1 = Bucket::new(table_id, BucketId(1));
    let offsets: BTreeMap<Bucket, i64> = [(b0, 10), (b1, 20)].into();

    assert!(
        coordinator
            .register_producer_offsets("job", offsets.clone(), None)
            .await
            .unwrap()
    );
    assert!(
        !coordinator
            .register_producer_offsets("job", [(b0, 999)].into(), None)
            .await
            .unwrap()
    );
    let row = coordinator.producer_offsets("job").unwrap();
    assert_eq!(row.offsets, offsets);
    assert_eq!(row.expires_ms, NOW_MS + 60_000, "the cluster default TTL");

    assert!(
        coordinator
            .register_producer_offsets("short", offsets.clone(), Some(Duration::from_secs(5)))
            .await
            .unwrap()
    );
    assert_eq!(
        coordinator.producer_offsets("short").unwrap().expires_ms,
        NOW_MS + 5_000
    );

    cluster.clock.advance(Duration::from_secs(6));
    assert!(coordinator.producer_offsets("short").is_none());
    assert!(coordinator.producer_offsets("job").is_some());
    assert_eq!(
        coordinator
            .reconcile()
            .await
            .unwrap()
            .producer_offsets_expired,
        1
    );
    assert_eq!(cluster.views.load().state.catalog.producer_offsets.len(), 1);
    cluster.clock.advance(Duration::from_secs(60));
    assert!(coordinator.producer_offsets("job").is_none());
    assert_eq!(
        coordinator
            .reconcile()
            .await
            .unwrap()
            .producer_offsets_expired,
        0,
        "not due yet"
    );
    cluster.clock.advance(Duration::from_secs(600));
    assert_eq!(
        coordinator
            .reconcile()
            .await
            .unwrap()
            .producer_offsets_expired,
        1
    );
    assert!(
        cluster
            .views
            .load()
            .state
            .catalog
            .producer_offsets
            .is_empty()
    );

    assert!(
        coordinator
            .register_producer_offsets("job", offsets, None)
            .await
            .unwrap()
    );
    coordinator.delete_producer_offsets("job").await.unwrap();
    assert!(coordinator.producer_offsets("job").is_none());
    coordinator.delete_producer_offsets("job").await.unwrap();

    assert!(matches!(
        coordinator
            .register_producer_offsets("job/1", [(b0, 1)].into(), None)
            .await
            .unwrap_err(),
        Error::Metadata(mink_metadata::Error::InvalidArgument { .. })
    ));
}
