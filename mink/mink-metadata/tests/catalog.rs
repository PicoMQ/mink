//! Catalog commands end to end through the local sink, and codec round trips.

use std::collections::BTreeMap;

use mink_metadata::codec::{decode_command, decode_result, encode_command, encode_result};
use mink_metadata::{
    Command, Counter, Error, KvSnapshotRow, LakeSnapshotRow, Outcome, State, apply, snapshot,
};
use mink_table::{
    Bucket, BucketId, Column, Descriptor, Id, PartitionName, Path, PrimaryKey, Schema, SchemaId,
};
use mink_types::DataType;
use s3stream::StreamState;

fn schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .column(Column::new("dt", DataType::string().with_nullable(false)).unwrap())
        .primary_key(PrimaryKey::new(vec!["k".into(), "dt".into()]).unwrap())
        .build()
        .unwrap()
}

fn descriptor(buckets: u32) -> Descriptor {
    Descriptor::builder(schema())
        .bucket_count(buckets)
        .build()
        .unwrap()
}

fn partitioned(buckets: u32) -> Descriptor {
    Descriptor::builder(schema())
        .partitioned_by(["dt"])
        .bucket_count(buckets)
        .build()
        .unwrap()
}

fn path(s: &str) -> Path {
    s.parse().unwrap()
}

fn leaders(nodes: &[i32]) -> BTreeMap<BucketId, i32> {
    nodes
        .iter()
        .enumerate()
        .map(|(i, &node)| (BucketId(i as u32), node))
        .collect()
}

fn run(state: &mut State, command: Command) -> Outcome {
    apply(state, &command).unwrap()
}

fn register(state: &mut State, node_id: i32) {
    run(
        state,
        Command::RegisterNode {
            node_id,
            node_epoch: 1,
            http_address: format!("node{node_id}:9000"),
            slots: 1,
            protocol_addresses: BTreeMap::new(),
        },
    );
}

fn cluster() -> (State, Id) {
    let mut state = State::new();
    register(&mut state, 1);
    register(&mut state, 2);
    run(
        &mut state,
        Command::CreateDatabase {
            name: "db".into(),
            comment: Some("first".into()),
            custom: BTreeMap::new(),
            now_ms: 10,
        },
    );
    let Outcome::Id(table_id) = run(
        &mut state,
        Command::CreateTable {
            path: path("db.t"),
            descriptor: descriptor(2),
            leaders: leaders(&[1, 2]),
            coordinator_epoch: 0,
            now_ms: 11,
        },
    ) else {
        panic!("table id")
    };
    (state, Id(table_id))
}

#[test]
fn create_table_registers_schema_buckets_and_streams() {
    let (state, table_id) = cluster();
    let table = state.catalog.table(&path("db.t")).unwrap();
    assert_eq!(table.table_id, table_id);
    assert_eq!(table.schemas, vec![schema()]);
    assert_eq!(table.latest_schema_id(), SchemaId(0));
    assert_eq!(state.catalog.table_by_id(table_id), Some(table));

    let buckets: Vec<_> = state.catalog.buckets_of(table_id, None).collect();
    assert_eq!(buckets.len(), 2);
    for (i, (bucket, row)) in buckets.iter().enumerate() {
        assert_eq!(**bucket, Bucket::new(table_id, BucketId(i as u32)));
        assert_eq!(row.leader, i as i32 + 1);
        assert_eq!(row.leader_epoch, 0);
        let stream = state.streams.get(&row.stream_id).unwrap();
        assert_eq!(stream.state, StreamState::Closed);
        assert_eq!(stream.epoch, -1);
    }
    assert_eq!(state.next_stream_id, 2);
}

#[test]
fn create_table_rejects_bad_inputs() {
    let (mut state, _) = cluster();
    let err = apply(
        &mut state,
        &Command::CreateTable {
            path: path("db.t"),
            descriptor: descriptor(2),
            leaders: leaders(&[1, 2]),
            coordinator_epoch: 0,
            now_ms: 0,
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::TableExists { .. }));

    let err = apply(
        &mut state,
        &Command::CreateTable {
            path: path("nodb.t"),
            descriptor: descriptor(2),
            leaders: leaders(&[1, 2]),
            coordinator_epoch: 0,
            now_ms: 0,
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::DatabaseNotExist { .. }));

    let err = apply(
        &mut state,
        &Command::CreateTable {
            path: path("db.t2"),
            descriptor: descriptor(2),
            leaders: leaders(&[1]),
            coordinator_epoch: 0,
            now_ms: 0,
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::Unexpected { .. }));

    let unresolved = Descriptor::builder(schema()).build().unwrap();
    let err = apply(
        &mut state,
        &Command::CreateTable {
            path: path("db.t3"),
            descriptor: unresolved,
            leaders: BTreeMap::new(),
            coordinator_epoch: 0,
            now_ms: 0,
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::Unexpected { .. }));
}

#[test]
fn failed_apply_leaves_state_untouched() {
    let (mut state, _) = cluster();
    let before = state.clone();
    apply(
        &mut state,
        &Command::CreateTable {
            path: path("db.t2"),
            descriptor: descriptor(2),
            leaders: leaders(&[1]),
            coordinator_epoch: 0,
            now_ms: 0,
        },
    )
    .unwrap_err();
    assert_eq!(state, before);
}

#[test]
fn drop_table_returns_streams_and_clears_everything() {
    let (mut state, table_id) = cluster();
    let bucket = Bucket::new(table_id, BucketId(0));
    run(
        &mut state,
        Command::Allocate {
            counter: Counter::SnapshotId(bucket),
            count: 1,
        },
    );
    run(
        &mut state,
        Command::CommitKvSnapshot {
            bucket,
            snapshot: KvSnapshotRow {
                snapshot_id: 0,
                log_offset: 5,
                row_count: 3,
                path: "s3://b/0".into(),
            },
            leader_epoch: 0,
            coordinator_epoch: 0,
        },
    );

    let result = run(&mut state, Command::DropTable { path: path("db.t") });
    assert_eq!(result, Outcome::Ids(vec![0, 1]));
    assert!(state.catalog.tables.is_empty());
    assert!(state.catalog.table_paths.is_empty());
    assert!(state.catalog.buckets.is_empty());
    assert!(state.catalog.kv_snapshots.is_empty());
    assert!(state.catalog.counters.is_empty());
    assert_eq!(state.streams.len(), 2);
}

#[test]
fn drop_database_requires_empty() {
    let (mut state, _) = cluster();
    let err = apply(&mut state, &Command::DropDatabase { name: "db".into() }).unwrap_err();
    assert!(matches!(err, Error::DatabaseNotEmpty { .. }));
    run(&mut state, Command::DropTable { path: path("db.t") });
    run(&mut state, Command::DropDatabase { name: "db".into() });
    assert!(state.catalog.databases.is_empty());
}

#[test]
fn alter_table_appends_schemas_and_replaces_the_descriptor() {
    let (mut state, _) = cluster();
    let current = state
        .catalog
        .table(&path("db.t"))
        .unwrap()
        .descriptor
        .clone();
    let evolved = mink_table::alter_table(
        &current,
        &[mink_table::Change::add_column("added", DataType::int())],
        None,
    )
    .unwrap();
    let result = run(
        &mut state,
        Command::AlterTable {
            path: path("db.t"),
            descriptor: evolved.clone(),
            now_ms: 20,
        },
    );
    assert_eq!(result, Outcome::Id(1));
    let table = state.catalog.table(&path("db.t")).unwrap();
    assert_eq!(table.latest_schema_id(), SchemaId(1));
    assert_eq!(table.schema(SchemaId(1)), Some(evolved.schema()));
    assert_eq!(table.descriptor, evolved);
    assert_eq!(table.modified_ms, 20);

    let custom: BTreeMap<String, String> = [("owner".to_string(), "me".to_string())].into();
    let result = run(
        &mut state,
        Command::AlterTable {
            path: path("db.t"),
            descriptor: evolved.with_custom(custom.clone()),
            now_ms: 21,
        },
    );
    assert_eq!(result, Outcome::Id(1));
    let table = state.catalog.table(&path("db.t")).unwrap();
    assert_eq!(table.descriptor.custom(), &custom);
    assert_eq!(table.schemas.len(), 2);

    let rebucketed = Descriptor::builder(schema())
        .bucket_count(99)
        .build()
        .unwrap();
    let err = apply(
        &mut state,
        &Command::AlterTable {
            path: path("db.t"),
            descriptor: rebucketed,
            now_ms: 22,
        },
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("bucket count cannot change"),
        "{err}"
    );
}

#[test]
fn partitions_get_their_own_buckets() {
    let mut state = State::new();
    register(&mut state, 1);
    run(
        &mut state,
        Command::CreateDatabase {
            name: "db".into(),
            comment: None,
            custom: BTreeMap::new(),
            now_ms: 0,
        },
    );
    let Outcome::Id(table_id) = run(
        &mut state,
        Command::CreateTable {
            path: path("db.p"),
            descriptor: partitioned(1),
            leaders: BTreeMap::new(),
            coordinator_epoch: 0,
            now_ms: 0,
        },
    ) else {
        panic!()
    };
    let table_id = Id(table_id);
    assert_eq!(state.catalog.buckets.len(), 0);

    let name: PartitionName = "2026".parse().unwrap();
    let Outcome::Id(partition_id) = run(
        &mut state,
        Command::CreatePartition {
            path: path("db.p"),
            name: name.clone(),
            leaders: leaders(&[1]),
            coordinator_epoch: 0,
            now_ms: 1,
        },
    ) else {
        panic!()
    };
    assert_eq!(state.catalog.partitions_of(table_id).count(), 1);
    let bucket = Bucket::partitioned(table_id, mink_table::PartitionId(partition_id), BucketId(0));
    assert!(state.catalog.buckets.contains_key(&bucket));

    let err = apply(
        &mut state,
        &Command::CreatePartition {
            path: path("db.p"),
            name: name.clone(),
            leaders: leaders(&[1]),
            coordinator_epoch: 0,
            now_ms: 1,
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::PartitionExists { .. }));

    let result = run(
        &mut state,
        Command::DropPartition {
            path: path("db.p"),
            name,
        },
    );
    assert_eq!(result, Outcome::Ids(vec![0]));
    assert!(state.catalog.buckets.is_empty());
    assert!(state.catalog.partitions.is_empty());
    assert!(state.catalog.partition_names.is_empty());
}

#[test]
fn lead_bucket_bumps_epoch_and_fences_stale_coordinator() {
    let (mut state, table_id) = cluster();
    let bucket = Bucket::new(table_id, BucketId(0));
    run(
        &mut state,
        Command::RegisterCoordinator {
            node_id: 1,
            epoch: 3,
            address: "c:1".into(),
        },
    );
    let result = run(
        &mut state,
        Command::LeadBucket {
            bucket,
            node_id: 2,
            coordinator_epoch: 3,
        },
    );
    assert_eq!(result, Outcome::Id(1));
    let row = state.catalog.buckets[&bucket];
    assert_eq!(
        (row.leader, row.leader_epoch, row.coordinator_epoch),
        (2, 1, 3)
    );

    let err = apply(
        &mut state,
        &Command::LeadBucket {
            bucket,
            node_id: 1,
            coordinator_epoch: 2,
        },
    )
    .unwrap_err();
    assert!(matches!(
        err,
        Error::CoordinatorFenced {
            current: 3,
            given: 2
        }
    ));

    let err = apply(
        &mut state,
        &Command::LeadBucket {
            bucket,
            node_id: 9,
            coordinator_epoch: 3,
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::NodeEpochMismatch { node_id: 9, .. }));

    let err = apply(
        &mut state,
        &Command::RegisterCoordinator {
            node_id: 2,
            epoch: 2,
            address: "c:2".into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::CoordinatorFenced { .. }));
}

#[test]
fn kv_snapshots_are_fenced_by_leader_epoch() {
    let (mut state, table_id) = cluster();
    let bucket = Bucket::new(table_id, BucketId(1));
    let snapshot = |id: u64| KvSnapshotRow {
        snapshot_id: id,
        log_offset: 100 + id as i64,
        row_count: 7,
        path: format!("s3://b/{id}"),
    };

    let err = apply(
        &mut state,
        &Command::CommitKvSnapshot {
            bucket,
            snapshot: snapshot(0),
            leader_epoch: 1,
            coordinator_epoch: 0,
        },
    )
    .unwrap_err();
    assert!(matches!(
        err,
        Error::LeaderFenced {
            current: 0,
            given: 1,
            ..
        }
    ));

    for id in 0..3 {
        run(
            &mut state,
            Command::CommitKvSnapshot {
                bucket,
                snapshot: snapshot(id),
                leader_epoch: 0,
                coordinator_epoch: 0,
            },
        );
    }
    let err = apply(
        &mut state,
        &Command::CommitKvSnapshot {
            bucket,
            snapshot: snapshot(1),
            leader_epoch: 0,
            coordinator_epoch: 0,
        },
    )
    .unwrap_err();
    assert!(err.is_redundant());

    assert_eq!(state.catalog.latest_kv_snapshot(bucket), Some(&snapshot(2)));
    assert_eq!(state.catalog.kv_snapshots_of(bucket).count(), 3);
    run(
        &mut state,
        Command::DropKvSnapshot {
            bucket,
            snapshot_id: 0,
        },
    );
    let ids: Vec<u64> = state
        .catalog
        .kv_snapshots_of(bucket)
        .map(|s| s.snapshot_id)
        .collect();
    assert_eq!(ids, vec![1, 2]);
    assert!(
        state
            .catalog
            .latest_kv_snapshot(Bucket::new(table_id, BucketId(0)))
            .is_none()
    );
}

#[test]
fn lake_snapshots_only_move_offsets_forward() {
    let (mut state, table_id) = cluster();
    let snap = |id: i64, offset: i64| LakeSnapshotRow {
        snapshot_id: id,
        bucket_log_end_offset: [(Bucket::new(table_id, BucketId(0)), offset)].into(),
    };
    run(
        &mut state,
        Command::CommitLakeSnapshot {
            table_id,
            snapshot: snap(5, 50),
        },
    );
    let err = apply(
        &mut state,
        &Command::CommitLakeSnapshot {
            table_id,
            snapshot: snap(5, 50),
        },
    )
    .unwrap_err();
    assert!(err.is_redundant());
    run(
        &mut state,
        Command::CommitLakeSnapshot {
            table_id,
            snapshot: snap(2, 60),
        },
    );
    assert_eq!(state.catalog.lake[&table_id], snap(2, 60));
    let err = apply(
        &mut state,
        &Command::CommitLakeSnapshot {
            table_id,
            snapshot: snap(9, 55),
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::Unexpected { .. }), "{err}");
    let err = apply(
        &mut state,
        &Command::CommitLakeSnapshot {
            table_id: Id(99),
            snapshot: snap(1, 1),
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::TableNotExist { .. }));
}

#[test]
fn counters_hand_out_disjoint_ranges() {
    let (mut state, table_id) = cluster();
    let counter = Counter::AutoIncrement {
        table_id,
        column_id: 0,
    };
    assert_eq!(
        run(
            &mut state,
            Command::Allocate {
                counter,
                count: 100
            }
        ),
        Outcome::Id(0)
    );
    assert_eq!(
        run(&mut state, Command::Allocate { counter, count: 5 }),
        Outcome::Id(100)
    );
    let other = Counter::AutoIncrement {
        table_id,
        column_id: 1,
    };
    assert_eq!(
        run(
            &mut state,
            Command::Allocate {
                counter: other,
                count: 1
            }
        ),
        Outcome::Id(0)
    );
    let err = apply(&mut state, &Command::Allocate { counter, count: 0 }).unwrap_err();
    assert!(matches!(err, Error::Unexpected { .. }));
}

#[test]
fn group_offsets_merge_per_bucket_and_expire() {
    let (mut state, table_id) = cluster();
    let b0 = Bucket::new(table_id, BucketId(0));
    let b1 = Bucket::new(table_id, BucketId(1));
    let commit = |group: &str, offsets: &[(Bucket, i64)], expires_ms| Command::CommitGroupOffsets {
        group: group.into(),
        offsets: offsets.iter().copied().collect(),
        expires_ms,
    };

    run(&mut state, commit("g", &[(b0, 5), (b1, 7)], 1_000));
    run(&mut state, commit("g", &[(b0, 9)], 2_000));
    let row = state.catalog.group_offsets("g", 100).unwrap();
    assert_eq!(row.expires_ms, 2_000);
    assert_eq!(row.offsets[&b0], 9);
    assert_eq!(
        row.offsets[&b1], 7,
        "untouched buckets survive a partial commit"
    );
    assert_eq!(
        state
            .catalog
            .group_offsets_by_expiry
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec![(2_000, "g".to_string())]
    );

    for bad in [
        commit("", &[(b0, 1)], 9_000),
        commit("g", &[(b0, -1)], 9_000),
    ] {
        assert!(matches!(
            apply(&mut state, &bad).unwrap_err(),
            Error::InvalidArgument { .. }
        ));
    }
    let missing = Bucket::new(Id(999), BucketId(0));
    assert!(matches!(
        apply(&mut state, &commit("g", &[(missing, 1)], 9_000)).unwrap_err(),
        Error::BucketNotExist { .. }
    ));

    run(&mut state, commit("late", &[(b0, 1)], 9_000));
    assert!(state.catalog.group_offsets("g", 2_001).is_none());
    assert_eq!(
        run(&mut state, Command::ExpireGroupOffsets { now_ms: 2_001 }),
        Outcome::Count(1)
    );
    assert_eq!(state.catalog.group_offsets.len(), 1);
    run(
        &mut state,
        Command::DeleteGroupOffsets {
            group: "late".into(),
        },
    );
    assert!(state.catalog.group_offsets.is_empty());
    assert!(state.catalog.group_offsets_by_expiry.is_empty());
}

#[test]
fn producer_offsets_register_once_until_expired_and_sweep() {
    let (mut state, table_id) = cluster();
    let b0 = Bucket::new(table_id, BucketId(0));
    let b1 = Bucket::new(table_id, BucketId(1));
    let register = |producer_id: &str, offsets: &[(Bucket, i64)], expires_ms, now_ms| {
        Command::RegisterProducerOffsets {
            producer_id: producer_id.into(),
            offsets: offsets.iter().copied().collect(),
            expires_ms,
            now_ms,
        }
    };

    assert_eq!(
        run(&mut state, register("job", &[(b0, 5), (b1, 7)], 1_000, 100)),
        Outcome::Bool(true)
    );
    assert_eq!(
        run(&mut state, register("job", &[(b0, 99)], 5_000, 200)),
        Outcome::Bool(false)
    );
    let row = state.catalog.producer_offsets("job", 200).unwrap();
    assert_eq!(row.expires_ms, 1_000);
    assert_eq!(row.offsets[&b0], 5);
    assert_eq!(row.offsets[&b1], 7);

    assert!(state.catalog.producer_offsets("job", 1_000).is_some());
    assert!(state.catalog.producer_offsets("job", 1_001).is_none());
    assert_eq!(
        run(&mut state, register("job", &[(b0, 99)], 5_000, 1_001)),
        Outcome::Bool(true)
    );
    assert_eq!(
        state
            .catalog
            .producer_offsets("job", 1_001)
            .unwrap()
            .offsets[&b0],
        99
    );
    assert_eq!(
        state
            .catalog
            .producer_offsets_by_expiry
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec![(5_000, "job".to_string())],
        "the expiry index follows the replacement"
    );

    for bad in [
        register("has space", &[(b0, 1)], 9_000, 1_001),
        register("", &[(b0, 1)], 9_000, 1_001),
        register("other", &[(b0, -1)], 9_000, 1_001),
        register("other", &[(b0, 1)], 900, 1_001),
    ] {
        assert!(matches!(
            apply(&mut state, &bad).unwrap_err(),
            Error::InvalidArgument { .. }
        ));
    }
    let missing = Bucket::new(Id(999), BucketId(0));
    assert!(matches!(
        apply(
            &mut state,
            &register("other", &[(missing, 1)], 9_000, 1_001)
        )
        .unwrap_err(),
        Error::BucketNotExist { .. }
    ));
    assert!(state.catalog.producer_offsets("other", 1_001).is_none());

    run(&mut state, register("early", &[(b0, 1)], 2_000, 1_001));
    run(&mut state, register("late", &[(b0, 1)], 9_000, 1_001));
    assert_eq!(
        state
            .catalog
            .expired_producer_offsets(5_001)
            .collect::<Vec<_>>(),
        vec!["early", "job"]
    );
    assert_eq!(
        run(&mut state, Command::ExpireProducerOffsets { now_ms: 5_001 }),
        Outcome::Count(2)
    );
    assert_eq!(state.catalog.producer_offsets.len(), 1);
    assert!(state.catalog.producer_offsets("late", 5_001).is_some());
    assert_eq!(
        run(&mut state, Command::ExpireProducerOffsets { now_ms: 5_001 }),
        Outcome::Count(0)
    );

    run(
        &mut state,
        Command::DeleteProducerOffsets {
            producer_id: "late".into(),
        },
    );
    run(
        &mut state,
        Command::DeleteProducerOffsets {
            producer_id: "late".into(),
        },
    );
    assert!(state.catalog.producer_offsets.is_empty());
    assert!(state.catalog.producer_offsets_by_expiry.is_empty());
}

fn every_command() -> Vec<Command> {
    let bucket = Bucket::partitioned(Id(7), mink_table::PartitionId(3), BucketId(2));
    vec![
        Command::CreateDatabase {
            name: "db".into(),
            comment: Some("c".into()),
            custom: [("a".to_string(), "b".to_string())].into(),
            now_ms: 1,
        },
        Command::CreateDatabase {
            name: "db2".into(),
            comment: None,
            custom: BTreeMap::new(),
            now_ms: 2,
        },
        Command::DropDatabase { name: "db".into() },
        Command::CreateTable {
            path: path("db.t"),
            descriptor: partitioned(3),
            leaders: leaders(&[1, 2, 3]),
            coordinator_epoch: 4,
            now_ms: 5,
        },
        Command::DropTable { path: path("db.t") },
        Command::AlterTable {
            path: path("db.t"),
            descriptor: descriptor(1),
            now_ms: 6,
        },
        Command::CreatePartition {
            path: path("db.t"),
            name: "2026$us".parse().unwrap(),
            leaders: leaders(&[1]),
            coordinator_epoch: 8,
            now_ms: 9,
        },
        Command::DropPartition {
            path: path("db.t"),
            name: "2026".parse().unwrap(),
        },
        Command::LeadBucket {
            bucket,
            node_id: 1,
            coordinator_epoch: 2,
        },
        Command::CommitKvSnapshot {
            bucket: Bucket::new(Id(1), BucketId(0)),
            snapshot: KvSnapshotRow {
                snapshot_id: 9,
                log_offset: -1,
                row_count: 0,
                path: "s3://x".into(),
            },
            leader_epoch: 3,
            coordinator_epoch: 4,
        },
        Command::DropKvSnapshot {
            bucket,
            snapshot_id: 9,
        },
        Command::CommitLakeSnapshot {
            table_id: Id(7),
            snapshot: LakeSnapshotRow {
                snapshot_id: 12,
                bucket_log_end_offset: [(bucket, 44)].into(),
            },
        },
        Command::Allocate {
            counter: Counter::SnapshotId(bucket),
            count: 1,
        },
        Command::Allocate {
            counter: Counter::AutoIncrement {
                table_id: Id(7),
                column_id: 2,
            },
            count: 1000,
        },
        Command::RegisterCoordinator {
            node_id: 1,
            epoch: 2,
            address: "host:1".into(),
        },
        Command::RegisterProducerOffsets {
            producer_id: "flink-job-7".into(),
            offsets: [(bucket, 44), (Bucket::new(Id(1), BucketId(0)), 0)].into(),
            expires_ms: 90_000,
            now_ms: 3_600,
        },
        Command::DeleteProducerOffsets {
            producer_id: "flink-job-7".into(),
        },
        Command::ExpireProducerOffsets { now_ms: 91_000 },
    ]
}

#[test]
fn commands_round_trip_through_the_codec() {
    for command in every_command() {
        let bytes = encode_command(&command);
        assert_eq!(decode_command(&bytes).unwrap(), command, "{command:?}");
    }
    let ids = Outcome::Ids(vec![3, 1, 2]);
    assert_eq!(decode_result(&encode_result(&ids)).unwrap(), ids);
    for created in [true, false] {
        let result = Outcome::Bool(created);
        assert_eq!(decode_result(&encode_result(&result)).unwrap(), result);
    }
}

#[test]
fn snapshot_round_trips_the_catalog() {
    let (mut state, table_id) = cluster();
    let bucket = Bucket::new(table_id, BucketId(0));
    run(
        &mut state,
        Command::CreateTable {
            path: path("db.p"),
            descriptor: partitioned(1),
            leaders: BTreeMap::new(),
            coordinator_epoch: 0,
            now_ms: 0,
        },
    );
    run(
        &mut state,
        Command::CreatePartition {
            path: path("db.p"),
            name: "2026".parse().unwrap(),
            leaders: leaders(&[2]),
            coordinator_epoch: 0,
            now_ms: 1,
        },
    );
    let evolved = mink_table::alter_table(
        &state.catalog.table(&path("db.t")).unwrap().descriptor,
        &[mink_table::Change::add_column("added", DataType::int())],
        None,
    )
    .unwrap();
    run(
        &mut state,
        Command::AlterTable {
            path: path("db.t"),
            descriptor: evolved,
            now_ms: 2,
        },
    );
    run(
        &mut state,
        Command::LeadBucket {
            bucket,
            node_id: 2,
            coordinator_epoch: 1,
        },
    );
    run(
        &mut state,
        Command::CommitKvSnapshot {
            bucket,
            snapshot: KvSnapshotRow {
                snapshot_id: 0,
                log_offset: 3,
                row_count: 1,
                path: "s3://b/0".into(),
            },
            leader_epoch: 1,
            coordinator_epoch: 1,
        },
    );
    run(
        &mut state,
        Command::CommitLakeSnapshot {
            table_id,
            snapshot: LakeSnapshotRow {
                snapshot_id: 1,
                bucket_log_end_offset: [(bucket, 3)].into(),
            },
        },
    );
    run(
        &mut state,
        Command::Allocate {
            counter: Counter::AutoIncrement {
                table_id,
                column_id: 0,
            },
            count: 10,
        },
    );
    run(
        &mut state,
        Command::RegisterCoordinator {
            node_id: 1,
            epoch: 1,
            address: "c:1".into(),
        },
    );
    run(
        &mut state,
        Command::RegisterProducerOffsets {
            producer_id: "sink".into(),
            offsets: [(bucket, 3)].into(),
            expires_ms: 1_000,
            now_ms: 10,
        },
    );

    let bytes = snapshot::encode(&state);
    let restored = snapshot::decode(&bytes).unwrap();
    assert_eq!(restored, state);
    assert_eq!(snapshot::encode(&restored), bytes);
}
