//! The functional suite: runs against one node (lite or full) and against a cluster.
//! Lake cases are skipped when the stack has no lake (`MINK_E2E_LAKE=0`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use futures::{StreamExt, TryStreamExt};
use mink_client::{Error, proto};
use mink_e2e::kafka::Kafka;
use mink_e2e::lake::Lake;
use mink_e2e::rows::*;
use mink_e2e::tables::*;
use mink_e2e::{Env, fresh_database, unique, wait_for};
use mink_table::{Change, Descriptor, Options, Path, Schema, SchemaId};
use mink_types::DataType;
use tonic::Code;

fn path(db: &str, table: &str) -> Path {
    format!("{db}.{table}").parse().unwrap()
}

#[tokio::test]
async fn catalog_ddl_round_trips() {
    let Some(env) = Env::load() else { return };
    let cluster = env.ready().await;
    let admin = cluster.admin();

    let db = fresh_database(&admin, "ddl").await;
    assert!(admin.database_exists(&db).await.unwrap());
    assert!(admin.list_databases().await.unwrap().contains(&db));

    let events_path = path(&db, "events");
    let users_path = path(&db, "users");
    let events = create(
        &admin,
        &cluster,
        &events_path,
        &events_descriptor(3, false).build().unwrap(),
    )
    .await;
    let users = create(
        &admin,
        &cluster,
        &users_path,
        &users_descriptor(2, false).build().unwrap(),
    )
    .await;
    assert_eq!(events.buckets().count(), 3);
    assert_eq!(users.buckets().count(), 2);
    assert!(users.descriptor().has_primary_key());

    let mut listed = admin.list_tables(&db).await.unwrap();
    listed.sort();
    assert_eq!(listed, ["events", "users"]);
    assert!(admin.table_exists(&users_path).await.unwrap());

    let again = admin
        .create_table(&users_path, users.descriptor(), true)
        .await
        .unwrap();
    assert_eq!(again, None);
    let clash = admin
        .create_table(&users_path, users.descriptor(), false)
        .await;
    assert!(matches!(clash, Err(Error::Status(s)) if s.code() == Code::AlreadyExists));

    let info = admin.get_table(&events_path).await.unwrap();
    assert_eq!(info.path, events_path);
    assert!(info.buckets.iter().all(|b| b.leader.is_some()));
    if env.multi_node() {
        let leaders: std::collections::BTreeSet<i32> = info
            .buckets
            .iter()
            .filter_map(|b| b.leader.as_ref().map(|l| l.node_id))
            .collect();
        assert!(
            leaders.len() > 1,
            "three buckets spread over the cluster: {leaders:?}"
        );
    }

    admin.drop_table(&events_path, false).await.unwrap();
    assert!(!admin.table_exists(&events_path).await.unwrap());
    let missing = admin.drop_table(&events_path, false).await;
    assert!(matches!(missing, Err(Error::Status(s)) if s.code() == Code::NotFound));
    admin.drop_table(&events_path, true).await.unwrap();

    admin.drop_database(&db, false, true).await.unwrap();
    assert!(!admin.database_exists(&db).await.unwrap());
    assert!(!admin.table_exists(&users_path).await.unwrap());
}

#[tokio::test]
async fn log_table_append_scan_and_tail() {
    let Some(env) = Env::load() else { return };
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "log").await;
    let table = create(
        &admin,
        &cluster,
        &path(&db, "events"),
        &events_descriptor(3, false).build().unwrap(),
    )
    .await;

    let mut tails = Vec::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        tails.push(table.tail(bucket, 0, None).await.unwrap());
    }

    let mut writer = table.append_writer().await.unwrap();
    let mut expected: Vec<Event> = Vec::new();
    for chunk in 0..10 {
        let batch = events_range(chunk * 100, (chunk + 1) * 100, "e");
        let routed = writer.append(&batch).await.unwrap();
        assert_eq!(routed.iter().map(|b| b.rows).sum::<usize>(), 100);
        expected.extend(event_rows(&batch));
    }
    expected.sort();

    let hw = high_watermarks(&table).await;
    assert_eq!(hw.values().sum::<i64>(), 1000);
    assert!(
        hw.values().all(|&h| h > 0),
        "k hashes over all buckets: {hw:?}"
    );

    assert_eq!(scan_events(&table).await, expected);
    assert_eq!(union_events(&table).await, expected);

    let mut tailed = Vec::new();
    for (bucket, tail) in table.buckets().zip(tails.iter_mut()) {
        let end = hw[&bucket];
        loop {
            let batch = tail.next().await.unwrap().unwrap();
            tailed.extend(event_rows(&batch.rows));
            if batch.meta.last_offset + 1 >= end {
                break;
            }
        }
    }
    tailed.sort();
    let mut dups = tailed.clone();
    dups.dedup();
    assert_eq!(
        (tailed.len(), dups.len()),
        (expected.len(), expected.len()),
        "tail delivered every row once"
    );
    assert_eq!(tailed, expected);

    let bucket = table.buckets().next().unwrap();
    let (start, end) = table.offsets(bucket).await.unwrap();
    let window: Vec<_> = table
        .scan(bucket, start, (start + 5).min(end), None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(window.iter().map(|b| b.rows.num_rows()).sum::<usize>(), 5);
    let latest = table
        .list_offset(bucket, proto::OffsetSpec::Latest)
        .await
        .unwrap();
    assert_eq!(latest, end);

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn pk_table_upsert_lookup_delete_partial_update_and_snapshot() {
    let Some(env) = Env::load() else { return };
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "pk").await;
    let table = create(
        &admin,
        &cluster,
        &path(&db, "users"),
        &users_descriptor(2, false).build().unwrap(),
    )
    .await;

    let mut writer = table.upsert_writer().await.unwrap();
    writer
        .upsert(&users(&[
            (1, "ann", 10),
            (2, "bob", 20),
            (3, "cid", 30),
            (4, "dee", 40),
        ]))
        .await
        .unwrap();
    let lookuper = table.lookuper().unwrap();
    let row = lookuper
        .lookup_one(&user_keys(&[2]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user_rows(&row)[&2], user("bob", 20));

    writer.upsert(&users(&[(2, "bob", 21)])).await.unwrap();
    writer.delete(&users(&[(3, "", 0)])).await.unwrap();
    assert!(
        lookuper
            .lookup_one(&user_keys(&[3]))
            .await
            .unwrap()
            .is_none()
    );

    let mut partial = table.partial_update_writer(vec![0, 2]).await.unwrap();
    partial.upsert(&users(&[(1, "ignored", 11)])).await.unwrap();
    let row = lookuper
        .lookup_one(&user_keys(&[1]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user_rows(&row)[&1], user("ann", 11));

    let found = lookuper.lookup(&user_keys(&[4, 3, 1, 99])).await.unwrap();
    assert_eq!(
        found.iter().map(Option::is_some).collect::<Vec<_>>(),
        [true, false, true, false]
    );

    let expected = BTreeMap::from([
        (1, user("ann", 11)),
        (2, user("bob", 21)),
        (4, user("dee", 40)),
    ]);
    let hw = high_watermarks(&table).await;
    for (bucket, hw) in &hw {
        if *hw == 0 {
            continue;
        }
        wait_for(&format!("kv snapshot of {bucket:?}"), async || {
            Ok(admin
                .latest_kv_snapshot(*bucket)
                .await?
                .filter(|s| s.log_offset >= *hw))
        })
        .await;
    }
    assert_eq!(snapshot_users(&table).await, expected);
    if env.lake {
        assert_eq!(union_users(&table).await, expected);
    }

    let changelog: Vec<RecordBatch> = scan_all(&table).await;
    assert!(count(&changelog) >= 7, "every change lands in the log");

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn partitioned_log_table_creates_partitions_on_write() {
    let Some(env) = Env::load() else { return };
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "part").await;
    let table_path = path(&db, "events");
    let descriptor = Descriptor::builder(partitioned_events_schema())
        .partitioned_by(["region"])
        .bucket_keys(["k"])
        .bucket_count(2)
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, false)
        .await
        .unwrap();
    let table = cluster.table(&table_path).await.unwrap();
    assert_eq!(table.buckets().count(), 0, "no partitions yet");

    let mut writer = table.append_writer().await.unwrap();
    let routed = writer
        .append(&partitioned_events(&[
            (1, "a", "eu"),
            (2, "b", "us"),
            (3, "c", "eu"),
            (4, "d", "us"),
            (5, "e", "eu"),
        ]))
        .await
        .unwrap();
    assert_eq!(routed.iter().map(|b| b.rows).sum::<usize>(), 5);
    let mut partitions: Vec<String> = admin
        .list_partitions(&table_path)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.name.to_string())
        .collect();
    partitions.sort();
    assert_eq!(partitions.len(), 2, "{partitions:?}");

    let table = wait_for("partition buckets led", async || {
        let table = cluster.table(&table_path).await?;
        let info = admin.get_table(&table_path).await?;
        Ok(
            (info.buckets.len() == 4 && info.buckets.iter().all(|b| b.leader.is_some()))
                .then_some(table),
        )
    })
    .await;
    let batches = union_all(&table).await;
    assert_eq!(count(&batches), 5);
    let mut ks: Vec<i64> = batches.iter().flat_map(|b| keys(b, "k")).collect();
    ks.sort();
    assert_eq!(ks, [1, 2, 3, 4, 5]);

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn schema_evolution_add_rename_promote_drop() {
    let Some(env) = Env::load() else { return };
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "evolve").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(2, env.lake).build().unwrap(),
    )
    .await;
    let mut writer = table.append_writer().await.unwrap();
    writer.append(&events(&[(1, "a"), (2, "b")])).await.unwrap();
    if env.lake {
        wait_tiered_to_head(&cluster, &table).await;
    }

    let v1 = admin
        .alter_table(
            &table_path,
            vec![Change::add_column("w", DataType::int())],
            false,
        )
        .await
        .unwrap();
    assert_eq!(v1, Some(SchemaId(1)));
    let table = cluster.table(&table_path).await.unwrap();
    assert_eq!(table.schema_id(), SchemaId(1));
    let mut writer = table.append_writer().await.unwrap();
    writer
        .append(
            &RecordBatch::try_new(
                table.arrow_schema(),
                vec![
                    Arc::new(Int64Array::from(vec![3])),
                    Arc::new(StringArray::from(vec!["c"])),
                    Arc::new(Int32Array::from(vec![Some(30)])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let v2 = admin
        .alter_table(
            &table_path,
            vec![
                Change::RenameColumn {
                    name: "v".into(),
                    new_name: "label".into(),
                },
                Change::ModifyColumn {
                    name: "w".into(),
                    data_type: DataType::big_int(),
                    comment: None,
                },
            ],
            false,
        )
        .await
        .unwrap();
    assert_eq!(v2, Some(SchemaId(2)));
    let table = cluster.table(&table_path).await.unwrap();
    let names: Vec<&str> = table.schema().columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["k", "label", "w"]);
    let mut writer = table.append_writer().await.unwrap();
    writer
        .append(
            &RecordBatch::try_new(
                table.arrow_schema(),
                vec![
                    Arc::new(Int64Array::from(vec![4])),
                    Arc::new(StringArray::from(vec!["d"])),
                    Arc::new(Int64Array::from(vec![Some(40)])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut rows: Vec<(i64, String, Option<i64>)> = Vec::new();
    for batch in union_all(&table).await {
        assert_eq!(batch.num_columns(), 3, "{:?}", batch.schema());
        let k = batch.column(0).as_primitive::<Int64Type>();
        let label = batch.column(1).as_string::<i32>();
        let w = batch.column(2).as_primitive::<Int64Type>();
        rows.extend((0..batch.num_rows()).map(|i| {
            (
                k.value(i),
                label.value(i).to_owned(),
                w.is_valid(i).then(|| w.value(i)),
            )
        }));
    }
    rows.sort();
    assert_eq!(
        rows,
        [
            (1, "a".to_owned(), None),
            (2, "b".to_owned(), None),
            (3, "c".to_owned(), Some(30)),
            (4, "d".to_owned(), Some(40)),
        ]
    );

    let rejected = admin
        .alter_table(
            &table_path,
            vec![Change::DropColumn { name: "k".into() }],
            false,
        )
        .await;
    assert!(
        matches!(rejected, Err(Error::Status(ref s)) if s.code() == Code::InvalidArgument),
        "bucket key cannot be dropped: {rejected:?}"
    );
    let v3 = admin
        .alter_table(
            &table_path,
            vec![Change::DropColumn {
                name: "label".into(),
            }],
            false,
        )
        .await
        .unwrap();
    assert_eq!(v3, Some(SchemaId(3)));
    let table = cluster.table(&table_path).await.unwrap();
    let names: Vec<&str> = table.schema().columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["k", "w"]);
    let batches = union_all(&table).await;
    assert_eq!(count(&batches), 4);
    assert!(batches.iter().all(|b| b.num_columns() == 2));

    if env.lake {
        wait_tiered_to_head(&cluster, &table).await;
        let lake = Lake::connect(&env).await;
        let columns = lake.column_names(&table_path).await;
        assert_eq!(columns, ["k", "w"], "Iceberg schema followed the evolution");
        let mut lake_keys: Vec<i64> = lake
            .rows(&table_path)
            .await
            .iter()
            .flat_map(|b| keys(b, "k"))
            .collect();
        lake_keys.sort();
        assert_eq!(lake_keys, [1, 2, 3, 4]);
        let w_type = lake
            .table(&table_path)
            .await
            .metadata()
            .current_schema()
            .field_by_name("w")
            .map(|f| f.field_type.to_string());
        assert_eq!(
            w_type.as_deref(),
            Some("long"),
            "int promoted to long in Iceberg"
        );
    }

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn lake_tiering_union_and_log_trim() {
    let Some(env) = Env::load() else { return };
    if !env.lake {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let lake = Lake::connect(&env).await;
    let db = fresh_database(&admin, "lake").await;
    let events_path = path(&db, "events");
    let users_path = path(&db, "users");
    let events_table = create(
        &admin,
        &cluster,
        &events_path,
        &Descriptor::builder(events_schema())
            .bucket_count(2)
            .options(Options {
                log_ttl: Some(Duration::from_secs(2)),
                ..lake_options()
            })
            .build()
            .unwrap(),
    )
    .await;
    let users_table = create(
        &admin,
        &cluster,
        &users_path,
        &users_descriptor(2, true).build().unwrap(),
    )
    .await;
    assert!(
        lake.exists(&events_path).await,
        "created in the catalog up front"
    );
    assert!(lake.exists(&users_path).await);

    let mut appender = events_table.append_writer().await.unwrap();
    appender.append(&events_range(0, 500, "a")).await.unwrap();
    let mut upserter = users_table.upsert_writer().await.unwrap();
    upserter
        .upsert(&users(&[(1, "ann", 10), (2, "bob", 20), (3, "cid", 30)]))
        .await
        .unwrap();

    let snapshot = wait_tiered_to_head(&cluster, &events_table).await;
    wait_tiered_to_head(&cluster, &users_table).await;
    assert_eq!(
        lake.current_snapshot_id(&events_path).await,
        Some(snapshot.snapshot_id),
        "the coordinator's snapshot is the catalog's current snapshot"
    );
    let lake_batches = lake.rows(&events_path).await;
    assert_eq!(count(&lake_batches), 500);
    assert!(
        lake_batches.iter().all(|b| b
            .schema()
            .fields()
            .iter()
            .all(|f| !f.name().starts_with("__"))),
        "no system columns in the lake: {:?}",
        lake_batches[0].schema()
    );
    if let Some(n) = lake.duckdb_count(&events_path).await {
        assert_eq!(n, 500, "DuckDB sees the same table");
    }

    appender.append(&events_range(500, 600, "b")).await.unwrap();
    upserter
        .upsert(&users(&[(2, "bob", 21), (4, "dee", 40)]))
        .await
        .unwrap();
    upserter.delete(&users(&[(3, "", 0)])).await.unwrap();

    let unioned = union_events(&events_table).await;
    assert_eq!(unioned.len(), 600, "cold 500 + hot 100, no duplicates");
    assert_eq!(unioned.first().unwrap().0, 0);
    assert_eq!(unioned.last().unwrap().0, 599);
    assert_eq!(
        union_users(&users_table).await,
        BTreeMap::from([
            (1, user("ann", 10)),
            (2, user("bob", 21)),
            (4, user("dee", 40)),
        ])
    );

    wait_tiered_to_head(&cluster, &events_table).await;
    wait_tiered_to_head(&cluster, &users_table).await;
    let lake_users: BTreeMap<i64, User> = lake
        .rows(&users_path)
        .await
        .iter()
        .flat_map(user_rows)
        .collect();
    assert_eq!(
        lake_users,
        BTreeMap::from([
            (1, user("ann", 10)),
            (2, user("bob", 21)),
            (4, user("dee", 40)),
        ]),
        "equality deletes applied in the lake"
    );

    for bucket in events_table.buckets().collect::<Vec<_>>() {
        wait_for(&format!("log of {bucket:?} trimmed"), async || {
            let earliest = events_table
                .list_offset(bucket, proto::OffsetSpec::Earliest)
                .await?;
            Ok((earliest > 0).then_some(earliest))
        })
        .await;
    }
    assert!(
        scan_events(&events_table).await.len() < 600,
        "hot log shrank"
    );
    assert_eq!(
        union_events(&events_table).await.len(),
        600,
        "union still complete"
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn lake_attach_adopts_existing_iceberg_data() {
    let Some(env) = Env::load() else { return };
    if !env.lake {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let lake = Lake::connect(&env).await;
    let db = fresh_database(&admin, "attach").await;
    let orders_path = path(&db, "orders");
    let users_path = path(&db, "users");

    let orders = create(
        &admin,
        &cluster,
        &orders_path,
        &Descriptor::builder(events_schema())
            .bucket_count(2)
            .options(lake_options())
            .build()
            .unwrap(),
    )
    .await;
    orders
        .append_writer()
        .await
        .unwrap()
        .append(&events_range(0, 100, "old"))
        .await
        .unwrap();
    let baseline = wait_tiered_to_head(&cluster, &orders).await;
    let users_table = create(
        &admin,
        &cluster,
        &users_path,
        &users_descriptor(2, true).build().unwrap(),
    )
    .await;
    users_table
        .upsert_writer()
        .await
        .unwrap()
        .upsert(&users(&[(1, "ann", 10), (2, "bob", 20)]))
        .await
        .unwrap();
    wait_tiered_to_head(&cluster, &users_table).await;

    admin.drop_table(&orders_path, false).await.unwrap();
    admin.drop_table(&users_path, false).await.unwrap();
    assert!(
        lake.exists(&orders_path).await,
        "dropping the Mink table keeps the Iceberg table"
    );
    assert_eq!(count(&lake.rows(&orders_path).await), 100);

    let attach_orders = Descriptor::builder(Schema::builder().build().unwrap())
        .bucket_count(2)
        .options(Options {
            lake_attach: true,
            ..lake_options()
        })
        .build()
        .unwrap();
    let orders = create(&admin, &cluster, &orders_path, &attach_orders).await;
    let names: Vec<&str> = orders.schema().columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["k", "v"], "schema adopted from Iceberg");
    assert!(orders.descriptor().options().lake_attach);
    let seeded = wait_for("attached snapshot seeded", async || {
        admin.lake_snapshot(&orders_path).await
    })
    .await;
    assert_eq!(seeded.snapshot_id, baseline.snapshot_id);
    assert_eq!(
        union_events(&orders).await.len(),
        100,
        "history visible before any write"
    );

    orders
        .append_writer()
        .await
        .unwrap()
        .append(&events_range(100, 150, "new"))
        .await
        .unwrap();
    assert_eq!(union_events(&orders).await.len(), 150);
    let tiered = wait_tiered_to_head(&cluster, &orders).await;
    assert_ne!(tiered.snapshot_id, baseline.snapshot_id);
    assert_eq!(
        lake.table(&orders_path)
            .await
            .metadata()
            .current_snapshot()
            .and_then(|s| s.parent_snapshot_id()),
        Some(baseline.snapshot_id),
        "new commits chain onto the baseline"
    );
    assert_eq!(count(&lake.rows(&orders_path).await), 150);

    let attach_users = users_descriptor(2, true)
        .options(Options {
            lake_attach: true,
            ..lake_options()
        })
        .build()
        .unwrap();
    let users_table = create(&admin, &cluster, &users_path, &attach_users).await;
    let mut writer = users_table.upsert_writer().await.unwrap();
    writer
        .upsert(&users(&[(2, "bob", 21), (3, "cid", 30)]))
        .await
        .unwrap();
    wait_tiered_to_head(&cluster, &users_table).await;
    let lake_users: BTreeMap<i64, User> = lake
        .rows(&users_path)
        .await
        .iter()
        .flat_map(user_rows)
        .collect();
    assert_eq!(
        lake_users,
        BTreeMap::from([
            (1, user("ann", 10)),
            (2, user("bob", 21)),
            (3, user("cid", 30)),
        ]),
        "attached PK writes retract the baseline row rather than duplicating it"
    );
    assert_eq!(
        union_users(&users_table).await,
        BTreeMap::from([
            (1, user("ann", 10)),
            (2, user("bob", 21)),
            (3, user("cid", 30)),
        ])
    );

    let mismatch = admin
        .create_table(
            &path(&db, "orders_typed"),
            &Descriptor::builder(users_schema())
                .options(Options {
                    lake_attach: true,
                    ..lake_options()
                })
                .build()
                .unwrap(),
            false,
        )
        .await;
    assert!(
        matches!(mismatch, Err(Error::Status(ref s)) if s.code() == Code::NotFound || s.code() == Code::InvalidArgument),
        "attach to a missing Iceberg table fails: {mismatch:?}"
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn kafka_produce_consume_and_group_offsets() {
    let Some(env) = Env::load() else { return };
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let kafka = Kafka::new(env.kafka_bootstrap());
    let topic = unique("orders");
    kafka.create_topic(&topic, 3).await;
    assert_eq!(kafka.partitions(&topic), 3);

    let records: Vec<(String, String)> = (0..300)
        .map(|i| (format!("k{}", i % 17), format!("v{i}")))
        .collect();
    let refs: Vec<(&str, &str)> = records
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let placed = kafka.produce(&topic, &refs).await;
    let partitions_used: std::collections::BTreeSet<i32> = placed.iter().map(|p| p.0).collect();
    assert!(
        partitions_used.len() > 1,
        "keys hash over partitions: {partitions_used:?}"
    );

    let topic_path: Path = format!("kafka.{topic}").parse().unwrap();
    let table = cluster.table(&topic_path).await.unwrap();
    assert_eq!(
        table.buckets().count(),
        3,
        "a topic is a 3 bucket log table"
    );
    let batches = scan_all(&table).await;
    assert_eq!(count(&batches), 300, "records land in the table");

    let consumed = kafka.consume_all(&topic, 300, Duration::from_secs(60));
    assert_eq!(consumed.len(), 300);
    let mut got: Vec<(String, String)> = consumed;
    got.sort();
    let mut want = records.clone();
    want.sort();
    assert_eq!(got, want);

    let group = unique("g");
    let first = kafka.consume_group(&topic, &group, 300, Duration::from_secs(60));
    assert_eq!(first.len(), 300);
    kafka.produce(&topic, &[("late", "1"), ("late", "2")]).await;
    let rest = kafka.consume_group(&topic, &group, 2, Duration::from_secs(60));
    assert_eq!(rest.len(), 2, "the group resumes at its committed offsets");

    admin.drop_table(&topic_path, false).await.unwrap();
}
