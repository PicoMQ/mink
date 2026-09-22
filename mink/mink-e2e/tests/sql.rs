//! The SQL suite: Flight SQL against the query service over a cluster with a lake, covering
//! visibility of fresh writes, unions with the lake, primary-key merges, partition pruning,
//! joins, schema evolution, catalog browsing, leader loss and a latency budget.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use mink_client::Cluster;
use mink_e2e::kafka::Kafka;
use mink_e2e::rows::*;
use mink_e2e::sql::{Sql, percentile, strings};
use mink_e2e::tables::*;
use mink_e2e::{Env, docker, fresh_database, unique, wait_for};
use mink_table::{Change, Descriptor, Path};
use mink_types::DataType;

fn path(db: &str, table: &str) -> Path {
    format!("{db}.{table}").parse().unwrap()
}

fn pairs(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
    rows.iter().map(|(k, v)| (*k, (*v).to_owned())).collect()
}

async fn setup() -> Option<(Env, Cluster, Sql)> {
    let env = Env::load()?;
    if env.query.is_empty() {
        return None;
    }
    let cluster = env.ready().await;
    let sql = Sql::connect(&env).await;
    Some((env, cluster, sql))
}

async fn wait_all_led(cluster: &Cluster, path: &Path, not_on: Option<i32>) {
    let admin = cluster.admin();
    wait_for(
        &format!("{path} fully led, not on {not_on:?}"),
        async || {
            let info = admin.get_table(path).await?;
            Ok(info
                .buckets
                .iter()
                .all(|b| b.leader.as_ref().is_some_and(|l| Some(l.node_id) != not_on))
                .then_some(()))
        },
    )
    .await
}

#[tokio::test]
async fn appended_rows_are_visible_before_and_after_tiering() {
    let Some((_, cluster, mut sql)) = setup().await else {
        return;
    };
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sql").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(3, true).build().unwrap(),
    )
    .await;
    let mut writer = table.append_writer().await.unwrap();
    writer
        .append(&events(&[(1, "a"), (2, "b"), (3, "c")]))
        .await
        .unwrap();

    assert_eq!(
        sql.keyed(&format!("SELECT k, v FROM {db}.events ORDER BY k"))
            .await,
        pairs(&[(1, "a"), (2, "b"), (3, "c")]),
        "rows are queryable as soon as they are acked"
    );

    wait_tiered_to_head(&cluster, &table).await;
    writer.append(&events(&[(4, "d"), (5, "e")])).await.unwrap();
    assert_eq!(
        sql.keyed(&format!("SELECT k, v FROM {db}.events ORDER BY k"))
            .await,
        pairs(&[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]),
        "the lake snapshot and the log tail union without gaps or duplicates"
    );
    assert_eq!(
        sql.count(&format!("SELECT count(*) FROM {db}.events WHERE k >= 4"))
            .await,
        2
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

fn files_in(plan: &str) -> usize {
    let start = plan.find("files=").expect("the scan reports its files") + "files=".len();
    plan[start..]
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn range_filters_skip_lake_files() {
    let Some((_, cluster, mut sql)) = setup().await else {
        return;
    };
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sql").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(1, true).build().unwrap(),
    )
    .await;
    let mut writer = table.append_writer().await.unwrap();
    for base in [0i64, 100, 200] {
        let values: Vec<String> = (base..base + 10).map(|k| format!("v{k}")).collect();
        let rows: Vec<(i64, &str)> = values
            .iter()
            .enumerate()
            .map(|(i, v)| (base + i as i64, v.as_str()))
            .collect();
        writer.append(&events(&rows)).await.unwrap();
        wait_tiered_to_head(&cluster, &table).await;
    }
    writer.append(&events(&[(300, "tail")])).await.unwrap();

    let all = strings(
        &sql.query(&format!("EXPLAIN SELECT k FROM {db}.events"))
            .await,
        1,
    )
    .join("\n");
    assert_eq!(files_in(&all), 3, "one file per tiering round:\n{all}");

    let filtered = strings(
        &sql.query(&format!("EXPLAIN SELECT k FROM {db}.events WHERE k >= 200"))
            .await,
        1,
    )
    .join("\n");
    assert!(filtered.contains("filter=k >= 200"), "{filtered}");
    assert_eq!(files_in(&filtered), 1, "{filtered}");
    assert_eq!(
        sql.count(&format!("SELECT count(*) FROM {db}.events WHERE k >= 200"))
            .await,
        11,
        "ten lake rows and the tail row"
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn kafka_topics_are_tables() {
    let Some((env, cluster, mut sql)) = setup().await else {
        return;
    };
    let kafka = Kafka::new(env.kafka_bootstrap());
    let topic = unique("orders");
    kafka.create_topic(&topic, 3).await;
    let records: Vec<(String, String)> = (0..120)
        .map(|i| (format!("k{}", i % 7), format!("v{i}")))
        .collect();
    let refs: Vec<(&str, &str)> = records
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    kafka.produce(&topic, &refs).await;

    assert_eq!(
        sql.count(&format!("SELECT count(*) FROM kafka.\"{topic}\""))
            .await,
        120
    );
    assert_eq!(
        sql.count(&format!(
            "SELECT count(DISTINCT key) FROM kafka.\"{topic}\""
        ))
        .await,
        7
    );

    cluster
        .admin()
        .drop_table(&path("kafka", &topic), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn primary_key_tables_read_merged_state() {
    let Some((_, cluster, mut sql)) = setup().await else {
        return;
    };
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sqlpk").await;
    let table_path = path(&db, "users");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &users_descriptor(2, true).build().unwrap(),
    )
    .await;
    let mut writer = table.upsert_writer().await.unwrap();
    writer
        .upsert(&users(&[(1, "ann", 10), (2, "bob", 20), (3, "cid", 30)]))
        .await
        .unwrap();
    assert_eq!(
        sql.keyed(&format!("SELECT id, name FROM {db}.users ORDER BY id"))
            .await,
        pairs(&[(1, "ann"), (2, "bob"), (3, "cid")]),
    );

    wait_tiered_to_head(&cluster, &table).await;
    writer
        .upsert(&users(&[(2, "BOB", 25), (4, "dee", 40)]))
        .await
        .unwrap();
    writer.delete(&users(&[(1, "ann", 10)])).await.unwrap();

    assert_eq!(
        sql.keyed(&format!("SELECT id, name FROM {db}.users ORDER BY id"))
            .await,
        pairs(&[(2, "BOB"), (3, "cid"), (4, "dee")]),
        "lake rows merged with later upserts and deletes"
    );
    assert_eq!(
        sql.count(&format!("SELECT sum(score) FROM {db}.users"))
            .await,
        95
    );

    let lookup = format!("SELECT id, name FROM {db}.users WHERE id IN (1, 2, 4) ORDER BY id");
    let explained = sql.query(&format!("EXPLAIN {lookup}")).await;
    let plan = strings(&explained, 1).join("\n");
    assert!(
        plan.contains("splits=1, files=0, keys=3"),
        "pinned primary keys are looked up, not scanned:\n{plan}"
    );
    assert_eq!(
        sql.keyed(&lookup).await,
        pairs(&[(2, "BOB"), (4, "dee")]),
        "lookups see the merged state"
    );
    assert_eq!(
        sql.count(&format!(
            "SELECT count(*) FROM {db}.users WHERE id = 3 AND score > 100"
        ))
        .await,
        0
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn results_are_stable_while_tiering_moves_the_boundary() {
    let Some((_, cluster, mut sql)) = setup().await else {
        return;
    };
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sqltier").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(3, true).build().unwrap(),
    )
    .await;
    let mut writer = table.append_writer().await.unwrap();
    let mut written = 0i64;
    for round in 0..8 {
        writer
            .append(&events_range(written, written + 250, &format!("r{round}")))
            .await
            .unwrap();
        written += 250;
        let count = sql
            .count(&format!("SELECT count(*) FROM {db}.events"))
            .await;
        assert_eq!(
            count, written,
            "round {round}: every acked row exactly once"
        );
        let distinct = sql
            .count(&format!("SELECT count(DISTINCT k) FROM {db}.events"))
            .await;
        assert_eq!(
            distinct, written,
            "round {round}: no duplicates across tiers"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    wait_tiered_to_head(&cluster, &table).await;
    assert_eq!(
        sql.count(&format!("SELECT count(*) FROM {db}.events"))
            .await,
        written
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn partition_filters_prune_and_joins_span_tables() {
    let Some((_, cluster, mut sql)) = setup().await else {
        return;
    };
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sqlpart").await;
    let regions_path = path(&db, "regions");
    let users_path = path(&db, "users");
    admin
        .create_table(
            &regions_path,
            &Descriptor::builder(partitioned_events_schema())
                .partitioned_by(["region"])
                .bucket_keys(["k"])
                .bucket_count(2)
                .options(lake_options())
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    let regions = cluster.table(&regions_path).await.unwrap();
    regions
        .append_writer()
        .await
        .unwrap()
        .append(&partitioned_events(&[
            (1, "a", "eu"),
            (2, "b", "us"),
            (3, "c", "eu"),
            (4, "d", "us"),
            (5, "e", "eu"),
            (6, "f", "apac"),
        ]))
        .await
        .unwrap();
    wait_for("partition buckets led", async || {
        let info = admin.get_table(&regions_path).await?;
        Ok(
            (info.buckets.len() == 6 && info.buckets.iter().all(|b| b.leader.is_some()))
                .then_some(()),
        )
    })
    .await;
    let users_table = create(
        &admin,
        &cluster,
        &users_path,
        &users_descriptor(1, false).build().unwrap(),
    )
    .await;
    users_table
        .upsert_writer()
        .await
        .unwrap()
        .upsert(&users(&[(1, "ann", 10), (3, "cid", 30), (4, "dee", 40)]))
        .await
        .unwrap();

    let explained = sql
        .query(&format!(
            "EXPLAIN SELECT k FROM {db}.regions WHERE region = 'eu'"
        ))
        .await;
    let plan = strings(&explained, 1).join("\n");
    assert!(
        plan.contains("splits=2") && !plan.contains("FilterExec"),
        "one partition's buckets only, filter applied exactly:\n{plan}"
    );
    let routed = format!("SELECT v FROM {db}.regions WHERE region = 'eu' AND k = 3");
    let explained = sql.query(&format!("EXPLAIN {routed}")).await;
    let plan = strings(&explained, 1).join("\n");
    assert!(
        plan.contains("splits=1") && plan.contains("FilterExec"),
        "the bucket key routes to one bucket and the filter stays:\n{plan}"
    );
    assert_eq!(sql.strings(&routed).await, ["c"]);
    assert_eq!(
        sql.count(&format!(
            "SELECT count(*) FROM {db}.regions WHERE region = 'eu'"
        ))
        .await,
        3
    );
    assert_eq!(
        sql.count(&format!(
            "SELECT count(*) FROM {db}.regions WHERE region IN ('us', 'apac')"
        ))
        .await,
        3
    );
    assert_eq!(
        sql.keyed(&format!(
            "SELECT r.k, u.name FROM {db}.regions r JOIN {db}.users u ON r.k = u.id \
             WHERE r.region = 'eu' ORDER BY r.k"
        ))
        .await,
        pairs(&[(1, "ann"), (3, "cid")]),
    );
    assert_eq!(
        sql.keyed(&format!(
            "SELECT count(*), region FROM {db}.regions GROUP BY region ORDER BY region"
        ))
        .await,
        pairs(&[(1, "apac"), (3, "eu"), (2, "us")]),
    );

    wait_tiered_to_head(&cluster, &regions).await;
    assert_eq!(
        sql.count(&format!(
            "SELECT count(*) FROM {db}.regions WHERE region = 'eu'"
        ))
        .await,
        3,
        "pruning holds once the partitions are in the lake"
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn added_columns_appear_with_nulls_for_older_rows() {
    let Some((_, cluster, mut sql)) = setup().await else {
        return;
    };
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sqlevolve").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(2, true).build().unwrap(),
    )
    .await;
    table
        .append_writer()
        .await
        .unwrap()
        .append(&events(&[(1, "a"), (2, "b")]))
        .await
        .unwrap();
    wait_tiered_to_head(&cluster, &table).await;

    admin
        .alter_table(
            &table_path,
            vec![Change::add_column("w", DataType::int())],
            false,
        )
        .await
        .unwrap();
    let table = cluster.table(&table_path).await.unwrap();
    table
        .append_writer()
        .await
        .unwrap()
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

    assert_eq!(
        sql.strings(&format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = '{db}' AND table_name = 'events' ORDER BY ordinal_position"
        ))
        .await,
        ["k", "v", "w"]
    );
    assert_eq!(
        sql.count(&format!("SELECT count(*) FROM {db}.events WHERE w IS NULL"))
            .await,
        2
    );
    assert_eq!(
        sql.count(&format!("SELECT sum(w) FROM {db}.events")).await,
        30
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn catalog_browsing_matches_the_cluster() {
    let Some((_, cluster, mut sql)) = setup().await else {
        return;
    };
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sqlcat").await;
    create(
        &admin,
        &cluster,
        &path(&db, "events"),
        &events_descriptor(1, false).build().unwrap(),
    )
    .await;
    create(
        &admin,
        &cluster,
        &path(&db, "users"),
        &users_descriptor(1, false).build().unwrap(),
    )
    .await;

    assert!(sql.schemas().await.contains(&db), "GetDbSchemas lists {db}");
    assert_eq!(sql.tables(&db).await, ["events", "users"], "GetTables");
    assert_eq!(
        sql.strings(&format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = '{db}' ORDER BY table_name"
        ))
        .await,
        ["events", "users"]
    );
    let error = sql
        .try_query(&format!("SELECT * FROM {db}.missing"))
        .await
        .expect_err("an unknown table fails");
    assert!(error.to_string().contains("missing"), "{error}");

    admin.drop_database(&db, false, true).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let tables: BTreeSet<String> = sql.tables(&db).await.into_iter().collect();
        if tables.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "dropped database is gone: {tables:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn queries_survive_a_leader_loss() {
    let Some((env, cluster, mut sql)) = setup().await else {
        return;
    };
    if !env.multi_node() {
        return;
    }
    assert!(docker::available().await, "docker socket in the runner");
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sqlchaos").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(6, true).build().unwrap(),
    )
    .await;
    table
        .append_writer()
        .await
        .unwrap()
        .append(&events_range(0, 3000, "pre"))
        .await
        .unwrap();
    wait_tiered_to_head(&cluster, &table).await;
    table
        .append_writer()
        .await
        .unwrap()
        .append(&events_range(3000, 3600, "tail"))
        .await
        .unwrap();
    let query = format!("SELECT count(*) FROM {db}.events");
    assert_eq!(sql.count(&query).await, 3600);

    let coordinator = admin
        .describe_cluster()
        .await
        .unwrap()
        .coordinator
        .map(|c| c.node_id);
    let victim = admin
        .get_table(&table_path)
        .await
        .unwrap()
        .buckets
        .iter()
        .filter_map(|b| b.leader.as_ref().map(|l| l.node_id))
        .find(|id| Some(*id) != coordinator)
        .expect("a leader that is not the coordinator");
    docker::kill(&docker::node(victim)).await;
    docker::wait_stopped(&docker::node(victim)).await;
    wait_all_led(&cluster, &table_path, Some(victim)).await;

    let deadline = tokio::time::Instant::now() + mink_e2e::WAIT;
    let count = loop {
        match sql.try_query(&query).await {
            Ok(batches) => {
                break batches[0].column(0).as_primitive::<Int64Type>().value(0);
            }
            Err(error) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "query after leader loss: {error}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    };
    assert_eq!(
        count, 3600,
        "the new leaders serve the tail, the lake the rest"
    );

    docker::start(&docker::node(victim)).await;
    docker::wait_healthy(&docker::node(victim)).await;
    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn latency_stays_within_budget() {
    let Some((_, cluster, mut sql)) = setup().await else {
        return;
    };
    let admin = cluster.admin();
    let db = fresh_database(&admin, "sqlperf").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(3, true).build().unwrap(),
    )
    .await;
    let mut writer = table.append_writer().await.unwrap();
    for chunk in 0..20 {
        writer
            .append(&events_range(chunk * 5_000, (chunk + 1) * 5_000, "load"))
            .await
            .unwrap();
    }
    wait_tiered_to_head(&cluster, &table).await;
    writer
        .append(&events_range(100_000, 101_000, "tail"))
        .await
        .unwrap();
    let users_table = create(
        &admin,
        &cluster,
        &path(&db, "users"),
        &users_descriptor(3, true).build().unwrap(),
    )
    .await;
    let mut upserts = users_table.upsert_writer().await.unwrap();
    for chunk in 0..4 {
        upserts
            .upsert(&users_range(chunk * 5_000, (chunk + 1) * 5_000))
            .await
            .unwrap();
    }

    let queries = [
        format!("SELECT count(*) FROM {db}.events"),
        format!("SELECT count(*), max(k) FROM {db}.events WHERE k % 10 = 3"),
        format!("SELECT v FROM {db}.events WHERE k BETWEEN 50000 AND 50010 ORDER BY k"),
        format!("SELECT name FROM {db}.users WHERE id IN (7, 7007, 17017)"),
    ];
    for query in &queries {
        let (batches, _) = sql.timed(query).await;
        assert!(!batches.is_empty(), "{query}");
        let mut samples = Vec::with_capacity(15);
        for _ in 0..15 {
            let (_, elapsed) = sql.timed(query).await;
            samples.push(elapsed);
        }
        let p50 = percentile(&mut samples, 0.5);
        let p99 = percentile(&mut samples, 0.99);
        eprintln!("{query}\n  p50 {p50:?} p99 {p99:?}");
        assert!(p50 <= Duration::from_millis(500), "{query}: p50 {p50:?}");
        assert!(p99 <= Duration::from_millis(1_000), "{query}: p99 {p99:?}");
    }
    assert_eq!(sql.count(&queries[0]).await, 101_000);
    assert_eq!(sql.strings(&queries[3]).await.len(), 3);

    admin.drop_database(&db, false, true).await.unwrap();
}
