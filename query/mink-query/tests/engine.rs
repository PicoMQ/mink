//! SQL over an in-process node with an in-memory Iceberg lake: log tails, lake plus tail unions,
//! primary-key merges, partition and bucket pruning, key lookups, joins and the catalog.

use std::net::TcpListener;
use std::path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::physical_plan::displayable;
use mink_client::{Cluster, Table};
use mink_lake::Config as LakeConfig;
use mink_lake::iceberg::Config as Iceberg;
use mink_query::{Config, Engine};
use mink_runtime::{Server, ServerConfig, start};
use mink_table::{Column, Descriptor, LakeFormat, Options, Path, PrimaryKey, Schema};
use mink_types::DataType;
use tokio::time::Instant;

fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(dir: &path::Path, port: u16, warehouse: &str) -> ServerConfig {
    ServerConfig {
        node_id: 1,
        cluster_id: "query-test".into(),
        meta_url: format!("sqlite:{}", dir.join("meta.db").display()),
        storage_uri: format!("-2@file://{}", dir.join("objects").display()),
        wal_uri: None,
        data_dir: dir.join("n1"),
        listen: format!("127.0.0.1:{port}").parse().unwrap(),
        advertise: format!("grpc://127.0.0.1:{port}"),
        lease_ttl: Duration::from_secs(2),
        lake: Some(LakeConfig::Iceberg(Iceberg::memory(warehouse))),
        tiering_poll_interval: Duration::from_millis(100),
        ..ServerConfig::default()
    }
}

fn lake_options() -> Options {
    Options {
        lake: Some(LakeFormat::Iceberg),
        lake_freshness: Duration::from_millis(200),
        ..Options::default()
    }
}

fn events_schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .build()
        .unwrap()
}

fn users_schema() -> Schema {
    Schema::builder()
        .column(Column::new("id", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("name", DataType::string()).unwrap())
        .column(Column::new("score", DataType::int()).unwrap())
        .primary_key(PrimaryKey::new(vec!["id".into()]).unwrap())
        .build()
        .unwrap()
}

fn regions_schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .column(Column::new("region", DataType::string().with_nullable(false)).unwrap())
        .build()
        .unwrap()
}

fn accounts_schema() -> Schema {
    Schema::builder()
        .column(Column::new("id", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("region", DataType::string().with_nullable(false)).unwrap())
        .column(Column::new("name", DataType::string()).unwrap())
        .primary_key(PrimaryKey::new(vec!["id".into(), "region".into()]).unwrap())
        .build()
        .unwrap()
}

fn events(rows: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(events_schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
        ],
    )
    .unwrap()
}

fn users(rows: &[(i64, &str, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(users_schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

fn regions(rows: &[(i64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(regions_schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

fn accounts(rows: &[(i64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(accounts_schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

async fn wait_until(what: &str, mut ready: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready().await {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn create(cluster: &Cluster, path: &Path, descriptor: &Descriptor) -> Table {
    let admin = cluster.admin();
    admin.create_table(path, descriptor, false).await.unwrap();
    wait_until("buckets led", async || {
        let info = admin.get_table(path).await.unwrap();
        !info.buckets.is_empty() && info.buckets.iter().all(|b| b.leader.is_some())
    })
    .await;
    cluster.table(path).await.unwrap()
}

async fn tiered_to(cluster: &Cluster, path: &Path, offsets: i64) {
    let admin = cluster.admin();
    wait_until(&format!("{path} tiered to {offsets}"), async || {
        admin
            .lake_snapshot(path)
            .await
            .unwrap()
            .is_some_and(|s| s.bucket_log_end_offset.iter().map(|(_, o)| o).sum::<i64>() == offsets)
    })
    .await;
}

async fn rows(engine: &Engine, sql: &str) -> Vec<RecordBatch> {
    engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn keyed(engine: &Engine, sql: &str) -> Vec<(i64, String)> {
    rows(engine, sql)
        .await
        .iter()
        .flat_map(|b| {
            let k = b.column(0).as_primitive::<Int64Type>();
            let v = b.column(1).as_string::<i32>();
            (0..b.num_rows())
                .map(|i| (k.value(i), v.value(i).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn count(engine: &Engine, sql: &str) -> i64 {
    let batches = rows(engine, sql).await;
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

async fn strings(engine: &Engine, sql: &str) -> Vec<String> {
    rows(engine, sql)
        .await
        .iter()
        .flat_map(|b| {
            let s = b.column(0).as_string::<i32>();
            (0..b.num_rows())
                .map(|i| s.value(i).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn physical_plan(engine: &Engine, sql: &str) -> String {
    let plan = engine
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    displayable(plan.as_ref()).indent(false).to_string()
}

fn pairs(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
    rows.iter().map(|(k, v)| (*k, (*v).to_owned())).collect()
}

struct Stack {
    server: Server,
    cluster: Cluster,
    engine: Engine,
    _dir: tempfile::TempDir,
}

async fn stack() -> Stack {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", dir.path().join("lake").display());
    let port = reserve_port();
    let server = start(config(dir.path(), port, &warehouse)).await.unwrap();
    let coordinator = server.coordinator().clone();
    wait_until("lease", async || coordinator.is_leader()).await;
    let cluster = Cluster::connect(server.advertise()).unwrap();
    cluster
        .admin()
        .create_database("q", None, Default::default(), false)
        .await
        .unwrap();

    let mut config = Config::new(vec![server.advertise().to_owned()]);
    config.database = Some("q".into());
    config.lake = Some(LakeConfig::Iceberg(Iceberg::memory(&warehouse)));
    let engine = Engine::connect(config).await.unwrap();

    Stack {
        server,
        cluster,
        engine,
        _dir: dir,
    }
}

#[tokio::test]
async fn log_tables_union_the_lake_with_the_tail() {
    let stack = stack().await;
    let path: Path = "q.events".parse().unwrap();
    let table = create(
        &stack.cluster,
        &path,
        &Descriptor::builder(events_schema())
            .bucket_count(2)
            .options(lake_options())
            .build()
            .unwrap(),
    )
    .await;
    let mut writer = table.append_writer().await.unwrap();
    writer
        .append(&events(&[(1, "a"), (2, "b"), (3, "c")]))
        .await
        .unwrap();

    assert_eq!(
        keyed(&stack.engine, "SELECT k, v FROM events ORDER BY k").await,
        pairs(&[(1, "a"), (2, "b"), (3, "c")]),
        "the tail alone answers before anything is tiered"
    );

    tiered_to(&stack.cluster, &path, 3).await;
    writer.append(&events(&[(4, "d"), (5, "e")])).await.unwrap();

    assert_eq!(
        keyed(&stack.engine, "SELECT k, v FROM q.events ORDER BY k").await,
        pairs(&[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]),
    );
    assert_eq!(count(&stack.engine, "SELECT count(*) FROM events").await, 5);
    assert_eq!(
        strings(&stack.engine, "SELECT v FROM events WHERE k > 3 ORDER BY v").await,
        ["d", "e"]
    );

    let plan = physical_plan(&stack.engine, "SELECT k FROM events").await;
    assert!(
        plan.contains("MinkScan: table=q.events, splits=3, files=1, projection=[0]"),
        "one lake read plus two tails, projected to k:\n{plan}"
    );

    stack.server.shutdown().await;
}

#[tokio::test]
async fn primary_key_tables_merge_upserts_and_deletes() {
    let stack = stack().await;
    let path: Path = "q.users".parse().unwrap();
    let table = create(
        &stack.cluster,
        &path,
        &Descriptor::builder(users_schema())
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    )
    .await;
    let mut writer = table.upsert_writer().await.unwrap();
    writer
        .upsert(&users(&[(1, "ann", 10), (2, "bob", 20)]))
        .await
        .unwrap();
    assert_eq!(
        keyed(&stack.engine, "SELECT id, name FROM users ORDER BY id").await,
        pairs(&[(1, "ann"), (2, "bob")]),
        "the node's snapshot answers before the lake has a snapshot"
    );

    tiered_to(&stack.cluster, &path, 2).await;
    writer
        .upsert(&users(&[(2, "BOB", 25), (3, "cid", 30)]))
        .await
        .unwrap();
    writer.delete(&users(&[(1, "ann", 10)])).await.unwrap();

    assert_eq!(
        keyed(&stack.engine, "SELECT id, name FROM users ORDER BY id").await,
        pairs(&[(2, "BOB"), (3, "cid")]),
    );
    assert_eq!(
        count(&stack.engine, "SELECT sum(score) FROM users").await,
        55
    );

    stack.server.shutdown().await;
}

#[tokio::test]
async fn partition_filters_prune_buckets_and_joins_work() {
    let stack = stack().await;
    let regions_path: Path = "q.regions".parse().unwrap();
    let users_path: Path = "q.users".parse().unwrap();
    stack
        .cluster
        .admin()
        .create_table(
            &regions_path,
            &Descriptor::builder(regions_schema())
                .partitioned_by(["region"])
                .bucket_keys(["k"])
                .bucket_count(2)
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    let regions_table = stack.cluster.table(&regions_path).await.unwrap();
    regions_table
        .append_writer()
        .await
        .unwrap()
        .append(&regions(&[
            (1, "a", "eu"),
            (2, "b", "us"),
            (3, "c", "eu"),
            (4, "d", "us"),
            (5, "e", "eu"),
        ]))
        .await
        .unwrap();
    let users_table = create(
        &stack.cluster,
        &users_path,
        &Descriptor::builder(users_schema())
            .bucket_count(1)
            .build()
            .unwrap(),
    )
    .await;
    users_table
        .upsert_writer()
        .await
        .unwrap()
        .upsert(&users(&[(1, "ann", 10), (3, "cid", 30), (4, "dee", 40)]))
        .await
        .unwrap();

    let plan = physical_plan(&stack.engine, "SELECT k FROM regions WHERE region = 'eu'").await;
    assert!(
        plan.contains("splits=2") && !plan.contains("FilterExec"),
        "only eu buckets are scanned and the filter is exact:\n{plan}"
    );
    let plan = physical_plan(
        &stack.engine,
        "SELECT v FROM regions WHERE region = 'eu' AND k = 1",
    )
    .await;
    assert!(
        plan.contains("splits=1") && plan.contains("FilterExec"),
        "the bucket key routes to one bucket of the eu partition and the filter stays:\n{plan}"
    );
    assert_eq!(
        strings(
            &stack.engine,
            "SELECT v FROM regions WHERE region = 'eu' AND k = 1"
        )
        .await,
        ["a"]
    );
    let plan = physical_plan(&stack.engine, "SELECT v FROM regions WHERE k = 1").await;
    assert!(
        plan.contains("splits=4"),
        "a bucket key alone cannot route without the partition:\n{plan}"
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM regions WHERE region = 'eu'"
        )
        .await,
        3
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM regions WHERE region IN ('eu', 'us')"
        )
        .await,
        5
    );
    assert_eq!(
        keyed(
            &stack.engine,
            "SELECT r.k, u.name FROM regions r JOIN users u ON r.k = u.id WHERE r.region = 'eu' \
             ORDER BY r.k"
        )
        .await,
        pairs(&[(1, "ann"), (3, "cid")]),
    );

    stack.server.shutdown().await;
}

#[tokio::test]
async fn catalog_lists_databases_and_tables() {
    let stack = stack().await;
    create(
        &stack.cluster,
        &"q.events".parse().unwrap(),
        &Descriptor::builder(events_schema())
            .bucket_count(1)
            .build()
            .unwrap(),
    )
    .await;
    stack
        .cluster
        .admin()
        .create_database("other", None, Default::default(), false)
        .await
        .unwrap();

    assert_eq!(
        strings(
            &stack.engine,
            "SELECT table_name FROM information_schema.tables \
             WHERE table_catalog = 'mink' AND table_schema = 'q'"
        )
        .await,
        ["events"]
    );
    assert_eq!(
        strings(
            &stack.engine,
            "SELECT schema_name FROM information_schema.schemata \
             WHERE catalog_name = 'mink' AND schema_name <> 'information_schema' \
             ORDER BY schema_name"
        )
        .await,
        ["other", "q"]
    );
    assert_eq!(
        strings(
            &stack.engine,
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = 'q' AND table_name = 'events' ORDER BY ordinal_position"
        )
        .await,
        ["k", "v"]
    );
    let error = stack
        .engine
        .sql("SELECT * FROM missing")
        .await
        .expect_err("unknown table fails at planning");
    assert!(error.to_string().contains("missing"), "{error}");

    stack.server.shutdown().await;
}

fn files_in(plan: &str) -> usize {
    counter_in(plan, "files=")
}

fn splits_in(plan: &str) -> usize {
    counter_in(plan, "splits=")
}

fn counter_in(plan: &str, key: &str) -> usize {
    let start = plan.find(key).expect("the scan reports its counters") + key.len();
    plan[start..]
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn range_filters_prune_lake_files_and_limits_reach_the_scan() {
    let stack = stack().await;
    let path: Path = "q.events".parse().unwrap();
    let table = create(
        &stack.cluster,
        &path,
        &Descriptor::builder(events_schema())
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    )
    .await;
    let mut writer = table.append_writer().await.unwrap();
    for (round, base) in [0i64, 100, 200].into_iter().enumerate() {
        let batch: Vec<(i64, String)> = (base..base + 10).map(|k| (k, format!("v{k}"))).collect();
        let refs: Vec<(i64, &str)> = batch.iter().map(|(k, v)| (*k, v.as_str())).collect();
        writer.append(&events(&refs)).await.unwrap();
        tiered_to(&stack.cluster, &path, 10 * (round as i64 + 1)).await;
    }
    writer.append(&events(&[(300, "tail")])).await.unwrap();

    let all = physical_plan(&stack.engine, "SELECT k FROM events").await;
    assert_eq!(files_in(&all), 3, "one file per tiering round:\n{all}");

    let filtered = physical_plan(&stack.engine, "SELECT k FROM events WHERE k >= 200").await;
    assert!(
        filtered.contains("filter=k >= 200") && filtered.contains("FilterExec"),
        "the filter is pushed to the lake and kept above the scan:\n{filtered}"
    );
    assert_eq!(
        files_in(&filtered),
        1,
        "two files fall outside the range:\n{filtered}"
    );
    assert_eq!(
        keyed(
            &stack.engine,
            "SELECT k, v FROM events WHERE k >= 200 ORDER BY k"
        )
        .await,
        (200..210)
            .map(|k| (k, format!("v{k}")))
            .chain([(300, "tail".to_owned())])
            .collect::<Vec<_>>(),
        "the tail is filtered by DataFusion"
    );

    let mixed = physical_plan(
        &stack.engine,
        "SELECT k FROM events WHERE k IN (5, 105) AND v IS NOT NULL AND length(v) > 1",
    )
    .await;
    assert!(
        mixed.contains("filter=(k = 5 OR k = 105) AND v IS NOT NULL"),
        "only convertible conjuncts are pushed:\n{mixed}"
    );
    assert_eq!(files_in(&mixed), 2, "{mixed}");

    let limited = physical_plan(&stack.engine, "SELECT k FROM events LIMIT 3").await;
    assert!(limited.contains("limit=3"), "{limited}");
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM (SELECT k FROM events LIMIT 3)"
        )
        .await,
        3
    );

    stack.server.shutdown().await;
}

#[tokio::test]
async fn filters_on_columns_newer_than_the_snapshot_fold_for_the_lake() {
    let stack = stack().await;
    let path: Path = "q.events".parse().unwrap();
    let table = create(
        &stack.cluster,
        &path,
        &Descriptor::builder(events_schema())
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    )
    .await;
    table
        .append_writer()
        .await
        .unwrap()
        .append(&events(&[(1, "a"), (2, "b")]))
        .await
        .unwrap();
    tiered_to(&stack.cluster, &path, 2).await;

    stack
        .cluster
        .admin()
        .alter_table(
            &path,
            vec![mink_table::Change::add_column("w", DataType::int())],
            false,
        )
        .await
        .unwrap();
    let table = stack.cluster.table(&path).await.unwrap();
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
        count(&stack.engine, "SELECT count(*) FROM events WHERE w IS NULL").await,
        2
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM events WHERE w IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        count(&stack.engine, "SELECT count(*) FROM events WHERE w = 30").await,
        1
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM events WHERE w IN (30, 31)"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM events WHERE NOT (w = 30)"
        )
        .await,
        0,
        "SQL three-valued logic: NULL = 30 is unknown, not false"
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM events WHERE w IS NULL OR k = 3"
        )
        .await,
        3
    );

    stack.server.shutdown().await;
}

#[tokio::test]
async fn bucket_key_filters_prune_buckets_on_the_hot_path() {
    let stack = stack().await;
    let path: Path = "q.events".parse().unwrap();
    let table = create(
        &stack.cluster,
        &path,
        &Descriptor::builder(events_schema())
            .bucket_keys(["k"])
            .bucket_count(4)
            .build()
            .unwrap(),
    )
    .await;
    let rows: Vec<(i64, String)> = (1..=8).map(|k| (k, format!("v{k}"))).collect();
    let refs: Vec<(i64, &str)> = rows.iter().map(|(k, v)| (*k, v.as_str())).collect();
    table
        .append_writer()
        .await
        .unwrap()
        .append(&events(&refs))
        .await
        .unwrap();

    let one = physical_plan(&stack.engine, "SELECT v FROM events WHERE k = 3").await;
    assert!(
        one.contains("splits=1") && one.contains("FilterExec"),
        "k routes to one bucket and the filter stays:\n{one}"
    );
    assert_eq!(
        strings(&stack.engine, "SELECT v FROM events WHERE k = 3").await,
        ["v3"]
    );
    assert_eq!(
        keyed(
            &stack.engine,
            "SELECT k, v FROM events WHERE k IN (1, 2, 3, 4, 5, 6, 7, 8) ORDER BY k"
        )
        .await,
        rows,
        "every key still comes back through its own bucket"
    );
    assert_eq!(
        keyed(
            &stack.engine,
            "SELECT k, v FROM events WHERE k = 3 OR k = 5 ORDER BY k"
        )
        .await,
        pairs(&[(3, "v3"), (5, "v5")]),
    );
    let disjunction =
        physical_plan(&stack.engine, "SELECT v FROM events WHERE k = 3 OR k = 5").await;
    assert!(
        splits_in(&disjunction) <= 2,
        "a disjunction on the key routes to its buckets:\n{disjunction}"
    );
    let mixed = physical_plan(
        &stack.engine,
        "SELECT v FROM events WHERE k = 3 OR v = 'v5'",
    )
    .await;
    assert_eq!(
        splits_in(&mixed),
        4,
        "a disjunction over other columns pins nothing:\n{mixed}"
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM events WHERE k IN (3, 5) AND v = 'nope'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM events WHERE k = 3 AND k = 5"
        )
        .await,
        0,
        "contradictory pins scan nothing"
    );

    stack.server.shutdown().await;
}

#[tokio::test]
async fn primary_key_filters_look_keys_up_instead_of_scanning() {
    let stack = stack().await;
    let path: Path = "q.users".parse().unwrap();
    let table = create(
        &stack.cluster,
        &path,
        &Descriptor::builder(users_schema())
            .bucket_count(2)
            .options(lake_options())
            .build()
            .unwrap(),
    )
    .await;
    let mut writer = table.upsert_writer().await.unwrap();
    writer
        .upsert(&users(&[(1, "ann", 10), (2, "bob", 20), (3, "cid", 30)]))
        .await
        .unwrap();

    let one = physical_plan(&stack.engine, "SELECT id, name FROM users WHERE id = 2").await;
    assert!(
        one.contains("splits=1, files=0, keys=1"),
        "one pinned key is one lookup:\n{one}"
    );
    assert_eq!(
        keyed(&stack.engine, "SELECT id, name FROM users WHERE id = 2").await,
        pairs(&[(2, "bob")])
    );

    tiered_to(&stack.cluster, &path, 3).await;
    writer
        .upsert(&users(&[(2, "BOB", 25), (4, "dan", 40)]))
        .await
        .unwrap();
    writer.delete(&users(&[(1, "ann", 10)])).await.unwrap();

    let many = physical_plan(
        &stack.engine,
        "SELECT id, name FROM users WHERE id IN (1, 2, 3, 4) ORDER BY id",
    )
    .await;
    assert!(many.contains("keys=4"), "{many}");
    assert_eq!(
        keyed(
            &stack.engine,
            "SELECT id, name FROM users WHERE id IN (1, 2, 3, 4) ORDER BY id"
        )
        .await,
        pairs(&[(2, "BOB"), (3, "cid"), (4, "dan")]),
        "lookups see the latest upserts and deletes"
    );
    assert_eq!(
        strings(&stack.engine, "SELECT name FROM users WHERE id = 4").await,
        ["dan"],
        "projection applies to looked-up rows"
    );
    assert_eq!(
        count(&stack.engine, "SELECT count(*) FROM users WHERE id = 3").await,
        1
    );
    assert_eq!(
        count(
            &stack.engine,
            "SELECT count(*) FROM users WHERE id = 2 AND score > 100"
        )
        .await,
        0,
        "the remaining filter applies to looked-up rows"
    );

    let keys: Vec<String> = (1..=1025).map(|k| k.to_string()).collect();
    let capped = physical_plan(
        &stack.engine,
        &format!("SELECT id FROM users WHERE id IN ({})", keys.join(", ")),
    )
    .await;
    assert!(
        !capped.contains("keys=") && capped.contains("splits=2"),
        "past the cap the query scans the buckets:\n{capped}"
    );
    assert_eq!(
        count(
            &stack.engine,
            &format!(
                "SELECT count(*) FROM users WHERE id IN ({})",
                keys.join(", ")
            )
        )
        .await,
        3
    );

    stack.server.shutdown().await;
}

#[tokio::test]
async fn partitioned_primary_keys_look_up_when_the_partition_is_pinned_too() {
    let stack = stack().await;
    let path: Path = "q.accounts".parse().unwrap();
    stack
        .cluster
        .admin()
        .create_table(
            &path,
            &Descriptor::builder(accounts_schema())
                .partitioned_by(["region"])
                .bucket_count(1)
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    let table = stack.cluster.table(&path).await.unwrap();
    table
        .upsert_writer()
        .await
        .unwrap()
        .upsert(&accounts(&[
            (1, "eu", "ann"),
            (1, "us", "ann-us"),
            (2, "eu", "bob"),
        ]))
        .await
        .unwrap();

    let pinned = physical_plan(
        &stack.engine,
        "SELECT name FROM accounts WHERE id = 1 AND region = 'us'",
    )
    .await;
    assert!(pinned.contains("keys=1"), "{pinned}");
    assert_eq!(
        strings(
            &stack.engine,
            "SELECT name FROM accounts WHERE id = 1 AND region = 'us'"
        )
        .await,
        ["ann-us"]
    );
    let unpinned = physical_plan(&stack.engine, "SELECT name FROM accounts WHERE id = 1").await;
    assert!(
        !unpinned.contains("keys=") && unpinned.contains("splits=2"),
        "without the partition every partition's bucket is read:\n{unpinned}"
    );
    assert_eq!(
        strings(
            &stack.engine,
            "SELECT name FROM accounts WHERE id = 1 ORDER BY name"
        )
        .await,
        ["ann", "ann-us"]
    );
    assert_eq!(
        strings(
            &stack.engine,
            "SELECT name FROM accounts WHERE id IN (1, 2) AND region IN ('eu', 'us') ORDER BY name"
        )
        .await,
        ["ann", "ann-us", "bob"],
        "a missing key in the product is simply absent"
    );

    stack.server.shutdown().await;
}
