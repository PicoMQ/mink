//! Several nodes on one metadata store: leadership, failover, writes and reads across nodes.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use futures::{StreamExt, TryStreamExt};
use mink_client::{Cluster, Error, Table, proto};
use mink_lake::Config;
use mink_runtime::{Server, ServerConfig, start};
use mink_table::{
    Bucket, Change, Column, Descriptor, LakeFormat, Options, Path, PrimaryKey, Schema, SchemaId,
};
use mink_types::DataType;
use tokio::time::Instant;
use tonic::Code;

const WAIT: Duration = Duration::from_secs(30);

fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(dir: &path::Path, node_id: i32, port: u16, warehouse: &str) -> ServerConfig {
    ServerConfig {
        node_id,
        cluster_id: "cluster-test".into(),
        meta_url: format!("sqlite:{}", dir.join("meta.db").display()),
        storage_uri: format!("-2@file://{}", dir.join("objects").display()),
        wal_uri: None,
        data_dir: dir.join(format!("n{node_id}")),
        listen: format!("127.0.0.1:{port}").parse().unwrap(),
        advertise: format!("grpc://127.0.0.1:{port}"),
        wal_upload_interval: Duration::from_millis(200),
        kv_snapshot_interval: Duration::from_millis(500),
        log_retention_interval: Duration::from_millis(200),
        lease_ttl: Duration::from_secs(2),
        coordinator_tick: Duration::from_millis(300),
        default_bucket_count: 2,
        lake: Some(Config::Iceberg(mink_lake::iceberg::Config::memory(
            warehouse,
        ))),
        tiering_poll_interval: Duration::from_millis(100),
        ..ServerConfig::default()
    }
}

async fn wait_for<T>(what: &str, mut probe: impl AsyncFnMut() -> Result<Option<T>, Error>) -> T {
    let deadline = Instant::now() + WAIT;
    let mut last = None;
    loop {
        match probe().await {
            Ok(Some(value)) => return value,
            Ok(None) => {}
            Err(e) if e.is_retriable() => last = Some(e),
            Err(e) => panic!("{what}: {e}"),
        }
        assert!(
            Instant::now() < deadline,
            "timed out: {what} (last error: {last:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
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

fn user_keys(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from_iter_values(ids.iter().copied())) as ArrayRef],
    )
    .unwrap()
}

fn event_rows(batch: &RecordBatch) -> Vec<(i64, String)> {
    let k = batch
        .column_by_name("k")
        .unwrap()
        .as_primitive::<Int64Type>();
    let v = batch.column_by_name("v").unwrap().as_string::<i32>();
    (0..batch.num_rows())
        .map(|i| (k.value(i), v.value(i).to_owned()))
        .collect()
}

fn user_rows(batch: &RecordBatch) -> BTreeMap<i64, (String, i32)> {
    let id = batch
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int64Type>();
    let name = batch.column_by_name("name").unwrap().as_string::<i32>();
    let score = batch
        .column_by_name("score")
        .unwrap()
        .as_primitive::<Int32Type>();
    (0..batch.num_rows())
        .map(|i| (id.value(i), (name.value(i).to_owned(), score.value(i))))
        .collect()
}

fn user(name: &str, score: i32) -> (String, i32) {
    (name.to_owned(), score)
}

async fn scan_all(table: &Table) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        let (start, end) = table.offsets(bucket).await.unwrap();
        let batches: Vec<_> = table
            .scan(bucket, start, end, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        rows.extend(batches.iter().flat_map(|b| event_rows(&b.rows)));
    }
    rows.sort();
    rows
}

async fn union_events(table: &Table) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        let batches: Vec<RecordBatch> = table
            .union(bucket, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        rows.extend(batches.iter().flat_map(event_rows));
    }
    rows.sort();
    rows
}

async fn union_users(table: &Table) -> BTreeMap<i64, (String, i32)> {
    let mut rows = BTreeMap::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        let batches: Vec<RecordBatch> = table
            .union(bucket, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        rows.extend(batches.iter().flat_map(user_rows));
    }
    rows
}

async fn high_watermarks(table: &Table) -> BTreeMap<Bucket, i64> {
    let mut out = BTreeMap::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        out.insert(bucket, table.offsets(bucket).await.unwrap().1);
    }
    out
}

async fn wait_tiered(cluster: &Cluster, path: &Path, watermarks: &BTreeMap<Bucket, i64>) {
    let admin = cluster.admin();
    wait_for(&format!("{path} tiered to {watermarks:?}"), async || {
        let Some(snapshot) = admin.lake_snapshot(path).await? else {
            return Ok(None);
        };
        let covered: BTreeMap<_, _> = snapshot.bucket_log_end_offset.iter().copied().collect();
        Ok(watermarks
            .iter()
            .all(|(b, hw)| covered.get(b).is_some_and(|end| end >= hw))
            .then_some(snapshot))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_end_to_end_through_the_client() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let ports = [reserve_port(), reserve_port()];
    let mut nodes: Vec<Server> = Vec::new();
    for (i, port) in ports.iter().enumerate() {
        nodes.push(
            start(config(dir.path(), i as i32 + 1, *port, &warehouse))
                .await
                .unwrap(),
        );
    }
    let cluster = Cluster::connect_all(
        ports
            .iter()
            .map(|p| format!("grpc://127.0.0.1:{p}"))
            .collect(),
    )
    .unwrap();
    let admin = cluster.admin();

    let info = wait_for("two live nodes", async || {
        let info = admin.describe_cluster().await?;
        Ok(
            (info.nodes.iter().filter(|n| n.live).count() == 2 && info.coordinator.is_some())
                .then_some(info),
        )
    })
    .await;
    assert_eq!(info.tables, 0);

    admin
        .create_database("shop", Some("the shop"), BTreeMap::new(), false)
        .await
        .unwrap();
    assert!(
        admin
            .list_databases()
            .await
            .unwrap()
            .contains(&"shop".to_owned())
    );

    let events_path: Path = "shop.events".parse().unwrap();
    let users_path: Path = "shop.users".parse().unwrap();
    admin
        .create_table(
            &events_path,
            &Descriptor::builder(events_schema())
                .bucket_keys(["k"])
                .bucket_count(2)
                .options(Options {
                    lake: Some(LakeFormat::Iceberg),
                    lake_freshness: Duration::from_millis(200),
                    log_ttl: Some(Duration::from_secs(1)),
                    ..Options::default()
                })
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap()
        .unwrap();
    admin
        .create_table(
            &users_path,
            &Descriptor::builder(users_schema())
                .bucket_count(2)
                .options(Options {
                    lake: Some(LakeFormat::Iceberg),
                    lake_freshness: Duration::from_millis(200),
                    ..Options::default()
                })
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(cluster.session().seen() > 0);
    for port in &ports {
        let node = cluster
            .connection(&format!("grpc://127.0.0.1:{port}"))
            .unwrap();
        let info: proto::TableInfo = node
            .action_one(
                proto::action::GET_TABLE,
                &proto::TableRef {
                    path: users_path.clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(info.path, users_path);
    }
    let mut listed = admin.list_tables("shop").await.unwrap();
    listed.sort();
    assert_eq!(listed, ["events", "users"]);
    assert_eq!(
        admin
            .create_table(
                &users_path,
                &Descriptor::builder(users_schema()).build().unwrap(),
                true
            )
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        admin
            .create_table(
                &users_path,
                &Descriptor::builder(users_schema()).build().unwrap(),
                false
            )
            .await,
        Err(Error::Status(status)) if status.code() == Code::AlreadyExists
    ));

    let led = wait_for("all buckets led", async || {
        let info = admin.describe_cluster().await?;
        Ok((info.buckets == 4 && info.unled_buckets == 0).then_some(info))
    })
    .await;
    assert!(
        led.nodes.iter().all(|n| n.leading > 0),
        "buckets spread over both nodes: {led:?}"
    );

    let events_table = cluster.table(&events_path).await.unwrap();
    let users_table = cluster.table(&users_path).await.unwrap();
    assert_eq!(events_table.buckets().count(), 2);
    let event_buckets: Vec<Bucket> = events_table.buckets().collect();

    let mut tails = Vec::new();
    for bucket in &event_buckets {
        tails.push(events_table.tail(*bucket, 0, None).await.unwrap());
    }

    let mut appender = events_table.append_writer().await.unwrap();
    let first: Vec<(i64, &str)> = (1..=6).map(|k| (k, "a")).collect();
    let routed = appender.append(&events(&first)).await.unwrap();
    assert_eq!(routed.iter().map(|b| b.rows).sum::<usize>(), 6);
    assert_eq!(routed.len(), 2, "k spreads over both buckets");

    let mut tailed = Vec::new();
    for tail in &mut tails {
        while tailed.len() < 6 {
            let batch = tail.next().await.unwrap().unwrap();
            tailed.extend(event_rows(&batch.rows));
            if batch.meta.last_offset + 1 >= batch.meta.high_watermark {
                break;
            }
        }
    }
    tailed.sort();
    assert_eq!(
        tailed,
        first
            .iter()
            .map(|(k, v)| (*k, v.to_string()))
            .collect::<Vec<_>>()
    );
    drop(tails);

    assert_eq!(scan_all(&events_table).await.len(), 6);

    let mut upserter = users_table.upsert_writer().await.unwrap();
    upserter
        .upsert(&users(&[
            (1, "ann", 10),
            (2, "bob", 20),
            (3, "cid", 30),
            (4, "dee", 40),
        ]))
        .await
        .unwrap();
    let lookuper = users_table.lookuper().unwrap();
    let row = lookuper
        .lookup_one(&user_keys(&[2]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user_rows(&row)[&2], user("bob", 20));

    upserter.delete(&users(&[(3, "", 0)])).await.unwrap();
    assert!(
        lookuper
            .lookup_one(&user_keys(&[3]))
            .await
            .unwrap()
            .is_none()
    );

    let mut partial = users_table.partial_update_writer(vec![0, 2]).await.unwrap();
    partial.upsert(&users(&[(1, "ignored", 11)])).await.unwrap();
    let row = lookuper
        .lookup_one(&user_keys(&[1]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user_rows(&row)[&1], user("ann", 11));

    let found = lookuper.lookup(&user_keys(&[4, 3, 1])).await.unwrap();
    assert_eq!(
        found.iter().map(|r| r.is_some()).collect::<Vec<_>>(),
        [true, false, true]
    );

    let user_hw = high_watermarks(&users_table).await;
    for (bucket, hw) in &user_hw {
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
    let mut bootstrapped = BTreeMap::new();
    for (bucket, hw) in &user_hw {
        let snapshot = users_table.snapshot(*bucket, Some(1)).await.unwrap();
        assert_eq!(snapshot.log_offset, *hw, "snapshot is as fresh as the log");
        let batches: Vec<RecordBatch> = snapshot.batches.try_collect().await.unwrap();
        bootstrapped.extend(batches.iter().flat_map(user_rows));
    }
    assert_eq!(
        bootstrapped,
        BTreeMap::from([
            (1, user("ann", 11)),
            (2, user("bob", 20)),
            (4, user("dee", 40)),
        ])
    );

    let event_hw = high_watermarks(&events_table).await;
    wait_tiered(&cluster, &events_path, &event_hw).await;
    wait_tiered(&cluster, &users_path, &user_hw).await;

    appender
        .append(&events(&[(7, "b"), (8, "b")]))
        .await
        .unwrap();
    let mut expected_events: Vec<(i64, String)> = (1..=6).map(|k| (k, "a".to_owned())).collect();
    expected_events.extend([(7, "b".to_owned()), (8, "b".to_owned())]);
    assert_eq!(union_events(&events_table).await, expected_events);

    upserter
        .upsert(&users(&[(2, "bob", 21), (5, "eve", 50)]))
        .await
        .unwrap();
    upserter.delete(&users(&[(4, "", 0)])).await.unwrap();
    assert_eq!(
        union_users(&users_table).await,
        BTreeMap::from([
            (1, user("ann", 11)),
            (2, user("bob", 21)),
            (5, user("eve", 50)),
        ])
    );

    let event_hw = high_watermarks(&events_table).await;
    wait_tiered(&cluster, &events_path, &event_hw).await;
    for (bucket, hw) in &event_hw {
        wait_for(&format!("log of {bucket:?} trimmed"), async || {
            let earliest = events_table
                .list_offset(*bucket, proto::OffsetSpec::Earliest)
                .await?;
            Ok((earliest > 0).then_some(earliest))
        })
        .await;
        let (start, end) = events_table.offsets(*bucket).await.unwrap();
        assert!(
            start > 0 && start <= *hw && end == *hw,
            "{bucket:?}: {start}..{end}"
        );
    }
    assert!(
        scan_all(&events_table).await.len() < 8,
        "the hot log is shorter"
    );
    assert_eq!(union_events(&events_table).await, expected_events);

    assert_eq!(
        admin
            .alter_table(
                &events_path,
                vec![Change::add_column("w", DataType::int())],
                false
            )
            .await
            .unwrap(),
        Some(SchemaId(1))
    );
    let altered = cluster.table(&events_path).await.unwrap();
    assert_eq!(altered.schema_id(), SchemaId(1));
    assert_eq!(altered.schema().fields().len(), 3);
    let mut writer = altered.append_writer().await.unwrap();
    writer
        .append(
            &RecordBatch::try_new(
                altered.arrow_schema(),
                vec![
                    Arc::new(Int64Array::from(vec![9])),
                    Arc::new(StringArray::from(vec!["c"])),
                    Arc::new(Int32Array::from(vec![Some(90)])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut widened: Vec<(i64, Option<i32>)> = Vec::new();
    for bucket in altered.buckets().collect::<Vec<_>>() {
        let (start, end) = altered.offsets(bucket).await.unwrap();
        let batches: Vec<_> = altered
            .scan(bucket, start, end, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        for b in &batches {
            assert_eq!(b.rows.num_columns(), 3);
            let k = b.rows.column(0).as_primitive::<Int64Type>();
            let w = b.rows.column(2).as_primitive::<Int32Type>();
            widened.extend(
                (0..b.rows.num_rows()).map(|i| (k.value(i), w.is_valid(i).then(|| w.value(i)))),
            );
        }
    }
    widened.sort();
    assert!(widened.contains(&(9, Some(90))));
    assert!(
        widened
            .iter()
            .filter(|(k, _)| *k < 9)
            .all(|(_, w)| w.is_none())
    );
    let event_hw = high_watermarks(&altered).await;
    wait_tiered(&cluster, &events_path, &event_hw).await;
    let mut unioned: Vec<(i64, Option<i32>)> = Vec::new();
    for bucket in altered.buckets().collect::<Vec<_>>() {
        let batches: Vec<RecordBatch> = altered
            .union(bucket, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        for b in &batches {
            assert_eq!(b.num_columns(), 3, "{:?}", b.schema());
            let k = b.column(0).as_primitive::<Int64Type>();
            let w = b.column(2).as_primitive::<Int32Type>();
            unioned
                .extend((0..b.num_rows()).map(|i| (k.value(i), w.is_valid(i).then(|| w.value(i)))));
        }
    }
    unioned.sort();
    let mut expected: Vec<(i64, Option<i32>)> = (1..=8).map(|k| (k, None)).collect();
    expected.push((9, Some(90)));
    assert_eq!(unioned, expected);

    let gone = nodes.pop().unwrap();
    let gone_id = 2;
    gone.shutdown().await;

    let after = wait_for("node 1 leads everything", async || {
        let info = admin.describe_cluster().await?;
        let survivor = info.nodes.iter().find(|n| n.node_id == 1).unwrap();
        Ok((info.unled_buckets == 0
            && survivor.leading == info.buckets
            && info.coordinator.as_ref().is_some_and(|c| c.node_id == 1))
        .then_some(info))
    })
    .await;
    assert!(
        !after
            .nodes
            .iter()
            .find(|n| n.node_id == gone_id)
            .unwrap()
            .live
    );

    let events = cluster.table(&events_path).await.unwrap();
    let mut writer = events.append_writer().await.unwrap();
    wait_for("append after failover", async || {
        Ok(Some(
            writer
                .append(
                    &RecordBatch::try_new(
                        events.arrow_schema(),
                        vec![
                            Arc::new(Int64Array::from(vec![10])),
                            Arc::new(StringArray::from(vec!["d"])),
                            Arc::new(Int32Array::from(vec![Some(100)])),
                        ],
                    )
                    .unwrap(),
                )
                .await?,
        ))
    })
    .await;
    let mut survived = Vec::new();
    for bucket in events.buckets().collect::<Vec<_>>() {
        let batches: Vec<RecordBatch> = wait_for("union after failover", async || {
            let batches: Vec<RecordBatch> = events.union(bucket, None).await?.try_collect().await?;
            Ok(Some(batches))
        })
        .await;
        for b in &batches {
            survived.extend(
                b.column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .copied(),
            );
        }
    }
    survived.sort();
    assert_eq!(survived, (1..=10).collect::<Vec<_>>());

    let users_table = cluster.table(&users_path).await.unwrap();
    let lookuper = users_table.lookuper().unwrap();
    let rows = wait_for("lookups after failover", async || {
        let found = lookuper.lookup(&user_keys(&[1, 2, 4, 5])).await?;
        Ok(Some(
            found
                .iter()
                .flatten()
                .flat_map(user_rows)
                .collect::<BTreeMap<_, _>>(),
        ))
    })
    .await;
    assert_eq!(
        rows,
        BTreeMap::from([
            (1, user("ann", 11)),
            (2, user("bob", 21)),
            (5, user("eve", 50)),
        ])
    );
    let mut upserter = users_table.upsert_writer().await.unwrap();
    upserter.upsert(&users(&[(6, "fay", 60)])).await.unwrap();
    assert_eq!(
        union_users(&users_table).await,
        BTreeMap::from([
            (1, user("ann", 11)),
            (2, user("bob", 21)),
            (5, user("eve", 50)),
            (6, user("fay", 60)),
        ])
    );

    admin.drop_table(&events_path, false).await.unwrap();
    assert!(!admin.table_exists(&events_path).await.unwrap());
    assert!(matches!(
        cluster.table(&events_path).await,
        Err(Error::Status(status)) if status.code() == Code::NotFound
    ));
    admin.drop_table(&users_path, false).await.unwrap();
    admin.drop_database("shop", false, false).await.unwrap();
    assert_eq!(admin.describe_cluster().await.unwrap().tables, 0);

    for node in nodes {
        node.shutdown().await;
    }
}
