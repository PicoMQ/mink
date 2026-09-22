//! Load: `mink bench` drives concurrent appends and upserts through the cluster; afterwards every
//! read path (offsets, scan, union, the lake itself, DuckDB) agrees with what the writers acked.

use std::collections::BTreeSet;

use mink_e2e::bench::Bench;
use mink_e2e::lake::Lake;
use mink_e2e::rows::*;
use mink_e2e::tables::create;
use mink_e2e::{Env, fresh_database};
use mink_table::Path;

fn path(db: &str, table: &str) -> Path {
    format!("{db}.{table}").parse().unwrap()
}

fn ids(batches: &[arrow_array::RecordBatch]) -> Vec<i64> {
    let mut ids: Vec<i64> = batches.iter().flat_map(|b| keys(b, "id")).collect();
    ids.sort_unstable();
    ids
}

fn assert_exactly(ids: &[i64], first: i64, count: u64, what: &str) {
    assert_eq!(ids.len() as u64, count, "{what}: row count");
    let distinct: BTreeSet<i64> = ids.iter().copied().collect();
    assert_eq!(distinct.len() as u64, count, "{what}: no duplicates");
    assert_eq!(ids.first().copied(), Some(first), "{what}: first id");
    assert_eq!(
        ids.last().copied(),
        Some(first + count as i64 - 1),
        "{what}: last id"
    );
}

#[tokio::test]
async fn append_load_is_read_back_exactly_hot_and_cold() {
    let Some(env) = Env::load() else { return };
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "load").await;
    let table_path = path(&db, "events");
    let rows = 200_000u64;

    let report = Bench::new(&env.nodes[0], &table_path.to_string())
        .create(6, env.lake)
        .append(rows, 1_000, 8)
        .run()
        .await;
    assert_eq!(report.errors, 0, "no write errors");
    assert_eq!(report.rows_acked, rows, "every row acked");
    assert_eq!(report.ids.distinct, rows);

    let table = cluster.table(&table_path).await.unwrap();
    let hw: i64 = high_watermarks(&table).await.values().sum();
    assert_eq!(hw as u64, rows, "offsets add up to acked rows");

    let scanned = ids(&scan_all(&table).await);
    assert_exactly(&scanned, 0, rows, "scan");

    if env.lake {
        wait_tiered_to_head(&cluster, &table).await;
        let lake = Lake::connect(&env).await;
        let cold = ids(&lake.rows(&table_path).await);
        assert_exactly(&cold, 0, rows, "iceberg");
        assert_eq!(lake.duckdb_count(&table_path).await, Some(rows as i64));
    }

    let unioned = ids(&union_all(&table).await);
    assert_exactly(&unioned, 0, rows, "union");

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn upsert_load_converges_to_the_key_space() {
    let Some(env) = Env::load() else { return };
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "load").await;
    let table_path = path(&db, "users");
    let rows = 100_000u64;
    let key_space = 20_000u64;

    let report = Bench::new(&env.nodes[0], &table_path.to_string())
        .create(6, false)
        .upsert(rows, 500, 8, key_space)
        .run()
        .await;
    assert_eq!(report.errors, 0, "no write errors");
    assert_eq!(report.rows_acked, rows);
    assert_eq!(report.ids.distinct, key_space);

    let table = cluster.table(&table_path).await.unwrap();
    let hw: i64 = high_watermarks(&table).await.values().sum();
    let inserts = key_space;
    let updates = rows - key_space;
    assert_eq!(
        hw as u64,
        inserts + 2 * updates,
        "changelog: +I per insert, -U/+U per update"
    );

    let current = ids(&snapshot_all(&table).await);
    assert_exactly(&current, 0, key_space, "kv snapshot");

    let unioned = ids(&union_by_bucket(&table).await);
    assert_exactly(
        &unioned,
        0,
        key_space,
        "union of a pk table is its current state",
    );

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn writers_on_every_node_share_one_table() {
    let Some(env) = Env::load() else { return };
    if !env.multi_node() {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "load").await;
    let table_path = path(&db, "events");
    let per_node = 50_000u64;

    create(
        &admin,
        &cluster,
        &table_path,
        &mink_e2e::bench::descriptor(6),
    )
    .await;

    let runs = env.nodes.iter().enumerate().map(|(i, node)| {
        Bench::new(node, &table_path.to_string())
            .append(per_node, 1_000, 4)
            .arg("--start", i as u64 * per_node)
    });
    let reports = futures::future::join_all(runs.map(|b| async move { b.run().await })).await;
    let total = per_node * env.nodes.len() as u64;
    for report in &reports {
        assert_eq!(report.errors, 0);
        assert_eq!(report.rows_acked, per_node);
    }

    let table = cluster.table(&table_path).await.unwrap();
    let scanned = ids(&scan_all(&table).await);
    assert_exactly(&scanned, 0, total, "scan across writers on every node");

    admin.drop_database(&db, false, true).await.unwrap();
}
