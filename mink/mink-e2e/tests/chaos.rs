//! Chaos on the three node stack: containers are SIGKILLed or restarted while `mink bench`
//! writes; afterwards every acked row is present exactly once and the cluster is whole again.

use std::collections::BTreeSet;
use std::time::Duration;

use arrow_array::RecordBatch;
use mink_client::Cluster;
use mink_e2e::bench::{self, Bench};
use mink_e2e::lake::Lake;
use mink_e2e::rows::*;
use mink_e2e::tables::create;
use mink_e2e::{Env, docker, fresh_database, wait_for};
use mink_table::Path;

fn path(db: &str, table: &str) -> Path {
    format!("{db}.{table}").parse().unwrap()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids: Vec<i64> = batches.iter().flat_map(|b| keys(b, "id")).collect();
    ids.sort_unstable();
    ids
}

fn assert_exactly_once(ids: &[i64], acked: u64, what: &str) {
    let distinct: BTreeSet<i64> = ids.iter().copied().collect();
    assert_eq!(
        ids.len() as u64,
        acked,
        "{what}: every acked row is readable"
    );
    assert_eq!(distinct.len(), ids.len(), "{what}: no row twice");
}

async fn coordinator_id(cluster: &Cluster) -> i32 {
    let admin = cluster.admin();
    wait_for("a coordinator", async || {
        Ok(admin
            .describe_cluster()
            .await?
            .coordinator
            .map(|c| c.node_id))
    })
    .await
}

async fn wait_coordinator_other_than(cluster: &Cluster, old: i32) -> i32 {
    let admin = cluster.admin();
    wait_for(
        &format!("a coordinator other than node {old}"),
        async || {
            Ok(admin
                .describe_cluster()
                .await?
                .coordinator
                .map(|c| c.node_id)
                .filter(|id| *id != old))
        },
    )
    .await
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

async fn wait_live(cluster: &Cluster, nodes: usize) {
    let admin = cluster.admin();
    wait_for(&format!("{nodes} live nodes"), async || {
        let info = admin.describe_cluster().await?;
        Ok((info.nodes.iter().filter(|n| n.live).count() >= nodes).then_some(()))
    })
    .await
}

fn bootstrap(env: &Env) -> String {
    env.nodes.join(",")
}

#[tokio::test]
async fn killing_a_leader_loses_no_acked_row() {
    let Some(env) = Env::load() else { return };
    if !env.multi_node() {
        return;
    }
    assert!(docker::available().await, "docker socket in the runner");
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "chaos").await;
    let table_path = path(&db, "events");
    create(&admin, &cluster, &table_path, &bench::descriptor(6)).await;

    let coordinator = coordinator_id(&cluster).await;
    let victim = admin
        .get_table(&table_path)
        .await
        .unwrap()
        .buckets
        .iter()
        .filter_map(|b| b.leader.as_ref().map(|l| l.node_id))
        .find(|id| *id != coordinator)
        .expect("a leader that is not the coordinator");

    let load = Bench::new(&bootstrap(&env), &table_path.to_string())
        .append(0, 500, 6)
        .sustained("40s", 2, "90s")
        .spawn();

    tokio::time::sleep(Duration::from_secs(5)).await;
    docker::kill(&docker::node(victim)).await;
    docker::wait_stopped(&docker::node(victim)).await;
    wait_all_led(&cluster, &table_path, Some(victim)).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    docker::start(&docker::node(victim)).await;
    docker::wait_healthy(&docker::node(victim)).await;
    wait_live(&cluster, env.nodes.len()).await;

    let report = load.await.unwrap();
    assert_eq!(report.errors, 0, "writers rode through the failover");
    assert!(report.rows_acked > 0);

    let table = cluster.table(&table_path).await.unwrap();
    let scanned = ids(&scan_all(&table).await);
    assert_exactly_once(&scanned, report.rows_acked, "scan after leader kill");

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn killing_the_coordinator_hands_over_and_ddl_keeps_working() {
    let Some(env) = Env::load() else { return };
    if !env.multi_node() {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "chaos").await;
    let table_path = path(&db, "events");
    create(&admin, &cluster, &table_path, &bench::descriptor(6)).await;

    let old = coordinator_id(&cluster).await;
    let load = Bench::new(&bootstrap(&env), &table_path.to_string())
        .append(0, 500, 6)
        .sustained("40s", 2, "90s")
        .spawn();

    tokio::time::sleep(Duration::from_secs(5)).await;
    docker::kill(&docker::node(old)).await;
    docker::wait_stopped(&docker::node(old)).await;
    let new = wait_coordinator_other_than(&cluster, old).await;
    assert_ne!(new, old);

    let during = path(&db, "during_outage");
    create(&admin, &cluster, &during, &bench::descriptor(3)).await;
    wait_all_led(&cluster, &table_path, Some(old)).await;

    docker::start(&docker::node(old)).await;
    docker::wait_healthy(&docker::node(old)).await;
    wait_live(&cluster, env.nodes.len()).await;

    let report = load.await.unwrap();
    assert_eq!(report.errors, 0);
    assert!(report.rows_acked > 0);
    let table = cluster.table(&table_path).await.unwrap();
    let scanned = ids(&scan_all(&table).await);
    assert_exactly_once(&scanned, report.rows_acked, "scan after coordinator kill");

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn restarting_postgres_and_object_storage_under_load() {
    let Some(env) = Env::load() else { return };
    if !env.multi_node() || !env.lake {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "chaos").await;
    let table_path = path(&db, "events");
    create(&admin, &cluster, &table_path, &bench::descriptor(6)).await;

    let load = Bench::new(&bootstrap(&env), &table_path.to_string())
        .append(0, 500, 6)
        .sustained("45s", 2, "90s")
        .spawn();

    tokio::time::sleep(Duration::from_secs(5)).await;
    docker::restart(docker::POSTGRES).await;
    docker::wait_healthy(docker::POSTGRES).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    docker::restart(docker::RUSTFS).await;
    docker::wait_healthy(docker::RUSTFS).await;

    let report = load.await.unwrap();
    for node in 1..=env.nodes.len() as i32 {
        let status = docker::status(&docker::node(node)).await;
        assert_eq!(status, "running healthy", "node {node} survived: {status}");
    }
    wait_live(&cluster, env.nodes.len()).await;
    assert_eq!(report.errors, 0, "writers rode through the restarts");
    assert!(report.rows_acked > 0);

    let table = cluster.table(&table_path).await.unwrap();
    let scanned = ids(&scan_all(&table).await);
    assert_exactly_once(&scanned, report.rows_acked, "scan after infra restarts");

    let after = path(&db, "after");
    create(&admin, &cluster, &after, &bench::descriptor(3)).await;

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn killing_the_tiering_node_mid_commit_tiers_exactly_once() {
    let Some(env) = Env::load() else { return };
    if !env.multi_node() || !env.lake {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "chaos").await;
    let table_path = path(&db, "events");
    Bench::new(&bootstrap(&env), &table_path.to_string())
        .create(6, true)
        .append(1_000, 1_000, 1)
        .run()
        .await;
    let table = cluster.table(&table_path).await.unwrap();
    wait_tiered_to_head(&cluster, &table).await;

    let load = Bench::new(&bootstrap(&env), &table_path.to_string())
        .append(0, 1_000, 6)
        .arg("--start", 1_000)
        .arg("--payload", 512)
        .sustained("30s", 0, "90s")
        .spawn();

    let tierer = coordinator_id(&cluster).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    let before = lake_snapshot(&cluster, &table_path)
        .await
        .expect("rounds ran");
    docker::kill(&docker::node(tierer)).await;
    docker::wait_stopped(&docker::node(tierer)).await;
    wait_coordinator_other_than(&cluster, tierer).await;
    docker::start(&docker::node(tierer)).await;
    docker::wait_healthy(&docker::node(tierer)).await;
    wait_live(&cluster, env.nodes.len()).await;

    let report = load.await.unwrap();
    assert_eq!(report.errors, 0);
    let rows = 1_000 + report.rows_acked;
    let after = wait_tiered_to_head(&cluster, &table).await;
    assert_ne!(
        after.snapshot_id, before.snapshot_id,
        "the new coordinator kept tiering"
    );
    let lake = Lake::connect(&env).await;
    let cold = ids(&lake.rows(&table_path).await);
    assert_exactly_once(&cold, rows, "iceberg after the tiering node died");
    assert_eq!(lake.duckdb_count(&table_path).await, Some(rows as i64));
    let unioned = ids(&union_all(&table).await);
    assert_exactly_once(&unioned, rows, "union after the tiering node died");

    admin.drop_database(&db, false, true).await.unwrap();
}
