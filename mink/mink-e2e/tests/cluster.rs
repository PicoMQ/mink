//! Three nodes: leadership spreads, writes through a non-leader get forwarded, every node
//! serves the same reads, and the coordinator tiers buckets it does not lead.

use std::collections::{BTreeMap, BTreeSet};

use futures::TryStreamExt;
use mink_client::{Cluster, proto};
use mink_e2e::lake::Lake;
use mink_e2e::rows::*;
use mink_e2e::tables::*;
use mink_e2e::{Env, fresh_database, wait_for};
use mink_table::Path;

fn path(db: &str, table: &str) -> Path {
    format!("{db}.{table}").parse().unwrap()
}

#[tokio::test]
async fn leadership_spreads_and_every_node_answers() {
    let Some(env) = Env::load() else { return };
    if !env.multi_node() {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let info = admin.describe_cluster().await.unwrap();
    assert_eq!(
        info.nodes.iter().filter(|n| n.live).count(),
        env.nodes.len()
    );
    let coordinator = info.coordinator.clone().expect("a coordinator");

    let db = fresh_database(&admin, "spread").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(6, false).build().unwrap(),
    )
    .await;

    let led = wait_for("six buckets led", async || {
        let info = admin.get_table(&table_path).await?;
        Ok(info
            .buckets
            .iter()
            .all(|b| b.leader.is_some())
            .then_some(info))
    })
    .await;
    let mut per_node: BTreeMap<i32, usize> = BTreeMap::new();
    for b in &led.buckets {
        *per_node
            .entry(b.leader.as_ref().unwrap().node_id)
            .or_default() += 1;
    }
    assert_eq!(
        per_node.len(),
        env.nodes.len(),
        "every node leads something: {per_node:?}"
    );
    assert!(
        per_node.values().max().unwrap() - per_node.values().min().unwrap() <= 1,
        "leadership balanced: {per_node:?}"
    );

    let mut writer = table.append_writer().await.unwrap();
    writer.append(&events_range(0, 600, "x")).await.unwrap();
    let expected: Vec<Event> = (0..600).map(|k| (k, format!("x-{k}"))).collect();

    for address in &env.nodes {
        let one = Cluster::connect(address).unwrap();
        let view = one.table(&table_path).await.unwrap();
        assert_eq!(scan_events(&view).await, expected, "via {address}");
        assert_eq!(union_events(&view).await, expected, "via {address}");
        let health = one.admin().health(None).await.unwrap();
        assert!(health.registered);
        assert_eq!(
            health.coordinator,
            health.node_id == coordinator.node_id,
            "{address}: {health:?}"
        );
    }

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn writes_through_a_non_leader_are_forwarded() {
    let Some(env) = Env::load() else { return };
    if !env.multi_node() {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let db = fresh_database(&admin, "forward").await;
    let table_path = path(&db, "users");
    create(
        &admin,
        &cluster,
        &table_path,
        &users_descriptor(3, false).build().unwrap(),
    )
    .await;
    let info = admin.get_table(&table_path).await.unwrap();
    let leaders: BTreeSet<String> = info
        .buckets
        .iter()
        .filter_map(|b| b.leader.as_ref().map(|l| l.address.clone()))
        .collect();
    assert!(leaders.len() > 1, "{leaders:?}");

    let mut expected = BTreeMap::new();
    for (i, address) in env.nodes.iter().enumerate() {
        let connection = cluster.connection(address).unwrap();
        let rows: Vec<(i64, String, i32)> = (0..30)
            .map(|j| (i as i64 * 1000 + j, format!("via-{i}"), i as i32))
            .collect();
        let batch = users(
            &rows
                .iter()
                .map(|(id, name, score)| (*id, name.as_str(), *score))
                .collect::<Vec<_>>(),
        );
        let write = proto::Write::PutTable {
            path: table_path.clone(),
            schema_id: mink_table::SchemaId(0),
            target_columns: None,
        };
        let acks = connection
            .put(&write, vec![(batch, proto::WriteBatch::default())])
            .await
            .unwrap_or_else(|e| panic!("table put through {address}: {e}"));
        let routed: proto::Routed = serde_json::from_slice(&acks[0]).unwrap();
        assert_eq!(routed.buckets.iter().map(|b| b.rows).sum::<usize>(), 30);
        let touched: BTreeSet<_> = routed.buckets.iter().map(|b| b.bucket).collect();
        assert!(
            touched.len() > 1,
            "rows spread over buckets on several nodes: {touched:?}"
        );
        for (id, name, score) in rows {
            expected.insert(id, (name, score));
        }
    }

    let table = cluster.table(&table_path).await.unwrap();
    let lookuper = table.lookuper().unwrap();
    for (id, want) in &expected {
        let found = lookuper.lookup_one(&user_keys(&[*id])).await.unwrap();
        let row = found.unwrap_or_else(|| panic!("row {id}"));
        assert_eq!(&user_rows(&row)[id], want);
    }
    assert_eq!(union_users(&table).await, expected);

    admin.drop_database(&db, false, true).await.unwrap();
}

#[tokio::test]
async fn coordinator_tiers_buckets_led_by_other_nodes() {
    let Some(env) = Env::load() else { return };
    if !env.multi_node() || !env.lake {
        return;
    }
    let cluster = env.ready().await;
    let admin = cluster.admin();
    let lake = Lake::connect(&env).await;
    let db = fresh_database(&admin, "tier").await;
    let table_path = path(&db, "events");
    let table = create(
        &admin,
        &cluster,
        &table_path,
        &events_descriptor(6, true).build().unwrap(),
    )
    .await;
    let info = admin.get_table(&table_path).await.unwrap();
    let leaders: BTreeSet<i32> = info
        .buckets
        .iter()
        .filter_map(|b| b.leader.as_ref().map(|l| l.node_id))
        .collect();
    assert!(leaders.len() > 1, "{leaders:?}");

    let mut writer = table.append_writer().await.unwrap();
    writer.append(&events_range(0, 3000, "t")).await.unwrap();
    let snapshot = wait_tiered_to_head(&cluster, &table).await;
    assert_eq!(
        snapshot.bucket_log_end_offset.len(),
        6,
        "every bucket in one snapshot"
    );
    assert_eq!(count(&lake.rows(&table_path).await), 3000);

    let stats: Vec<proto::NodeStats> = admin
        .cluster_stats()
        .await
        .unwrap()
        .into_iter()
        .map(|(node, stats)| stats.unwrap_or_else(|e| panic!("stats of {}: {e}", node.address)))
        .collect();
    let coordinators: Vec<&proto::NodeStats> = stats.iter().filter(|s| s.coordinator).collect();
    assert_eq!(coordinators.len(), 1);
    assert!(
        coordinators[0].tiering.iter().any(|t| t.path == table_path),
        "the coordinator schedules the table: {:?}",
        coordinators[0].tiering
    );
    assert!(
        stats
            .iter()
            .filter(|s| !s.coordinator)
            .all(|s| s.tiering.is_empty())
    );

    let bucket = table.buckets().next().unwrap();
    let (start, end) = table.offsets(bucket).await.unwrap();
    let tail: Vec<_> = table
        .scan(bucket, start, end, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!tail.is_empty());

    admin.drop_database(&db, false, true).await.unwrap();
}
