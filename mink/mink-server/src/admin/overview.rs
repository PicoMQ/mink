use std::cmp::Reverse;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use mink_core::now_ms;
use serde_json::{Value, json};
use tokio::time::Instant;

use super::Admin;
use super::lag;
use super::reply::Failure;
use crate::runs::SharedRuns;
use crate::services::Services;

const FRESH: Duration = Duration::from_secs(60);
const RANKED: usize = 10;

pub(super) struct Computed {
    last: Mutex<Option<(Instant, Value)>>,
    runs: SharedRuns<(), Value>,
}

impl Default for Computed {
    fn default() -> Computed {
        Computed {
            last: Mutex::default(),
            runs: SharedRuns::new(),
        }
    }
}

impl Computed {
    fn fresh(&self) -> Option<Value> {
        let last = self.last.lock().expect("the overview is never poisoned");

        last.as_ref()
            .filter(|(at, _)| at.elapsed() < FRESH)
            .map(|(_, value)| value.clone())
    }

    fn keep(&self, value: Value) {
        *self.last.lock().expect("the overview is never poisoned") = Some((Instant::now(), value));
    }
}

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new().route("/admin/overview", get(overview))
}

async fn overview(State(admin): State<Arc<Admin>>) -> Result<Json<Value>, Failure> {
    if let Some(value) = admin.overview.fresh() {
        return Ok(Json(value));
    }

    let computed = admin
        .overview
        .runs
        .run((), compute(&admin.services))
        .await?;

    admin.overview.keep(computed.clone());
    Ok(Json(computed))
}

async fn compute(services: &Services) -> Result<Value, Failure> {
    let topics = services.store.topics();
    let totals = topics.totals().await?;
    let nodes = services.store.members().live().await?.len();

    let indexing: Vec<String> = topics
        .indexing()
        .await?
        .iter()
        .map(|topic| topic.name().to_owned())
        .collect();
    let mut summaries = topics.summaries(&indexing).await?;

    let mut lagging = Vec::new();
    for name in &indexing {
        match lag(services, name).await {
            Ok(lag) if lag > 0 => lagging.push((name.clone(), lag)),
            Ok(_) => {}
            Err(error) => tracing::warn!(table = %name, %error, "the lag of a table was not read"),
        }
    }
    lagging.sort_by_key(|(_, lag)| Reverse(*lag));
    lagging.truncate(RANKED);

    summaries.sort_by_key(|summary| Reverse(summary.bytes()));
    let largest: Vec<Value> = summaries
        .iter()
        .take(RANKED)
        .map(|summary| json!({ "table": summary.name(), "bytes": summary.bytes() }))
        .collect();

    summaries.sort_by_key(|summary| summary.snapshot_at_ms());
    let stalest: Vec<Value> = summaries
        .iter()
        .take(RANKED)
        .map(|summary| json!({ "table": summary.name(), "snapshotAtMs": summary.snapshot_at_ms() }))
        .collect();

    Ok(json!({
        "computedAtMs": now_ms(),
        "tables": totals.tables(),
        "nodes": nodes,
        "records": totals.records(),
        "bytes": totals.bytes(),
        "collectable": totals.collectable(),
        "mostLagging": lagging
            .iter()
            .map(|(table, lag)| json!({ "table": table, "lag": lag }))
            .collect::<Vec<_>>(),
        "largest": largest,
        "stalest": stalest,
    }))
}

#[cfg(test)]
mod tests {
    use mink_core::{DaySketches, Granularity, Topic};

    use crate::Role;
    use crate::admin::tests::{admin, get};
    use crate::testing;

    #[tokio::test]
    async fn the_overview_counts_and_ranks_tables() {
        let Some(bootstrap) = testing::kafka() else {
            return;
        };
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };
        let services = &node.services;
        let base_name = format!("o{}", uuid::Uuid::new_v4().simple());
        let small = Topic::new(&base_name, uuid::Uuid::new_v4(), 1).unwrap();
        let large = Topic::new(format!("{base_name}-dlq"), uuid::Uuid::new_v4(), 1).unwrap();
        for topic in [&small, &large] {
            services
                .store
                .topics()
                .add(topic, Granularity::None)
                .await
                .unwrap();
        }
        for (topic, end) in [(&small, 2), (&large, 9)] {
            mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
            let lease = testing::partition_lease(services, topic, 0).await;
            services
                .store
                .indexed_ranges()
                .start(&lease, 0)
                .await
                .unwrap();
            let built = mink_conformance::built(&[mink_conformance::file(topic.name(), 0, 0, end)]);
            services
                .store
                .files()
                .add(&lease, &built, &DaySketches::default())
                .await
                .unwrap();
        }

        let (_, overview) = get(&base, "/admin/overview").await;
        let (_, cluster) = get(&base, "/admin/cluster").await;
        let (_, leases) = get(
            &base,
            &format!("/admin/leases?table={}&node=", large.name()),
        )
        .await;
        let (_, first) = get(&base, "/admin/leases?limit=1").await;
        let cursor = first["next"].as_str().unwrap().to_owned();
        let (_, second) = get(&base, &format!("/admin/leases?limit=1&cursor={cursor}")).await;
        node.shutdown().await.unwrap();

        assert_eq!(overview["tables"], 2);
        assert_eq!(overview["nodes"], 1);
        assert_eq!(overview["records"], 11);
        assert_eq!(overview["largest"][0]["table"], large.name());
        assert_eq!(
            overview["stalest"][0]["snapshotAtMs"],
            serde_json::Value::Null
        );
        assert_eq!(cluster["catalogUrl"], "http://127.0.0.1:8181");
        assert_eq!(cluster["registry"], serde_json::Value::Null);
        assert_eq!(leases["items"].as_array().unwrap().len(), 1);
        assert_eq!(leases["items"][0]["partition"], 0);
        let paged = [&first["items"][0]["table"], &second["items"][0]["table"]];
        assert_eq!(paged, [small.name(), large.name()]);
        assert_eq!(second["next"], serde_json::Value::Null);
    }
}
