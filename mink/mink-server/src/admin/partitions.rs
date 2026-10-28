use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use mink_core::{Offsets, Partition, Scope};
use serde_json::{Value, json};

use super::Admin;
use super::leases::lease;
use super::paging::{PageQuery, Paging};
use super::reply::{Failure, Params, Segments};
use super::tables::require_indexing;
use crate::Error;
use crate::indexer::backfill;

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new().route("/admin/tables/{name}/partitions", get(partitions))
}

async fn partitions(
    State(admin): State<Arc<Admin>>,
    Segments(name): Segments<String>,
    Params(page): Params<PageQuery>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    let store = &services.store;
    let paging = Paging::new(&page, &[("table", &name)])?;
    let after = paging.after::<i32>()?;
    let topic = require_indexing(services, &name).await?;

    let first = after.map_or(0, |after| after + 1);
    let indices: Vec<i32> = (first..topic.partition_count())
        .take(paging.wanted())
        .collect();
    let (indices, more) = paging.split(indices);
    let ranges = store.indexed_ranges().list(&name).await?;
    let leases = store.leases().holders(&name).await?;
    let mut cluster = services.kafka.cluster().await?;

    let mut items = Vec::with_capacity(indices.len());
    for index in indices {
        let partition = Partition::new(&name, index).map_err(Error::from)?;
        let range = ranges.iter().find(|range| *range.partition() == partition);
        let earliest = cluster.earliest(&partition).await?;
        let stable = cluster.last_stable_offset(&partition).await?;

        let history = &services.config.history;
        let target = match range {
            Some(range) => {
                backfill::target(&mut cluster, history, &partition, range, earliest).await?
            }
            None => None,
        };
        let remaining = match (range, target) {
            (Some(range), Some(target)) => (range.start_offset() - target.max(earliest)).max(0),
            _ => 0,
        };
        let held = leases
            .iter()
            .find(|held| matches!(held.scope(), Scope::Partition(leased) if *leased == partition));

        let item = json!({
            "index": index,
            "earliestOffset": earliest,
            "lastStableOffset": stable,
            "startOffset": range.map(Offsets::start_offset),
            "endOffset": range.map(Offsets::end_offset),
            "lag": range.map(|range| (stable - range.end_offset()).max(0)),
            "backfillRemaining": remaining,
            "lease": held.map_or(Value::Null, lease),
        });
        items.push((index, item));
    }

    Ok(paging.page(items, more))
}

#[cfg(test)]
mod tests {
    use crate::Role;
    use crate::admin::tests::{admin, get};
    use crate::testing::{self, kafka, registered};

    #[tokio::test]
    async fn partitions_show_kafka_offsets_beside_the_indexed_range() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let Some((node, base)) = admin(&[Role::Admin], |config| {
            config.history = crate::History::All
        })
        .await
        else {
            return;
        };
        let services = &node.services;
        let topic = registered(services, 2).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 2).await;
        mink_conformance::produce_values(&bootstrap, topic.name(), 0, &["a", "b", "c", "d", "e"]);
        let lease = testing::partition_lease(services, &topic, 0).await;
        services
            .store
            .indexed_ranges()
            .start(&lease, 2)
            .await
            .unwrap();

        let path = format!("/admin/tables/{}/partitions", topic.name());
        let (_, first) = get(&base, &format!("{path}?limit=1")).await;
        let cursor = first["next"].as_str().unwrap().to_owned();
        let (_, second) = get(&base, &format!("{path}?limit=1&cursor={cursor}")).await;
        node.shutdown().await.unwrap();

        let indexed = &first["items"][0];
        assert_eq!(indexed["index"], 0);
        assert_eq!(indexed["earliestOffset"], 0);
        assert_eq!(indexed["lastStableOffset"], 5);
        assert_eq!(indexed["startOffset"], 2);
        assert_eq!(indexed["endOffset"], 2);
        assert_eq!(indexed["lag"], 3);
        assert_eq!(indexed["backfillRemaining"], 2);
        assert_eq!(
            indexed["lease"]["holder"],
            uuid::Uuid::from_u128(1).to_string()
        );
        let unstarted = &second["items"][0];
        assert_eq!(unstarted["index"], 1);
        assert_eq!(unstarted["startOffset"], serde_json::Value::Null);
        assert_eq!(unstarted["lease"], serde_json::Value::Null);
        assert_eq!(second["next"], serde_json::Value::Null);
    }
}
