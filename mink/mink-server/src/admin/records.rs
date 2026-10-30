use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use mink_core::{Offsets, Partition};
use mink_iceberg::{FileSchema, decode};
use serde::Deserialize;
use serde_json::{Value, json};

use super::Admin;
use super::reply::{Failure, Params, Segments, filter};
use super::tables::require_indexing;
use super::values::{bytes, headers, row};
use crate::Error;

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;
const SCANNED_PER_RECORD: i64 = 10;

#[derive(Deserialize)]
struct Start {
    #[serde(default, deserialize_with = "filter")]
    offset: Option<i64>,
    #[serde(default, deserialize_with = "filter")]
    timestamp: Option<i64>,
    #[serde(default, deserialize_with = "filter")]
    limit: Option<i64>,
}

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new().route(
        "/admin/tables/{name}/partitions/{index}/records",
        get(records),
    )
}

async fn records(
    State(admin): State<Arc<Admin>>,
    Segments((name, index)): Segments<(String, i32)>,
    Params(start): Params<Start>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    let limit = start.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(Failure::bad(format!("limit must be 1 to {MAX_LIMIT}")));
    }

    let topic = require_indexing(services, &name).await?;
    if !(0..topic.partition_count()).contains(&index) {
        return Err(Failure::missing(format!(
            "table {name} has no partition {index}"
        )));
    }
    let partition = Partition::new(&name, index).map_err(Error::from)?;

    let mut cluster = services.kafka.cluster().await?;
    let earliest = cluster.earliest(&partition).await?;
    let stable = cluster.last_stable_offset(&partition).await?;
    let first = match (start.offset, start.timestamp) {
        (Some(offset), _) => offset,
        (None, Some(timestamp)) => cluster
            .offset_at(&partition, timestamp)
            .await?
            .unwrap_or(stable),
        (None, None) => match services.store.indexed_ranges().get(&partition).await? {
            Some(range) => range.start_offset(),
            None => earliest,
        },
    };

    let first = first.clamp(earliest, stable);
    let scanned = first
        .saturating_add(limit.saturating_mul(SCANNED_PER_RECORD))
        .min(stable);
    let mut records = Vec::new();
    let mut end = first;
    while end < scanned && (records.len() as i64) < limit {
        let wanted = limit - records.len() as i64;
        let next = end.saturating_add(wanted).min(scanned);
        let read = cluster
            .read(&Offsets::new(partition.clone(), end, next).map_err(Error::from)?)
            .await?;

        records.extend(read);
        end = next;
    }
    drop(cluster);

    let latest = services.store.schemas().latest(&name).await?;
    let schema = services
        .schemas
        .file_schema(&name, latest.schema_id())
        .await?
        .unwrap_or(FileSchema::Binary);

    let items: Vec<Value> = records
        .iter()
        .map(|record| {
            let mut item = json!({
                "offset": record.offset(),
                "timestampAtMs": record.timestamp_ms(),
                "key": bytes(record.key().map(AsRef::as_ref)),
                "value": bytes(record.value().map(AsRef::as_ref)),
                "headers": headers(record),
            });

            match decode(&schema, record) {
                Ok(decoded) => item["row"] = row(schema.schema(), index, record, &decoded),
                Err(error) => {
                    item["row"] = Value::Null;
                    item["rowError"] = json!(error.to_string());
                }
            }
            item
        })
        .collect();

    let next = (end < stable).then_some(end);
    Ok(Json(json!({ "items": items, "next": next })))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::Role;
    use crate::admin::tests::{admin, get, with_registry};
    use crate::testing::{kafka, registered};

    #[tokio::test]
    async fn aborted_records_do_not_shorten_a_page() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };
        let topic = registered(&node.services, 1).await;
        let name = topic.name();
        mink_conformance::create_topic(&bootstrap, name, 1).await;
        mink_conformance::produce_values(&bootstrap, name, 0, &["a", "b"]);
        mink_conformance::produce_aborted(&bootstrap, name, 0, &["x", "y", "z"]);
        let partition = Partition::new(name, 0).unwrap();
        let mut cluster = node.services.kafka.cluster().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while cluster.last_stable_offset(&partition).await.unwrap() < 6 && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            Instant::now() < deadline,
            "the abort marker was never written"
        );
        drop(cluster);
        mink_conformance::produce_values(&bootstrap, name, 0, &["c", "d"]);

        let path = format!("/admin/tables/{name}/partitions/0/records");
        let (_, page) = get(&base, &format!("{path}?offset=0&limit=4&timestamp=")).await;
        let (removed, _) = get(&base, "/admin/tables/nothing/partitions/0/records").await;
        node.shutdown().await.unwrap();

        let values: Vec<&str> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["value"]["data"].as_str().unwrap())
            .collect();
        assert_eq!(values, ["a", "b", "c", "d"]);
        assert_eq!(page["next"], Value::Null);
        assert_eq!(removed, reqwest::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn records_show_raw_bytes_and_the_decoded_row() {
        let Some(setup) = with_registry().await else {
            return;
        };
        let a = json!({ "name": "a", "type": "long" });
        let b = json!({ "name": "b", "type": ["null", "string"], "default": null });
        let c = json!({ "name": "c", "type": ["null", "int"], "default": null });
        let first = setup.version(json!([a])).await;
        let second = setup.version(json!([a, b])).await;
        setup.produce(&[
            mink_conformance::prefixed(first, &[10]),
            mink_conformance::prefixed(second, &[12, 2, 2, b'x']),
        ]);
        setup.merge().await;
        let third = setup.version(json!([a, b, c])).await;
        setup.produce(&[mink_conformance::prefixed(third, &[14, 0, 0])]);

        let path = format!("/admin/tables/{}/partitions/0/records", setup.topic.name());
        let (_, page) = get(&setup.base, &format!("{path}?offset=0&limit=2")).await;
        let (_, rest) = get(&setup.base, &format!("{path}?offset=2")).await;
        let (bad, _) = get(&setup.base, &format!("{path}?limit=501")).await;
        setup.node.shutdown().await.unwrap();

        let first_row = &page["items"][0]["row"];
        assert_eq!(first_row["a"], "5");
        assert_eq!(first_row["b"], Value::Null);
        assert_eq!(first_row["_kafka_offset"], "0");
        assert_eq!(page["items"][1]["row"]["b"], "x");
        assert_eq!(page["items"][0]["value"]["bytes"], 6);
        assert_eq!(page["next"], 2);
        let unmerged = &rest["items"][0];
        assert_eq!(unmerged["offset"], 2);
        assert_eq!(unmerged["row"], Value::Null);
        assert!(
            unmerged["rowError"]
                .as_str()
                .unwrap()
                .contains("not merged"),
            "{unmerged}"
        );
        assert_eq!(rest["next"], Value::Null);
        assert_eq!(bad, reqwest::StatusCode::BAD_REQUEST);
    }
}
