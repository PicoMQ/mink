use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use iceberg::spec::Schema;
use mink_core::{File, TableLocation};
use mink_schema::{BINARY_SCHEMA_ID, binary_schema};
use mink_store::StoredFile;
use serde::Deserialize;
use serde_json::{Value, json};

use super::Admin;
use super::paging::{PageQuery, Paging};
use super::reply::{Failure, Params, Segments, filter};
use super::tables::require;
use super::values::bound;
use crate::services::Services;

#[derive(Deserialize)]
struct Filters {
    #[serde(default, deserialize_with = "filter")]
    partition: Option<i32>,
    #[serde(default, deserialize_with = "filter")]
    bucket: Option<i32>,
}

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new()
        .route("/admin/tables/{name}/files", get(files))
        .route("/admin/tables/{name}/files/{*file}", get(file))
}

async fn files(
    State(admin): State<Arc<Admin>>,
    Segments(name): Segments<String>,
    Params(page): Params<PageQuery>,
    Params(filters): Params<Filters>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    let root = services.warehouse.root();
    let partition = filters.partition.map(|partition| partition.to_string());
    let bucket = filters.bucket.map(|bucket| bucket.to_string());
    let paging = Paging::new(
        &page,
        &[
            ("table", &name),
            ("partition", partition.as_deref().unwrap_or_default()),
            ("bucket", bucket.as_deref().unwrap_or_default()),
        ],
    )?;
    require(services, &name).await?;

    let after = match paging.after::<String>()? {
        Some(after) => Some(
            TableLocation::parse_data_file_name(root, &name, &after)
                .ok_or_else(|| Failure::bad("the cursor is not valid"))?,
        ),
        None => None,
    };
    let stored = services
        .store
        .files()
        .page(
            &name,
            filters.partition,
            filters.bucket,
            after.as_ref(),
            paging.wanted() as i64,
        )
        .await?;

    let (stored, more) = paging.split(stored);
    let items = stored
        .iter()
        .map(|stored| {
            (
                TableLocation::data_file_name(stored.indexed().file()),
                item(stored),
            )
        })
        .collect();
    Ok(paging.page(items, more))
}

async fn file(
    State(admin): State<Arc<Admin>>,
    Segments((name, file)): Segments<(String, String)>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    let root = services.warehouse.root();
    let not_found = || Failure::missing(format!("no file {file} in table {name}"));
    require(services, &name).await?;

    let parsed = TableLocation::parse_data_file_name(root, &name, &file).ok_or_else(not_found)?;
    let stored = services
        .store
        .files()
        .find(&parsed)
        .await?
        .ok_or_else(not_found)?;
    let schema = schema(services, &parsed).await?;

    let stats = stored.indexed().stats();
    let ids: BTreeSet<i32> = stats
        .lower()
        .keys()
        .chain(stats.upper().keys())
        .chain(stats.nulls().keys())
        .chain(stats.nans().keys())
        .copied()
        .collect();
    let columns: Vec<Value> = ids
        .iter()
        .map(|id| {
            let field = schema.field_by_id(*id);
            let shown = |bytes: Option<&Vec<u8>>| match (field, bytes) {
                (Some(field), Some(bytes)) => bound(field, bytes),
                _ => Value::Null,
            };

            json!({
                "fieldId": id,
                "name": field.map(|field| field.name.clone()),
                "lower": shown(stats.lower().get(id)),
                "upper": shown(stats.upper().get(id)),
                "nulls": stats.nulls().get(id),
                "nans": stats.nans().get(id),
            })
        })
        .collect();

    let mut detail = item(&stored);
    detail["path"] = json!(TableLocation::data_file(root, &parsed));
    detail["replacedSnapshotId"] = json!(stored.replaced_snapshot_id());
    detail["partsRanges"] = stored
        .parts()
        .unwrap_or_default()
        .iter()
        .map(|range| json!({ "startOffset": range.start(), "endOffset": range.end() }))
        .collect();
    detail["columns"] = Value::Array(columns);
    Ok(Json(detail))
}

async fn schema(services: &Services, file: &File) -> Result<Schema, Failure> {
    if file.schema_id() == BINARY_SCHEMA_ID {
        return Ok(binary_schema().clone());
    }

    let topic = file.partition().topic();
    let schema = services
        .schemas
        .table_schema(topic, file.schema_id())
        .await?;
    Ok(schema)
}

fn item(stored: &StoredFile) -> Value {
    let indexed = stored.indexed();
    let file = indexed.file();
    let stats = indexed.stats();

    json!({
        "name": TableLocation::data_file_name(file),
        "partition": file.partition().index(),
        "startOffset": file.start_offset(),
        "endOffset": file.end_offset(),
        "bucket": file.time().bucket(),
        "specId": file.time().spec_id(),
        "schemaId": file.schema_id(),
        "version": file.version().number(),
        "parts": stored.parts().map_or(0, <[_]>::len),
        "records": stats.records(),
        "bytes": stats.bytes(),
        "firstTimestampAtMs": stats.first_timestamp_ms(),
        "addedSnapshotId": stored.added_snapshot_id(),
    })
}

#[cfg(test)]
mod tests {
    use mink_core::{DaySketches, IndexedFile, Partition, Stats, TimePartition};

    use super::*;
    use crate::Role;
    use crate::admin::tests::{admin, get};
    use crate::testing::{self, registered};

    fn bucketed(topic: &str, partition: i32, start: i64, end: i64, bucket: i32) -> IndexedFile {
        let partition = Partition::new(topic, partition).unwrap();
        let file = File::v1(
            partition,
            start,
            end,
            TimePartition::new(0, Some(bucket)),
            0,
        )
        .unwrap();
        let stats = Stats::new((end - start) as u64, 100, 7)
            .with_bounds(
                2,
                start.to_le_bytes().to_vec(),
                (end - 1).to_le_bytes().to_vec(),
            )
            .with_nulls(9, 1);

        IndexedFile::new(file, stats)
    }

    #[tokio::test]
    async fn files_filter_by_partition_and_bucket() {
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };
        let services = &node.services;
        let topic = registered(services, 2).await;
        let name = topic.name();
        for (partition, files) in [
            (
                0,
                vec![bucketed(name, 0, 0, 10, 5), bucketed(name, 0, 10, 20, 6)],
            ),
            (
                1,
                vec![bucketed(name, 1, 0, 4, 5), bucketed(name, 1, 0, 4, 6)],
            ),
        ] {
            let lease = testing::partition_lease(services, &topic, partition).await;
            let ranges = services.store.indexed_ranges();
            ranges.start(&lease, 0).await.unwrap();
            let built = mink_conformance::built(&files);
            services
                .store
                .files()
                .add(&lease, &built, &DaySketches::default())
                .await
                .unwrap();
        }

        let path = format!("/admin/tables/{name}/files");
        let (_, all) = get(&base, &path).await;
        let (_, second) = get(&base, &format!("{path}?partition=1&bucket=")).await;
        let (_, one) = get(&base, &format!("{path}?partition=1&limit=1")).await;
        let cursor = one["next"].as_str().unwrap().to_owned();
        let (_, two) = get(
            &base,
            &format!("{path}?partition=1&limit=1&cursor={cursor}"),
        )
        .await;
        let (bad, error) = get(&base, &format!("{path}?partition=x")).await;
        let (_, sixth) = get(&base, &format!("{path}?bucket=6")).await;
        let shown = all["items"][1]["name"].as_str().unwrap().to_owned();
        let (_, detail) = get(&base, &format!("{path}/{shown}")).await;
        let (unknown, _) = get(&base, &format!("{path}/p=0/1-2.t0.s0.v1.parquet")).await;
        node.shutdown().await.unwrap();

        let starts = |page: &Value| -> Vec<(i64, i64)> {
            page["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| {
                    (
                        item["partition"].as_i64().unwrap(),
                        item["startOffset"].as_i64().unwrap(),
                    )
                })
                .collect()
        };
        assert_eq!(starts(&all), [(0, 0), (0, 10), (1, 0), (1, 0)]);
        assert_eq!(starts(&second), [(1, 0), (1, 0)]);
        assert_eq!(one["items"][0]["bucket"], 5);
        assert_eq!(two["items"][0]["bucket"], 6);
        assert_eq!(two["next"], Value::Null);
        assert_eq!(bad, reqwest::StatusCode::BAD_REQUEST);
        assert!(error["error"].is_string());
        assert_eq!(starts(&sixth), [(0, 10), (1, 0)]);
        assert_eq!(shown, "p=0/10-20.t0_6.s0.v1.parquet");
        assert_eq!(
            detail["path"],
            format!("s3://mink/warehouse/{name}/data/{shown}")
        );
        assert_eq!(detail["records"], 10);
        assert_eq!(detail["firstTimestampAtMs"], 7);
        assert_eq!(
            detail["columns"],
            json!([
                { "fieldId": 2, "name": "_kafka_offset", "lower": "10", "upper": "19", "nulls": null, "nans": null },
                { "fieldId": 9, "name": "_kafka_value", "lower": null, "upper": null, "nulls": 1, "nans": null },
            ])
        );
        assert_eq!(unknown, reqwest::StatusCode::NOT_FOUND);
    }
}
