use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use mink_core::{Snapshot, TableLocation};
use mink_store::SnapshotTotals;
use serde_json::{Value, json};

use super::Admin;
use super::paging::{PageQuery, Paging};
use super::reply::{Failure, Params, Segments};
use super::tables::require;
use crate::services::Services;

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new()
        .route("/admin/tables/{name}/snapshots", get(snapshots))
        .route("/admin/tables/{name}/snapshots/{id}", get(snapshot))
}

async fn snapshots(
    State(admin): State<Arc<Admin>>,
    Segments(name): Segments<String>,
    Params(page): Params<PageQuery>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    let paging = Paging::new(&page, &[("table", &name)])?;
    let before = paging.after::<i64>()?;
    require(services, &name).await?;

    let snapshots = services
        .store
        .snapshots()
        .page(&name, before, paging.wanted() as i64)
        .await?;
    let (snapshots, more) = paging.split(snapshots);

    let items = items(services, &name, &snapshots).await?;
    Ok(paging.page(items, more))
}

async fn snapshot(
    State(admin): State<Arc<Admin>>,
    Segments((name, id)): Segments<(String, i64)>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    require(services, &name).await?;

    let snapshot = services
        .store
        .snapshots()
        .get(&name, id)
        .await?
        .ok_or_else(|| Failure::missing(format!("table {name} has no snapshot {id}")))?;

    let location = TableLocation::new(services.warehouse.root(), &name);
    let mut items = items(services, &name, std::slice::from_ref(&snapshot)).await?;
    let (_, mut detail) = items.remove(0);
    detail["offsets"] = snapshot
        .offsets()
        .iter()
        .map(|offsets| {
            json!({
                "partition": offsets.partition().index(),
                "startOffset": offsets.start_offset(),
                "endOffset": offsets.end_offset(),
            })
        })
        .collect();
    detail["metadataLocation"] = json!(location.metadata_file(&snapshot.key()));
    Ok(Json(detail))
}

async fn items(
    services: &Services,
    name: &str,
    snapshots: &[Snapshot],
) -> Result<Vec<(i64, Value)>, Failure> {
    let ids: Vec<i64> = snapshots.iter().map(Snapshot::id).collect();

    let totals = services.store.snapshots().totals(name, &ids).await?;

    Ok(snapshots
        .iter()
        .map(|snapshot| {
            let totals = totals
                .iter()
                .find(|(id, _)| *id == snapshot.id())
                .map(|(_, totals)| totals);

            let item = json!({
                "id": snapshot.id(),
                "timestampAtMs": snapshot.timestamp_ms(),
                "epoch": snapshot.epoch(),
                "records": totals.map(SnapshotTotals::records),
                "addedFiles": totals.map(SnapshotTotals::added_files),
                "replacedFiles": totals.map(SnapshotTotals::replaced_files),
            });
            (snapshot.id(), item)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use mink_core::{DaySketches, Scope};
    use uuid::Uuid;

    use super::*;
    use crate::Role;
    use crate::admin::tests::{admin, get};
    use crate::testing::{self, TTL, kafka, registered};
    use crate::writer::Writer;

    #[tokio::test]
    async fn snapshots_list_newest_first_with_their_offsets() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };
        let services = &node.services;
        let topic = registered(services, 1).await;
        let name = topic.name();
        mink_conformance::create_topic(&bootstrap, name, 1).await;
        let indexer = testing::partition_lease(services, &topic, 0).await;
        services
            .store
            .indexed_ranges()
            .start(&indexer, 0)
            .await
            .unwrap();
        let scope = Scope::Topic(name.to_owned());
        let lease = services
            .store
            .leases()
            .acquire(&scope, Uuid::from_u128(2), TTL)
            .await;
        let mut writer = Writer::start(services.clone(), lease.unwrap().unwrap(), name.to_owned())
            .await
            .unwrap();

        for (start, end) in [(0, 10), (10, 25)] {
            let file = mink_conformance::file(name, 0, start, end);
            let built = mink_conformance::built(&[file]);
            services
                .store
                .files()
                .add(&indexer, &built, &DaySketches::default())
                .await
                .unwrap();
            writer.tick().await.unwrap();
        }

        let path = format!("/admin/tables/{name}/snapshots");
        let (_, listed) = get(&base, &path).await;
        let (_, detail) = get(&base, &format!("{path}/2")).await;
        let (unknown, _) = get(&base, &format!("{path}/9")).await;
        node.shutdown().await.unwrap();

        let ids: Vec<i64> = listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, [2, 1]);
        assert_eq!(listed["items"][1]["records"], 10);
        assert_eq!(listed["items"][1]["addedFiles"], 1);
        assert_eq!(listed["items"][0]["addedFiles"], 1);
        assert_eq!(detail["records"], 25);
        assert_eq!(
            detail["offsets"],
            json!([{ "partition": 0, "startOffset": 0, "endOffset": 25 }])
        );
        assert!(
            detail["metadataLocation"]
                .as_str()
                .unwrap()
                .ends_with("/metadata/v2-e1.metadata.json")
        );
        assert_eq!(unknown, reqwest::StatusCode::NOT_FOUND);
    }
}
