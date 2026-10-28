use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use mink_core::{Granularity, Scope, TableLocation, TableState, Topic};
use mink_store::TableSummary;
use serde::Deserialize;
use serde_json::{Value, json};

use super::leases::lease;
use super::paging::{PageQuery, Paging};
use super::reply::{Body, Failure, Params, Segments};
use super::{Admin, lag};
use crate::Error;
use crate::registration;
use crate::services::Services;

#[derive(Deserialize)]
struct Filters {
    prefix: Option<String>,
}

#[derive(Deserialize)]
struct Added {
    topic: String,
    granularity: Option<String>,
}

#[derive(Deserialize)]
struct Changed {
    granularity: String,
}

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new()
        .route("/admin/tables", get(tables).post(add))
        .route(
            "/admin/tables/{name}",
            get(table).patch(change).delete(remove),
        )
}

pub(super) async fn require(
    services: &Services,
    name: &str,
) -> Result<(Topic, TableState), Failure> {
    services
        .store
        .topics()
        .table(name)
        .await?
        .ok_or_else(|| Failure::missing(format!("no table {name}")))
}

pub(super) async fn require_indexing(services: &Services, name: &str) -> Result<Topic, Failure> {
    match require(services, name).await? {
        (topic, TableState::Indexing) => Ok(topic),
        _ => Err(Failure::from(Error::NotIndexing(name.to_owned()))),
    }
}

async fn tables(
    State(admin): State<Arc<Admin>>,
    Params(page): Params<PageQuery>,
    Params(filters): Params<Filters>,
) -> Result<Json<Value>, Failure> {
    let prefix = filters.prefix.filter(|prefix| !prefix.is_empty());
    let paging = Paging::new(&page, &[("prefix", prefix.as_deref().unwrap_or_default())])?;
    let after = paging.after::<String>()?;
    let topics = admin.services.store.topics();

    let listed = topics
        .page(prefix.as_deref(), after.as_deref(), paging.wanted() as i64)
        .await?;
    let (listed, more) = paging.split(listed);
    let names: Vec<String> = listed
        .iter()
        .map(|(topic, _)| topic.name().to_owned())
        .collect();
    let summaries = topics.summaries(&names).await?;

    let items = listed
        .iter()
        .filter_map(|(topic, state)| {
            let summary = summaries
                .iter()
                .find(|summary| summary.name() == topic.name())?;

            Some((topic.name(), item(topic, *state, summary)))
        })
        .collect();
    Ok(paging.page(items, more))
}

async fn table(
    State(admin): State<Arc<Admin>>,
    Segments(name): Segments<String>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    let store = &services.store;
    let (topic, state) = require(services, &name).await?;

    let summary = summary(services, &name).await?;
    let location = TableLocation::new(services.warehouse.root(), &name);
    let latest = store.snapshots().latest(&name).await?;
    let specs = store.time_specs().list(&name).await?;
    let leases = store.leases().holders(&name).await?;
    let statistics_at_ms = statistics_at_ms(services, &name, &location).await?;
    let lag = match state {
        TableState::Indexing => Some(lag(services, &name).await?),
        _ => None,
    };

    let mut detail = item(&topic, state, &summary);
    detail["location"] = json!(location.path());
    detail["metadataLocation"] =
        json!(latest.map(|snapshot| { location.metadata_file(&snapshot.key()) }));
    detail["specs"] = specs
        .granularities()
        .iter()
        .enumerate()
        .map(|(id, granularity)| json!({ "id": id, "granularity": granularity.name() }))
        .collect();
    detail["writer"] = leases
        .iter()
        .find(|held| matches!(held.scope(), Scope::Topic(_)))
        .map_or(Value::Null, lease);
    detail["statisticsAtMs"] = json!(statistics_at_ms);
    detail["lag"] = json!(lag);
    Ok(Json(detail))
}

async fn add(
    State(admin): State<Arc<Admin>>,
    Body(added): Body<Added>,
) -> Result<Response, Failure> {
    let services = &admin.services;
    let granularity = match added.granularity.as_deref() {
        Some(name) => granularity(name)?,
        None => services.config.granularity,
    };

    let topic =
        registration::add(&services.store, &services.kafka, &added.topic, granularity).await?;

    let summary = summary(services, topic.name()).await?;
    let body = item(&topic, TableState::Indexing, &summary);
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

async fn change(
    State(admin): State<Arc<Admin>>,
    Segments(name): Segments<String>,
    Body(changed): Body<Changed>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    let granularity = granularity(&changed.granularity)?;

    services
        .store
        .time_specs()
        .record(&name, granularity)
        .await?;

    let (topic, state) = require(services, &name).await?;
    let summary = summary(services, &name).await?;
    Ok(Json(item(&topic, state, &summary)))
}

async fn remove(
    State(admin): State<Arc<Admin>>,
    Segments(name): Segments<String>,
) -> Result<StatusCode, Failure> {
    admin.services.store.topics().remove(&name).await?;

    Ok(StatusCode::ACCEPTED)
}

async fn statistics_at_ms(
    services: &Services,
    name: &str,
    location: &TableLocation,
) -> Result<Option<i64>, Failure> {
    let paths = services.store.objects().statistics(name).await?;

    let latest = paths
        .iter()
        .filter_map(|path| location.parse_statistics(path))
        .map(|(id, _)| id)
        .max();
    let Some(id) = latest else {
        return Ok(None);
    };

    let snapshot = services.store.snapshots().get(name, id).await?;
    Ok(snapshot.map(|snapshot| snapshot.timestamp_ms()))
}

async fn summary(services: &Services, name: &str) -> Result<TableSummary, Failure> {
    let summaries = services
        .store
        .topics()
        .summaries(&[name.to_owned()])
        .await?;

    summaries
        .into_iter()
        .next()
        .ok_or_else(|| Failure::missing(format!("no table {name}")))
}

fn granularity(name: &str) -> Result<Granularity, Failure> {
    Granularity::from_name(name).ok_or_else(|| {
        Failure::bad(format!(
            "{name:?} is not a granularity: hour, day, month or none"
        ))
    })
}

fn item(topic: &Topic, state: TableState, summary: &TableSummary) -> Value {
    json!({
        "name": topic.name(),
        "uuid": topic.uuid(),
        "partitions": topic.partition_count(),
        "granularity": summary.granularity().name(),
        "state": state.name(),
        "snapshotId": summary.snapshot_id(),
        "snapshotAtMs": summary.snapshot_at_ms(),
        "records": summary.records(),
        "bytes": summary.bytes(),
        "schemaId": summary.schema_id(),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::Method;
    use tokio::time::Instant;
    use uuid::Uuid;

    use super::*;
    use crate::Role;
    use crate::admin::tests::{admin, get, send};
    use crate::testing::{kafka, registered};

    #[tokio::test]
    async fn a_table_is_added_from_kafka_and_indexed() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let roles = [Role::Indexer, Role::Admin];
        let Some((node, base)) = admin(&roles, |config| {
            config.file_window = Duration::from_millis(200);
            config.snapshot_interval = Duration::from_millis(200);
            config.lease_ttl = Duration::from_secs(3);
            config.history = crate::History::All;
        })
        .await
        else {
            return;
        };
        let name = format!("a{}", Uuid::new_v4().simple());
        mink_conformance::create_topic(&bootstrap, &name, 2).await;
        mink_conformance::produce_values(&bootstrap, &name, 0, &["a", "b", "c"]);

        let body = json!({ "topic": name, "granularity": "day" });
        let (added, table) = send(Method::POST, &base, "/admin/tables", Some(body.clone())).await;
        let (again, _) = send(Method::POST, &base, "/admin/tables", Some(body)).await;
        let unknown = json!({ "topic": format!("u{}", Uuid::new_v4().simple()) });
        let (missing, _) = send(Method::POST, &base, "/admin/tables", Some(unknown)).await;

        let deadline = Instant::now() + Duration::from_secs(30);
        let mut detail = Value::Null;
        while (detail["records"] != 3 || detail["snapshotId"].is_null())
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
            detail = get(&base, &format!("/admin/tables/{name}")).await.1;
        }
        let (_, listed) = get(&base, &format!("/admin/tables?prefix={name}")).await;
        node.shutdown().await.unwrap();

        assert_eq!(added, StatusCode::CREATED);
        assert_eq!(table["state"], "indexing");
        assert_eq!(table["granularity"], "day");
        assert_eq!(table["partitions"], 2);
        assert_eq!(again, StatusCode::CONFLICT);
        assert_eq!(missing, StatusCode::NOT_FOUND);
        assert_eq!(detail["records"], 3, "{detail}");
        assert!(detail["snapshotId"].is_i64(), "{detail}");
        assert_eq!(detail["lag"], 0);
        assert_eq!(detail["writer"]["partition"], Value::Null);
        assert_eq!(listed["items"][0]["name"], json!(name));
        assert_eq!(listed["next"], Value::Null);
    }

    #[tokio::test]
    async fn granularity_changes_record_a_new_spec() {
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };
        let topic = registered(&node.services, 1).await;
        let path = format!("/admin/tables/{}", topic.name());

        let (changed, table) = send(
            Method::PATCH,
            &base,
            &path,
            Some(json!({ "granularity": "day" })),
        )
        .await;
        let (_, detail) = get(&base, &path).await;
        let (bad, _) = send(
            Method::PATCH,
            &base,
            &path,
            Some(json!({ "granularity": "week" })),
        )
        .await;
        let (removed, _) = send(Method::DELETE, &base, &path, None).await;
        let (_, removing) = get(&base, &path).await;
        let (late, _) = send(
            Method::PATCH,
            &base,
            &path,
            Some(json!({ "granularity": "hour" })),
        )
        .await;
        let (unknown, _) = get(&base, "/admin/tables/nothing").await;
        node.shutdown().await.unwrap();

        assert_eq!(changed, StatusCode::OK);
        assert_eq!(table["granularity"], "day");
        assert_eq!(
            detail["specs"],
            json!([{ "id": 0, "granularity": "none" }, { "id": 1, "granularity": "day" }])
        );
        assert_eq!(bad, StatusCode::BAD_REQUEST);
        assert_eq!(removed, StatusCode::ACCEPTED);
        assert_eq!(removing["state"], "removing");
        assert_eq!(late, StatusCode::CONFLICT);
        assert_eq!(unknown, StatusCode::NOT_FOUND);
    }
}
