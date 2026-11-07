use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use mink_core::{CatalogTable, ExternalTable, TierSchedule};
use serde::Deserialize;
use serde_json::{Value, json};

use super::Admin;
use super::reply::{Body, Failure, Segments};
use crate::Error;
use crate::config::{format_duration, parse_duration};
use crate::external::{self, Attachment, TableSource};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttachRequest {
    catalog: String,
    namespace: String,
    name: String,
    source: TableSource,
    tier_after: Option<String>,
    tier_interval: Option<String>,
}

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new().route("/admin/tables/{name}/external", post(attach).delete(detach))
}

pub(super) fn external_json(external: &ExternalTable) -> Value {
    let table = external.table();

    json!({
        "catalog": table.catalog(),
        "namespace": table.namespace(),
        "name": table.name(),
        "uuid": external.uuid(),
        "location": external.location().path(),
        "generation": external.generation(),
        "tierAfter": format_duration(external.schedule().after()),
        "tierInterval": format_duration(external.schedule().interval()),
    })
}

async fn attach(
    State(admin): State<Arc<Admin>>,
    Segments(topic): Segments<String>,
    Body(request): Body<AttachRequest>,
) -> Result<Response, Failure> {
    let services = &admin.services;
    let attachment = attachment(topic, request)?;
    let catalog = services.external_catalog(attachment.table.catalog())?;

    let external = external::attach(
        &services.store,
        &services.kafka,
        &catalog,
        &services.warehouse,
        attachment,
    )
    .await?;

    Ok((StatusCode::CREATED, Json(external_json(&external))).into_response())
}

async fn detach(
    State(admin): State<Arc<Admin>>,
    Segments(topic): Segments<String>,
) -> Result<StatusCode, Failure> {
    admin
        .services
        .store
        .external_tables()
        .detach(&topic)
        .await?;

    Ok(StatusCode::NO_CONTENT)
}

fn attachment(topic: String, request: AttachRequest) -> Result<Attachment, Failure> {
    let duration = |text: Option<String>| {
        text.map(|text| parse_duration(&text))
            .transpose()
            .map_err(Error::from)
    };
    let schedule = TierSchedule::given(
        duration(request.tier_after)?,
        duration(request.tier_interval)?,
    )
    .map_err(Error::from)?;

    Ok(Attachment {
        topic,
        table: CatalogTable::new(&request.catalog, &request.namespace, &request.name),
        schedule,
        source: request.source,
    })
}

#[cfg(test)]
mod tests {
    use reqwest::Method;

    use super::*;
    use crate::Role;
    use crate::admin::tests::{admin, send};
    use crate::testing::registered;

    #[tokio::test]
    async fn a_source_names_its_kind_and_takes_only_its_options() {
        let Some((node, base)) = admin(&[Role::Admin], |_| {}).await else {
            return;
        };
        let topic = registered(&node.services, 1).await;
        let path = format!("/admin/tables/{}/external", topic.name());
        let body = |extra: Value| {
            let mut body = json!({ "catalog": "rest", "namespace": "kafka", "name": "orders" });
            body.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            Some(body)
        };

        let (unnamed, _) = send(Method::POST, &base, &path, body(json!({}))).await;
        let (unpartitioned, _) = send(
            Method::POST,
            &base,
            &path,
            body(json!({ "source": { "new": {} } })),
        )
        .await;
        let (existing, _) = send(
            Method::POST,
            &base,
            &path,
            body(json!({ "source": { "existing": { "partitioning": "unpartitioned" } } })),
        )
        .await;
        let (unreadable, _) = send(
            Method::POST,
            &base,
            &path,
            body(json!({
                "source": { "new": { "partitioning": { "fields": ["decade(_kafka_timestamp)"] } } }
            })),
        )
        .await;
        let (detached, _) = send(Method::DELETE, &base, &path, None).await;
        node.shutdown().await.unwrap();

        assert_eq!(unnamed, StatusCode::BAD_REQUEST);
        assert_eq!(unpartitioned, StatusCode::BAD_REQUEST);
        assert_eq!(existing, StatusCode::BAD_REQUEST);
        assert_eq!(unreadable, StatusCode::BAD_REQUEST);
        assert_eq!(detached, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_node_with_only_the_admin_role_attaches_and_detaches() {
        let (Some(bootstrap), Some(endpoint), Some(uri)) = (
            crate::testing::kafka(),
            mink_conformance::service("TEST_S3_ENDPOINT"),
            mink_conformance::service("TEST_ICEBERG_REST"),
        ) else {
            return;
        };
        let Some((node, base)) = admin(&[Role::Admin], |config| {
            config.storage = crate::testing::storage(endpoint);
            config.external_catalogs = std::collections::BTreeMap::from([(
                "fixture".to_owned(),
                crate::config::ExternalCatalog {
                    uri: uri.clone(),
                    warehouse: None,
                    auth: None,
                },
            )]);
        })
        .await
        else {
            return;
        };
        let topic = registered(&node.services, 1).await;
        mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
        reqwest::Client::new()
            .post(format!("{uri}/v1/namespaces"))
            .json(&json!({ "namespace": ["mink"] }))
            .send()
            .await
            .unwrap();
        let path = format!("/admin/tables/{}/external", topic.name());
        let body = json!({
            "catalog": "fixture",
            "namespace": "mink",
            "name": topic.name(),
            "source": { "new": { "partitioning": "unpartitioned" } },
            "tierAfter": "2h",
        });
        let unknown = json!({
            "catalog": "nowhere",
            "namespace": "mink",
            "name": "t",
            "source": { "new": { "partitioning": "unpartitioned" } },
        });

        let (attached, external) = send(Method::POST, &base, &path, Some(body.clone())).await;
        let (_, detail) =
            crate::admin::tests::get(&base, &format!("/admin/tables/{}", topic.name())).await;
        let (again, _) = send(Method::POST, &base, &path, Some(body)).await;
        let (uncatalogued, _) = send(Method::POST, &base, &path, Some(unknown)).await;
        let (detached, _) = send(Method::DELETE, &base, &path, None).await;
        node.shutdown().await.unwrap();

        assert_eq!(attached, StatusCode::CREATED, "{external}");
        assert_eq!(external["generation"], 1);
        assert_eq!(external["tierAfter"], "2h");
        assert_eq!(external["tierInterval"], "1h");
        assert_eq!(detail["external"]["name"], json!(topic.name()));
        assert_eq!(again, StatusCode::CONFLICT);
        assert_eq!(uncatalogued, StatusCode::BAD_REQUEST);
        assert_eq!(detached, StatusCode::NO_CONTENT);
    }
}
