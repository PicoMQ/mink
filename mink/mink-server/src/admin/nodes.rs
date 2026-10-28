use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use mink_core::{Member, Role, Scope};
use mink_store::LeaseHolder;
use serde_json::{Value, json};
use uuid::Uuid;

use super::paging::{PageQuery, Paging};
use super::reply::{Failure, Params, Segments};
use super::{Admin, PROMETHEUS_TEXT};
use crate::forward::FORWARDED_BY;

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new()
        .route("/admin/nodes", get(nodes))
        .route("/admin/nodes/{id}", get(node))
        .route("/admin/nodes/{id}/metrics", get(metrics))
}

async fn nodes(
    State(admin): State<Arc<Admin>>,
    Params(page): Params<PageQuery>,
) -> Result<Json<Value>, Failure> {
    let paging = Paging::new(&page, &[])?;
    let after = paging.after::<Uuid>()?;
    let store = &admin.services.store;

    let members = store.members().list().await?;
    let leases = store.leases().page(None, None, None, i64::MAX).await?;

    let rows: Vec<&(Member, i64)> = members
        .iter()
        .filter(|(member, _)| after.is_none_or(|after| member.id() > after))
        .take(paging.wanted())
        .collect();
    let (rows, more) = paging.split(rows);
    let items = rows
        .into_iter()
        .map(|(member, expires_at_ms)| (member.id(), summary(member, *expires_at_ms, &leases)))
        .collect();
    Ok(paging.page(items, more))
}

async fn node(
    State(admin): State<Arc<Admin>>,
    Segments(id): Segments<Uuid>,
    headers: HeaderMap,
) -> Result<Response, Failure> {
    let services = &admin.services;

    if id != services.membership.id() {
        return forward(&admin, id, &format!("/admin/nodes/{id}"), &headers).await;
    }

    let members = services.store.members().list().await?;
    let leases = services
        .store
        .leases()
        .page(None, Some(id), None, i64::MAX)
        .await?;
    let (member, expires_at_ms) = members
        .iter()
        .find(|(member, _)| member.id() == id)
        .ok_or_else(|| Failure::missing(format!("no node {id}")))?;

    let mut detail = summary(member, *expires_at_ms, &leases);
    detail["settings"] = json!(services.config.describe());
    detail["cache"] = match &admin.cache {
        Some(cache) => {
            let usage = cache.usage();
            json!({
                "memoryBytes": usage.memory_bytes,
                "memoryMaxBytes": usage.memory_max_bytes,
                "diskMaxBytes": usage.disk_max_bytes,
                "building": usage.building,
            })
        }
        None => Value::Null,
    };
    Ok(Json(detail).into_response())
}

async fn metrics(
    State(admin): State<Arc<Admin>>,
    Segments(id): Segments<Uuid>,
    headers: HeaderMap,
) -> Result<Response, Failure> {
    if id != admin.services.membership.id() {
        return forward(&admin, id, &format!("/admin/nodes/{id}/metrics"), &headers).await;
    }

    let text = admin.render();
    Ok(([(header::CONTENT_TYPE, PROMETHEUS_TEXT)], text).into_response())
}

async fn forward(
    admin: &Admin,
    id: Uuid,
    path: &str,
    headers: &HeaderMap,
) -> Result<Response, Failure> {
    if headers.contains_key(FORWARDED_BY) {
        return Err(Failure::missing(format!("node {id} is not this node")));
    }

    let members = admin.services.store.members().live().await?;
    let member = members
        .iter()
        .find(|member| member.id() == id)
        .ok_or_else(|| Failure::missing(format!("no node {id}")))?;
    let address = member
        .admin_address()
        .ok_or_else(|| Failure::missing(format!("node {id} has no admin address")))?;

    let answer = admin
        .forwarder
        .forward(address, Method::GET, path, None)
        .await
        .map_err(|error| Failure::unreachable(error.to_string()))?;

    let status = answer.status();
    let content_type = answer.headers().get(header::CONTENT_TYPE).cloned();
    let body = answer
        .bytes()
        .await
        .map_err(|error| Failure::unreachable(error.to_string()))?;

    let mut response = (status, Body::from(body)).into_response();
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    Ok(response)
}

fn summary(member: &Member, expires_at_ms: i64, leases: &[LeaseHolder]) -> Value {
    let held = leases.iter().filter(|lease| lease.holder() == member.id());
    let partition_leases = held
        .clone()
        .filter(|lease| matches!(lease.scope(), Scope::Partition(_)))
        .count();
    let topic_leases = held.count() - partition_leases;
    let roles: Vec<&str> = member.roles().iter().map(Role::name).collect();

    json!({
        "id": member.id(),
        "roles": roles,
        "s3Address": member.address(),
        "adminAddress": member.admin_address(),
        "expiresAtMs": expires_at_ms,
        "partitionLeases": partition_leases,
        "topicLeases": topic_leases,
    })
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;

    use super::*;
    use crate::admin::tests::get;
    use crate::testing;

    #[tokio::test]
    async fn node_routes_are_forwarded_once() {
        let Some(config) = testing::config(&[Role::Admin, Role::S3]).await else {
            return;
        };
        let first = testing::node_with(config.clone()).await;
        let second = testing::node_with(config.clone()).await;
        let base = format!("http://{}", first.admin_address().unwrap());
        let second_id = second.services.membership.id();

        let (_, listed) = get(&base, "/admin/nodes").await;
        let (status, detail) = get(&base, &format!("/admin/nodes/{second_id}")).await;
        let metrics = reqwest::get(format!("{base}/admin/nodes/{second_id}/metrics"))
            .await
            .unwrap();
        let again = reqwest::Client::new()
            .get(format!("{base}/admin/nodes/{second_id}"))
            .header(FORWARDED_BY, Uuid::new_v4().to_string())
            .send()
            .await
            .unwrap();
        let (unknown, _) = get(&base, &format!("/admin/nodes/{}", Uuid::new_v4())).await;
        let second_text = second.services.metrics.render();
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();

        assert_eq!(listed["items"].as_array().unwrap().len(), 2);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["id"], second_id.to_string());
        assert_eq!(detail["cache"]["memoryMaxBytes"], config.cache_bytes);
        assert_eq!(detail["cache"]["diskMaxBytes"], Value::Null);
        assert_eq!(detail["cache"]["building"], 0);
        assert!(detail["settings"]["postgres"].is_string());
        assert_eq!(metrics.status(), StatusCode::OK);
        assert!(
            metrics
                .text()
                .await
                .unwrap()
                .starts_with("# TYPE mink_indexed_records_total")
        );
        assert!(
            second_text.contains(r#"mink_admin_requests_total{status="2xx"} 2"#),
            "{second_text}"
        );
        assert_eq!(again.status(), StatusCode::NOT_FOUND);
        assert_eq!(unknown, StatusCode::NOT_FOUND);
    }
}
