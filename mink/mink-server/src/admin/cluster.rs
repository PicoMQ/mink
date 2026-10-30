use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

use super::Admin;

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new().route("/admin/cluster", get(cluster))
}

async fn cluster(State(admin): State<Arc<Admin>>) -> Json<Value> {
    let services = &admin.services;
    let config = &services.config;
    let roles: Vec<&str> = config.roles.iter().map(|role| role.name()).collect();

    let registry = match services.schemas.registry() {
        Some(registry) => json!({ "reachable": registry.reachable().await }),
        None => Value::Null,
    };

    let sql = match admin.sql {
        true => json!({
            "duckdbEngine": config.duckdb_engine,
            "duckdbExtensions": config.duckdb_extensions,
        }),
        false => Value::Null,
    };

    Json(json!({
        "nodeId": services.membership.id(),
        "roles": roles,
        "version": env!("CARGO_PKG_VERSION"),
        "namespace": config.namespace,
        "catalogUrl": config.catalog_endpoint,
        "s3Endpoint": config.s3_endpoint,
        "warehouse": config.warehouse,
        "registry": registry,
        "sql": sql,
    }))
}
