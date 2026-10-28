use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use mink_store::LeaseHolder;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::Admin;
use super::paging::{PageQuery, Paging};
use super::reply::{Failure, Params, filter};

#[derive(Deserialize)]
struct Filters {
    table: Option<String>,
    #[serde(default, deserialize_with = "filter")]
    node: Option<Uuid>,
}

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new().route("/admin/leases", get(leases))
}

pub(super) fn lease(lease: &LeaseHolder) -> Value {
    json!({
        "table": lease.scope().topic(),
        "partition": lease.scope().partition_index(),
        "holder": lease.holder(),
        "epoch": lease.epoch(),
        "expiresAtMs": lease.expires_at_ms(),
    })
}

async fn leases(
    State(admin): State<Arc<Admin>>,
    Params(page): Params<PageQuery>,
    Params(filters): Params<Filters>,
) -> Result<Json<Value>, Failure> {
    let table = filters.table.filter(|table| !table.is_empty());
    let node = filters
        .node
        .map(|node| node.to_string())
        .unwrap_or_default();
    let paging = Paging::new(
        &page,
        &[
            ("table", table.as_deref().unwrap_or_default()),
            ("node", &node),
        ],
    )?;
    let after = paging.after::<(String, Option<i32>)>()?;

    let leases = admin
        .services
        .store
        .leases()
        .page(
            table.as_deref(),
            filters.node,
            after
                .as_ref()
                .map(|(topic, partition)| (topic.as_str(), *partition)),
            paging.wanted() as i64,
        )
        .await?;

    let (leases, more) = paging.split(leases);
    let items = leases
        .iter()
        .map(|held| {
            let key = (
                held.scope().topic().to_owned(),
                held.scope().partition_index(),
            );

            (key, lease(held))
        })
        .collect();
    Ok(paging.page(items, more))
}
