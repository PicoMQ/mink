use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use mink_schema::{Reference, SchemaFormat};
use serde_json::{Value, json};

use super::Admin;
use super::paging::{PageQuery, Paging};
use super::reply::{Failure, Params, Segments};

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new()
        .route("/admin/registry", get(schemas))
        .route("/admin/registry/{id}", get(schema))
}

async fn schemas(
    State(admin): State<Arc<Admin>>,
    Params(page): Params<PageQuery>,
) -> Result<Json<Value>, Failure> {
    let paging = Paging::new(&page, &[])?;
    let after = paging.after::<i32>()?;

    let listed = admin
        .services
        .store
        .registry()
        .page(after, paging.wanted() as i64)
        .await?;

    let (listed, more) = paging.split(listed);
    let items = listed
        .iter()
        .map(|schema| {
            (
                schema.id(),
                item(
                    schema.id(),
                    schema.format(),
                    schema.references(),
                    schema.tables(),
                ),
            )
        })
        .collect();
    Ok(paging.page(items, more))
}

async fn schema(
    State(admin): State<Arc<Admin>>,
    Segments(id): Segments<i32>,
) -> Result<Json<Value>, Failure> {
    let registry = admin.services.store.registry();

    let schema = registry
        .get(id)
        .await?
        .ok_or_else(|| Failure::missing(format!("no registry schema {id}")))?;
    let tables = registry.tables(id).await?;

    let mut detail = item(id, schema.format(), schema.references(), &tables);
    detail["definition"] = json!(schema.definition());
    Ok(Json(detail))
}

fn item(id: i32, format: SchemaFormat, references: &[Reference], tables: &[String]) -> Value {
    let references: Vec<Value> = references
        .iter()
        .map(|reference| json!({ "name": reference.name(), "id": reference.id() }))
        .collect();

    json!({
        "id": id,
        "format": format.name(),
        "references": references,
        "tables": tables,
    })
}

#[cfg(test)]
mod tests {
    use crate::admin::tests::{get, with_registry};

    #[tokio::test]
    async fn stored_schemas_list_with_the_tables_that_use_them() {
        let Some(setup) = with_registry().await else {
            return;
        };
        let first = setup
            .version(serde_json::json!([{ "name": "a", "type": "long" }]))
            .await;
        setup.produce(&[mink_conformance::prefixed(first, &[10])]);
        setup.merge().await;

        let (_, detail) = get(&setup.base, &format!("/admin/registry/{first}")).await;
        let (unknown, _) = get(&setup.base, "/admin/registry/2147483000").await;
        let mut listed = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let path = match &cursor {
                Some(cursor) => format!("/admin/registry?limit=1000&cursor={cursor}"),
                None => "/admin/registry?limit=1000".to_owned(),
            };
            let (_, page) = get(&setup.base, &path).await;
            listed.extend(page["items"].as_array().unwrap().clone());
            cursor = page["next"].as_str().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        setup.node.shutdown().await.unwrap();

        let item = listed.iter().find(|item| item["id"] == first).unwrap();
        assert_eq!(item["format"], "avro");
        assert_eq!(item["tables"], serde_json::json!([setup.topic.name()]));
        assert!(
            detail["definition"]
                .as_str()
                .unwrap()
                .contains("\"name\":\"a\"")
        );
        assert_eq!(unknown, reqwest::StatusCode::NOT_FOUND);
    }
}
