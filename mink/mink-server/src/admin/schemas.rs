use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use iceberg::spec::{NestedFieldRef, Schema, Type};
use mink_schema::{MergeResult, binary_schema};
use serde_json::{Value, json};

use super::Admin;
use super::reply::{Failure, Segments};
use super::tables::require;

pub(super) fn routes() -> Router<Arc<Admin>> {
    Router::new().route("/admin/tables/{name}/schemas", get(schemas))
}

async fn schemas(
    State(admin): State<Arc<Admin>>,
    Segments(name): Segments<String>,
) -> Result<Json<Value>, Failure> {
    let services = &admin.services;
    let store = services.store.schemas();
    require(services, &name).await?;

    let typed = store.table_schemas(&name).await?;
    let merged = store.topic_schemas(&name).await?;

    let mut schemas = vec![binary_schema().clone()];
    schemas.extend(typed);
    let current = schemas
        .iter()
        .map(Schema::schema_id)
        .max()
        .unwrap_or_default();

    let references: Vec<Value> = merged
        .iter()
        .map(|merged| {
            let schema_ref = merged.schema_ref();
            let (registry_key, schema_id, rejection) = match merged.result() {
                MergeResult::Merged {
                    registry_key,
                    table_schema_id,
                    ..
                } => (Some(registry_key.to_string()), Some(*table_schema_id), None),
                MergeResult::Rejected(rejection) => (None, None, Some(rejection.name())),
            };

            json!({
                "part": schema_ref.part().name(),
                "reference": schema_ref.schema_ref().to_string(),
                "registryKey": registry_key,
                "schemaId": schema_id,
                "rejection": rejection,
            })
        })
        .collect();

    Ok(Json(json!({
        "current": current,
        "schemas": schemas
            .iter()
            .map(|schema| json!({ "id": schema.schema_id(), "fields": fields(schema.as_struct().fields()) }))
            .collect::<Vec<_>>(),
        "references": references,
    })))
}

fn fields(fields: &[NestedFieldRef]) -> Value {
    fields.iter().map(field).collect()
}

fn field(field: &NestedFieldRef) -> Value {
    let (kind, nested) = match field.field_type.as_ref() {
        Type::Primitive(primitive) => (primitive.to_string(), None),
        Type::Struct(fields) => ("struct".to_owned(), Some(self::fields(fields.fields()))),
        Type::List(list) => (
            "list".to_owned(),
            Some(json!([self::field(&list.element_field)])),
        ),
        Type::Map(map) => (
            "map".to_owned(),
            Some(json!([
                self::field(&map.key_field),
                self::field(&map.value_field)
            ])),
        ),
    };

    let mut shown = json!({
        "id": field.id,
        "name": field.name,
        "type": kind,
        "required": field.required,
    });
    if let Some(nested) = nested {
        shown["fields"] = nested;
    }
    shown
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::admin::tests::{get, with_registry};

    #[tokio::test]
    async fn references_show_merges_and_rejections() {
        let Some(setup) = with_registry().await else {
            return;
        };
        let first = setup
            .version(json!([{ "name": "a", "type": "long" }]))
            .await;
        setup.produce(&[
            mink_conformance::prefixed(first, &[10]),
            mink_conformance::prefixed(i32::MAX - 7, &[10]),
        ]);
        setup.merge().await;

        let (_, schemas) = get(
            &setup.base,
            &format!("/admin/tables/{}/schemas", setup.topic.name()),
        )
        .await;
        setup.node.shutdown().await.unwrap();

        assert_eq!(schemas["current"], 1);
        assert_eq!(schemas["schemas"][0]["id"], 0);
        let typed = schemas["schemas"][1]["fields"].as_array().unwrap();
        let field = typed.iter().find(|field| field["name"] == "a").unwrap();
        assert_eq!(field["type"], "long");
        assert!(field["id"].as_i64().unwrap() > 9);
        let references = schemas["references"].as_array().unwrap();
        let merged = references
            .iter()
            .find(|reference| reference["reference"] == format!("confluent-id:{first}"))
            .unwrap();
        let rejected = references
            .iter()
            .find(|reference| reference["reference"] == format!("confluent-id:{}", i32::MAX - 7))
            .unwrap();
        assert_eq!(merged["part"], "value");
        assert_eq!(merged["registryKey"], format!("confluent:{first}"));
        assert_eq!(merged["schemaId"], 1);
        assert_eq!(rejected["rejection"], "missing");
        assert_eq!(rejected["schemaId"], serde_json::Value::Null);
    }
}
