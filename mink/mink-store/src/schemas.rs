use std::collections::BTreeMap;

use iceberg::spec::Schema;
use mink_core::Part;
use mink_schema::{
    BINARY_SCHEMA_ID, FieldIds, Merge, MergeResult, PartSchemaRef, Rejection, SchemaRef,
    TopicSchema, binary_schema,
};
use serde_json::Value;
use sqlx::types::Json;
use sqlx::{FromRow, PgConnection, PgPool};

use crate::Error;

pub struct Schemas<'a> {
    pool: &'a PgPool,
}

#[derive(FromRow)]
struct TopicSchemaRow {
    part: String,
    schema_ref: String,
    registry_id: Option<i32>,
    table_schema_id: Option<i32>,
    field_ids: Option<Json<BTreeMap<String, i32>>>,
    rejection: Option<String>,
}

impl<'a> Schemas<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Schemas<'a> {
        Schemas { pool }
    }

    pub async fn merge(
        &self,
        topic: &str,
        schema_ref: PartSchemaRef,
        merged: &Result<Merge, Rejection>,
    ) -> Result<MergeResult, Error> {
        let mut transaction = self.pool.begin().await?;

        crate::topics::lock(&mut transaction, topic).await?;

        if let Some(stored) = result(&mut transaction, topic, schema_ref).await? {
            return Ok(stored);
        }

        let result = match merged {
            Ok(merge) => {
                let latest = latest_id(&mut transaction, topic).await?;
                if latest != merge.based_on() {
                    return Err(Error::SchemaConflict {
                        topic: topic.to_owned(),
                        based_on: merge.based_on(),
                        latest,
                    });
                }

                if let Some(schema) = merge.schema() {
                    insert_schema(&mut transaction, topic, merge.based_on(), schema).await?;
                }

                upsert_field_ids(
                    &mut transaction,
                    topic,
                    schema_ref.part(),
                    merge.identity_field_ids(),
                )
                .await?;

                merge.result()
            }
            Err(rejection) => MergeResult::Rejected(*rejection),
        };

        insert_result(&mut transaction, topic, schema_ref, &result).await?;
        transaction.commit().await?;

        Ok(result)
    }

    pub async fn copy(&self, topic: &str, from: PartSchemaRef, to: SchemaRef) -> Result<(), Error> {
        sqlx::query(
            "INSERT INTO topic_schema \
             (topic, part, schema_ref, registry_id, table_schema_id, field_ids, rejection) \
             SELECT topic, part, $4, registry_id, table_schema_id, field_ids, rejection \
             FROM topic_schema WHERE topic = $1 AND part = $2 AND schema_ref = $3 \
             ON CONFLICT (topic, part, schema_ref) DO NOTHING",
        )
        .bind(topic)
        .bind(from.part().name())
        .bind(from.schema_ref().to_string())
        .bind(to.to_string())
        .execute(self.pool)
        .await?;

        Ok(())
    }

    pub async fn result(
        &self,
        topic: &str,
        schema_ref: PartSchemaRef,
    ) -> Result<Option<MergeResult>, Error> {
        let mut connection = self.pool.acquire().await?;

        result(&mut connection, topic, schema_ref).await
    }

    pub async fn topic_schemas(&self, topic: &str) -> Result<Vec<TopicSchema>, Error> {
        let rows: Vec<TopicSchemaRow> = sqlx::query_as(
            "SELECT part, schema_ref, registry_id, table_schema_id, field_ids, rejection \
             FROM topic_schema WHERE topic = $1 ORDER BY part, schema_ref",
        )
        .bind(topic)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(topic_schema).collect()
    }

    pub async fn latest(&self, topic: &str) -> Result<Schema, Error> {
        let definition: Option<Json<Value>> = sqlx::query_scalar(
            "SELECT definition FROM table_schema WHERE topic = $1 ORDER BY id DESC LIMIT 1",
        )
        .bind(topic)
        .fetch_optional(self.pool)
        .await?;

        match definition {
            Some(Json(definition)) => parsed(definition),
            None => Ok(binary_schema().clone()),
        }
    }

    pub async fn table_schema(&self, topic: &str, id: i32) -> Result<Option<Schema>, Error> {
        if id == BINARY_SCHEMA_ID {
            return Ok(Some(binary_schema().clone()));
        }

        let definition: Option<Json<Value>> =
            sqlx::query_scalar("SELECT definition FROM table_schema WHERE topic = $1 AND id = $2")
                .bind(topic)
                .bind(id)
                .fetch_optional(self.pool)
                .await?;

        definition
            .map(|Json(definition)| parsed(definition))
            .transpose()
    }

    pub async fn table_schemas(&self, topic: &str) -> Result<Vec<Schema>, Error> {
        let definitions: Vec<Json<Value>> =
            sqlx::query_scalar("SELECT definition FROM table_schema WHERE topic = $1 ORDER BY id")
                .bind(topic)
                .fetch_all(self.pool)
                .await?;

        definitions
            .into_iter()
            .map(|Json(definition)| parsed(definition))
            .collect()
    }

    pub async fn field_ids(&self, topic: &str, part: Part) -> Result<FieldIds, Error> {
        let rows: Vec<(String, i32)> =
            sqlx::query_as("SELECT path, field_id FROM field WHERE topic = $1 AND part = $2")
                .bind(topic)
                .bind(part.name())
                .fetch_all(self.pool)
                .await?;

        Ok(rows.into_iter().collect())
    }
}

async fn result(
    connection: &mut PgConnection,
    topic: &str,
    schema_ref: PartSchemaRef,
) -> Result<Option<MergeResult>, Error> {
    let row: Option<TopicSchemaRow> = sqlx::query_as(
        "SELECT part, schema_ref, registry_id, table_schema_id, field_ids, rejection \
         FROM topic_schema WHERE topic = $1 AND part = $2 AND schema_ref = $3",
    )
    .bind(topic)
    .bind(schema_ref.part().name())
    .bind(schema_ref.schema_ref().to_string())
    .fetch_optional(&mut *connection)
    .await?;

    let stored = row.map(topic_schema).transpose()?;

    Ok(stored.map(|stored| stored.result().clone()))
}

async fn latest_id(connection: &mut PgConnection, topic: &str) -> Result<i32, Error> {
    let latest: Option<i32> =
        sqlx::query_scalar("SELECT MAX(id) FROM table_schema WHERE topic = $1")
            .bind(topic)
            .fetch_one(&mut *connection)
            .await?;

    Ok(latest.unwrap_or(BINARY_SCHEMA_ID))
}

async fn insert_schema(
    connection: &mut PgConnection,
    topic: &str,
    based_on: i32,
    schema: &Schema,
) -> Result<(), Error> {
    if schema.schema_id() != based_on + 1 {
        return Err(Error::UnexpectedSchemaId {
            topic: topic.to_owned(),
            based_on,
            id: schema.schema_id(),
        });
    }

    let definition = serde_json::to_value(schema).map_err(|error| {
        Error::Corrupt(format!(
            "table schema {} of {topic}: {error}",
            schema.schema_id()
        ))
    })?;
    sqlx::query("INSERT INTO table_schema (topic, id, definition) VALUES ($1, $2, $3)")
        .bind(topic)
        .bind(schema.schema_id())
        .bind(Json(definition))
        .execute(&mut *connection)
        .await?;

    Ok(())
}

async fn upsert_field_ids(
    connection: &mut PgConnection,
    topic: &str,
    part: Part,
    field_ids: &FieldIds,
) -> Result<(), Error> {
    for (path, field_id) in field_ids.iter() {
        sqlx::query(
            "INSERT INTO field (topic, part, path, field_id) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (topic, part, path) DO UPDATE SET field_id = EXCLUDED.field_id",
        )
        .bind(topic)
        .bind(part.name())
        .bind(path)
        .bind(field_id)
        .execute(&mut *connection)
        .await?;
    }

    Ok(())
}

async fn insert_result(
    connection: &mut PgConnection,
    topic: &str,
    schema_ref: PartSchemaRef,
    result: &MergeResult,
) -> Result<(), Error> {
    let (registry_id, table_schema_id, field_ids, rejection) = match result {
        MergeResult::Merged {
            registry_id,
            table_schema_id,
            field_ids,
        } => {
            let field_ids: BTreeMap<&str, i32> = field_ids.iter().collect();

            (
                Some(*registry_id),
                Some(*table_schema_id),
                Some(Json(field_ids)),
                None,
            )
        }
        MergeResult::Rejected(rejection) => (None, None, None, Some(rejection.name())),
    };

    sqlx::query(
        "INSERT INTO topic_schema \
         (topic, part, schema_ref, registry_id, table_schema_id, field_ids, rejection) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(topic)
    .bind(schema_ref.part().name())
    .bind(schema_ref.schema_ref().to_string())
    .bind(registry_id)
    .bind(table_schema_id)
    .bind(field_ids)
    .bind(rejection)
    .execute(&mut *connection)
    .await?;

    Ok(())
}

fn parsed(definition: Value) -> Result<Schema, Error> {
    serde_json::from_value(definition)
        .map_err(|error| Error::Corrupt(format!("a stored table schema: {error}")))
}

fn topic_schema(row: TopicSchemaRow) -> Result<TopicSchema, Error> {
    let corrupt = || Error::Corrupt("a topic schema row".to_owned());
    let part = Part::from_name(&row.part).ok_or_else(corrupt)?;
    let schema_ref = SchemaRef::from_key(&row.schema_ref).ok_or_else(corrupt)?;

    let result = match (
        row.registry_id,
        row.table_schema_id,
        row.field_ids,
        row.rejection,
    ) {
        (Some(registry_id), Some(table_schema_id), Some(Json(field_ids)), None) => {
            MergeResult::Merged {
                registry_id,
                table_schema_id,
                field_ids: field_ids.into_iter().collect(),
            }
        }
        (None, None, None, Some(rejection)) => {
            MergeResult::Rejected(Rejection::from_name(&rejection).ok_or_else(corrupt)?)
        }
        _ => return Err(corrupt()),
    };

    Ok(TopicSchema::new(
        PartSchemaRef::new(part, schema_ref),
        result,
    ))
}

#[cfg(test)]
mod tests {
    use mink_schema::{PayloadSchema, RegistrySchema, SchemaFormat, SchemaSet, merge};
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::Store;
    use crate::testing;

    async fn registered(store: &Store, fields: &[&str]) -> RegistrySchema {
        let id = i32::try_from(Uuid::new_v4().as_u128() % 1_000_000_000).unwrap() + 1_000_000;
        let fields: Vec<Value> = fields
            .iter()
            .map(|name| json!({"name": name, "type": "int"}))
            .collect();
        let definition = json!({"type": "record", "name": "R", "fields": fields});
        let schema =
            RegistrySchema::new(id, SchemaFormat::Avro, definition.to_string(), Vec::new());

        store.registry().put(&schema).await.unwrap();
        schema
    }

    fn merged(table: &Schema, field_ids: &FieldIds, registry: &RegistrySchema) -> Merge {
        let set = SchemaSet::new(registry.clone(), Vec::new());
        let payload = PayloadSchema::parse(&set).unwrap();

        merge(table, field_ids, Part::Value, &payload, registry.id()).unwrap()
    }

    fn by_id(registry: &RegistrySchema) -> PartSchemaRef {
        PartSchemaRef::new(Part::Value, SchemaRef::Id(registry.id()))
    }

    #[tokio::test]
    async fn racing_merges_number_schemas_without_gaps() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let (first, second) = (
            registered(&store, &["a"]).await,
            registered(&store, &["b"]).await,
        );
        let schemas = store.schemas();
        let none = FieldIds::default();
        let (first_merge, second_merge) = (
            Ok(merged(binary_schema(), &none, &first)),
            Ok(merged(binary_schema(), &none, &second)),
        );

        let (one, two) = tokio::join!(
            schemas.merge(topic.name(), by_id(&first), &first_merge),
            schemas.merge(topic.name(), by_id(&second), &second_merge),
        );

        let (conflict, loser) = match (&one, &two) {
            (Ok(_), Err(_)) => (&two, &second),
            (Err(_), Ok(_)) => (&one, &first),
            _ => panic!("exactly one merge based on schema 0 wins: {one:?} {two:?}"),
        };
        assert!(matches!(
            conflict,
            Err(Error::SchemaConflict {
                based_on: 0,
                latest: 1,
                ..
            })
        ));

        let latest = schemas.latest(topic.name()).await.unwrap();
        let field_ids = schemas.field_ids(topic.name(), Part::Value).await.unwrap();
        let retried = Ok(merged(&latest, &field_ids, loser));
        schemas
            .merge(topic.name(), by_id(loser), &retried)
            .await
            .unwrap();

        let ids: Vec<i32> = schemas
            .table_schemas(topic.name())
            .await
            .unwrap()
            .iter()
            .map(Schema::schema_id)
            .collect();
        assert_eq!(ids, [1, 2]);
        assert_eq!(
            schemas.table_schema(topic.name(), 2).await.unwrap(),
            Some(schemas.latest(topic.name()).await.unwrap())
        );
    }

    #[tokio::test]
    async fn a_schema_merges_once_and_is_copied_to_its_guid() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let registry = registered(&store, &["a"]).await;
        let guid = SchemaRef::Guid(Uuid::new_v4());
        let rejected_guid = PartSchemaRef::new(Part::Value, SchemaRef::Guid(Uuid::new_v4()));
        let schemas = store.schemas();
        let first_merge = Ok(merged(binary_schema(), &FieldIds::default(), &registry));

        let first = schemas
            .merge(topic.name(), by_id(&registry), &first_merge)
            .await
            .unwrap();
        let again = schemas
            .merge(topic.name(), by_id(&registry), &Err(Rejection::Unsupported))
            .await
            .unwrap();
        schemas
            .merge(topic.name(), rejected_guid, &Err(Rejection::Missing))
            .await
            .unwrap();
        schemas
            .copy(topic.name(), by_id(&registry), guid)
            .await
            .unwrap();

        assert_eq!(first, again);
        assert_eq!(
            schemas
                .result(topic.name(), PartSchemaRef::new(Part::Value, guid))
                .await
                .unwrap(),
            Some(first)
        );
        assert_eq!(schemas.table_schemas(topic.name()).await.unwrap().len(), 1);
        assert_eq!(
            schemas.result(topic.name(), rejected_guid).await.unwrap(),
            Some(MergeResult::Rejected(Rejection::Missing))
        );
        assert_eq!(schemas.topic_schemas(topic.name()).await.unwrap().len(), 3);
        assert_eq!(
            schemas
                .table_schema(topic.name(), BINARY_SCHEMA_ID)
                .await
                .unwrap(),
            Some(binary_schema().clone())
        );
    }
}
