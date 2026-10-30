use mink_schema::{Reference, RegistrySchema, SchemaFormat, SchemaKey};
use serde_json::{Value, json};
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};

use crate::Error;

pub struct Registry<'a> {
    pool: &'a PgPool,
}

pub struct ListedSchema {
    key: SchemaKey,
    format: SchemaFormat,
    references: Vec<Reference>,
    tables: Vec<String>,
}

#[derive(FromRow)]
struct ListedRow {
    key: String,
    format: String,
    referenced: Json<Value>,
    tables: Vec<String>,
}

impl ListedRow {
    fn listed(self) -> Result<ListedSchema, Error> {
        let corrupt = || Error::Corrupt(format!("registry schema {}", self.key));
        let key = SchemaKey::parse(&self.key).ok_or_else(corrupt)?;
        let format = SchemaFormat::from_name(&self.format).ok_or_else(corrupt)?;

        Ok(ListedSchema {
            key,
            format,
            references: references(key, &self.referenced)?,
            tables: self.tables,
        })
    }
}

impl ListedSchema {
    pub fn key(&self) -> SchemaKey {
        self.key
    }

    pub fn format(&self) -> SchemaFormat {
        self.format
    }

    pub fn references(&self) -> &[Reference] {
        &self.references
    }

    pub fn tables(&self) -> &[String] {
        &self.tables
    }
}

#[derive(FromRow)]
struct SchemaRow {
    format: String,
    definition: String,
    referenced: Json<Value>,
}

impl<'a> Registry<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Registry<'a> {
        Registry { pool }
    }

    pub async fn put(&self, schema: &RegistrySchema) -> Result<(), Error> {
        let references: Vec<Value> = schema
            .references()
            .iter()
            .map(
                |reference| json!({ "name": reference.name(), "key": reference.key().to_string() }),
            )
            .collect();

        sqlx::query(
            "INSERT INTO registry_schema (key, format, definition, referenced) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (key) DO NOTHING",
        )
        .bind(schema.key().to_string())
        .bind(schema.format().name())
        .bind(schema.definition())
        .bind(Json(references))
        .execute(self.pool)
        .await?;

        Ok(())
    }

    pub async fn get(&self, key: SchemaKey) -> Result<Option<RegistrySchema>, Error> {
        let row: Option<SchemaRow> = sqlx::query_as(
            "SELECT format, definition, referenced FROM registry_schema WHERE key = $1",
        )
        .bind(key.to_string())
        .fetch_optional(self.pool)
        .await?;

        row.map(|row| schema(key, row)).transpose()
    }

    pub async fn page(
        &self,
        after: Option<SchemaKey>,
        limit: i64,
    ) -> Result<Vec<ListedSchema>, Error> {
        let rows: Vec<ListedRow> = sqlx::query_as(
            "SELECT registry_schema.key, registry_schema.format, registry_schema.referenced, \
                 coalesce(array_agg(DISTINCT topic_schema.topic ORDER BY topic_schema.topic) \
                     FILTER (WHERE topic_schema.topic IS NOT NULL), '{}') AS tables \
             FROM registry_schema \
             LEFT JOIN topic_schema ON topic_schema.registry_key = registry_schema.key \
             WHERE $1::TEXT IS NULL OR registry_schema.key > $1 \
             GROUP BY registry_schema.key ORDER BY registry_schema.key LIMIT $2",
        )
        .bind(after.map(|key| key.to_string()))
        .bind(limit)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(ListedRow::listed).collect()
    }

    pub async fn tables(&self, key: SchemaKey) -> Result<Vec<String>, Error> {
        let tables = sqlx::query_scalar(
            "SELECT DISTINCT topic FROM topic_schema WHERE registry_key = $1 ORDER BY topic",
        )
        .bind(key.to_string())
        .fetch_all(self.pool)
        .await?;

        Ok(tables)
    }
}

fn schema(key: SchemaKey, row: SchemaRow) -> Result<RegistrySchema, Error> {
    let corrupt = || Error::Corrupt(format!("registry schema {key}"));
    let format = SchemaFormat::from_name(&row.format).ok_or_else(corrupt)?;

    let references = references(key, &row.referenced)?;

    Ok(RegistrySchema::new(key, format, row.definition, references))
}

fn references(key: SchemaKey, referenced: &Value) -> Result<Vec<Reference>, Error> {
    let corrupt = || Error::Corrupt(format!("registry schema {key}"));

    referenced
        .as_array()
        .ok_or_else(corrupt)?
        .iter()
        .map(|reference| {
            let name = reference["name"].as_str().ok_or_else(corrupt)?;
            let referenced = reference["key"].as_str().and_then(SchemaKey::parse);

            Ok(Reference::new(name, referenced.ok_or_else(corrupt)?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::testing;

    #[tokio::test]
    async fn a_registry_schema_is_stored_once() {
        let Some(store) = testing::store().await else {
            return;
        };
        let id = i32::try_from(Uuid::new_v4().as_u128() % 1_000_000_000).unwrap() + 1_000_000;
        let key = SchemaKey::Confluent(id);
        let first = RegistrySchema::new(
            key,
            SchemaFormat::Avro,
            "\"int\"",
            vec![Reference::new("a.B", SchemaKey::Confluent(3))],
        );
        let second = RegistrySchema::new(key, SchemaFormat::Json, "{}", Vec::new());

        store.registry().put(&first).await.unwrap();
        store.registry().put(&second).await.unwrap();

        assert_eq!(store.registry().get(key).await.unwrap(), Some(first));
        assert_eq!(
            store
                .registry()
                .get(SchemaKey::Confluent(id + 1))
                .await
                .unwrap(),
            None
        );
    }
}
