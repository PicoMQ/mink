use mink_schema::{Reference, RegistrySchema, SchemaFormat};
use serde_json::{Value, json};
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};

use crate::Error;

pub struct Registry<'a> {
    pool: &'a PgPool,
}

pub struct ListedSchema {
    id: i32,
    format: SchemaFormat,
    references: Vec<Reference>,
    tables: Vec<String>,
}

#[derive(FromRow)]
struct ListedRow {
    id: i32,
    format: String,
    referenced: Json<Value>,
    tables: Vec<String>,
}

impl ListedRow {
    fn listed(self) -> Result<ListedSchema, Error> {
        let corrupt = || Error::Corrupt(format!("registry schema {}", self.id));
        let format = SchemaFormat::from_name(&self.format).ok_or_else(corrupt)?;

        Ok(ListedSchema {
            id: self.id,
            format,
            references: references(self.id, &self.referenced)?,
            tables: self.tables,
        })
    }
}

impl ListedSchema {
    pub fn id(&self) -> i32 {
        self.id
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
            .map(|reference| json!({ "name": reference.name(), "id": reference.id() }))
            .collect();

        sqlx::query(
            "INSERT INTO registry_schema (id, format, definition, referenced) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
        )
        .bind(schema.id())
        .bind(schema.format().name())
        .bind(schema.definition())
        .bind(Json(references))
        .execute(self.pool)
        .await?;

        Ok(())
    }

    pub async fn get(&self, id: i32) -> Result<Option<RegistrySchema>, Error> {
        let row: Option<SchemaRow> = sqlx::query_as(
            "SELECT format, definition, referenced FROM registry_schema WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(self.pool)
        .await?;

        row.map(|row| schema(id, row)).transpose()
    }

    pub async fn page(&self, after: Option<i32>, limit: i64) -> Result<Vec<ListedSchema>, Error> {
        let rows: Vec<ListedRow> = sqlx::query_as(
            "SELECT registry_schema.id, registry_schema.format, registry_schema.referenced, \
                 coalesce(array_agg(DISTINCT topic_schema.topic ORDER BY topic_schema.topic) \
                     FILTER (WHERE topic_schema.topic IS NOT NULL), '{}') AS tables \
             FROM registry_schema \
             LEFT JOIN topic_schema ON topic_schema.registry_id = registry_schema.id \
             WHERE $1::INTEGER IS NULL OR registry_schema.id > $1 \
             GROUP BY registry_schema.id ORDER BY registry_schema.id LIMIT $2",
        )
        .bind(after)
        .bind(limit)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(ListedRow::listed).collect()
    }

    pub async fn tables(&self, id: i32) -> Result<Vec<String>, Error> {
        let tables = sqlx::query_scalar(
            "SELECT DISTINCT topic FROM topic_schema WHERE registry_id = $1 ORDER BY topic",
        )
        .bind(id)
        .fetch_all(self.pool)
        .await?;

        Ok(tables)
    }
}

fn schema(id: i32, row: SchemaRow) -> Result<RegistrySchema, Error> {
    let corrupt = || Error::Corrupt(format!("registry schema {id}"));
    let format = SchemaFormat::from_name(&row.format).ok_or_else(corrupt)?;

    let references = references(id, &row.referenced)?;

    Ok(RegistrySchema::new(id, format, row.definition, references))
}

fn references(id: i32, referenced: &Value) -> Result<Vec<Reference>, Error> {
    let corrupt = || Error::Corrupt(format!("registry schema {id}"));

    referenced
        .as_array()
        .ok_or_else(corrupt)?
        .iter()
        .map(|reference| {
            let name = reference["name"].as_str().ok_or_else(corrupt)?;
            let id = reference["id"]
                .as_i64()
                .and_then(|id| i32::try_from(id).ok());

            Ok(Reference::new(name, id.ok_or_else(corrupt)?))
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
        let first = RegistrySchema::new(
            id,
            SchemaFormat::Avro,
            "\"int\"",
            vec![Reference::new("a.B", 3)],
        );
        let second = RegistrySchema::new(id, SchemaFormat::Json, "{}", Vec::new());

        store.registry().put(&first).await.unwrap();
        store.registry().put(&second).await.unwrap();

        assert_eq!(store.registry().get(id).await.unwrap(), Some(first));
        assert_eq!(store.registry().get(id + 1).await.unwrap(), None);
    }
}
