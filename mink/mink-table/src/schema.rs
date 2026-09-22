//! A table schema: columns with stable field ids, an optional primary key and an optional auto-increment column.

use std::collections::HashSet;

use mink_types::{DataType, Fields, Kind, Root};
use serde::{Deserialize, Serialize};

use crate::{Column, Error, PrimaryKey};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Repr", into = "Repr")]
pub struct Schema {
    columns: Vec<Column>,
    fields: Fields,
    primary_key: Option<PrimaryKey>,
    auto_increment: Option<String>,
    next_field_id: u32,
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::default()
    }

    pub fn new(
        columns: Vec<Column>,
        primary_key: Option<PrimaryKey>,
        auto_increment: Option<String>,
        next_field_id: u32,
    ) -> Result<Self, Error> {
        let columns = match &primary_key {
            Some(key) => normalize_key_columns(columns, key)?,
            None => columns,
        };
        let fields = Fields::new(columns.iter().map(|c| c.field().clone()).collect())?;
        check_field_ids(&fields, next_field_id)?;
        if let Some(column) = &auto_increment {
            check_auto_increment(&columns, primary_key.as_ref(), column)?;
        }

        Ok(Schema {
            columns,
            fields,
            primary_key,
            auto_increment,
            next_field_id,
        })
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name() == name)
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.index_of(name)
    }

    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    pub fn primary_key(&self) -> Option<&PrimaryKey> {
        self.primary_key.as_ref()
    }

    pub fn primary_key_indexes(&self) -> Vec<usize> {
        self.primary_key
            .iter()
            .flat_map(|key| key.columns())
            .map(|column| {
                self.index_of(column)
                    .unwrap_or_else(|| unreachable!("primary key column `{column}` was validated"))
            })
            .collect()
    }

    pub fn auto_increment(&self) -> Option<&str> {
        self.auto_increment.as_deref()
    }

    pub fn next_field_id(&self) -> u32 {
        self.next_field_id
    }
}

fn normalize_key_columns(columns: Vec<Column>, key: &PrimaryKey) -> Result<Vec<Column>, Error> {
    for name in key.columns() {
        if !columns.iter().any(|column| column.name() == name) {
            return Err(Error::UnknownColumn(name.clone()));
        }
    }

    columns
        .into_iter()
        .map(|column| {
            if !key.contains(column.name()) {
                return Ok(column);
            }
            if column.aggregate().is_some() {
                return Err(Error::AggregateOnPrimaryKey(column.name().to_owned()));
            }
            let data_type = column.data_type().clone().with_nullable(false);
            Ok(column.with_data_type(data_type))
        })
        .collect()
}

fn check_field_ids(fields: &Fields, next_field_id: u32) -> Result<(), Error> {
    let mut ids = Vec::new();
    collect_ids(fields, &mut ids)?;

    let mut seen = HashSet::with_capacity(ids.len());
    for id in ids {
        if !seen.insert(id) {
            return Err(Error::DuplicateFieldId(id));
        }
        if id >= next_field_id {
            return Err(Error::FieldIdRange {
                id,
                next: next_field_id,
            });
        }
    }

    Ok(())
}

fn collect_ids(fields: &Fields, ids: &mut Vec<u32>) -> Result<(), Error> {
    for field in fields {
        let id = field
            .id()
            .ok_or_else(|| Error::MissingFieldId(field.name().to_owned()))?;
        ids.push(id.0);
        collect_nested_ids(field.data_type(), ids)?;
    }
    Ok(())
}

fn collect_nested_ids(data_type: &DataType, ids: &mut Vec<u32>) -> Result<(), Error> {
    match data_type.kind() {
        Kind::Row(fields) => collect_ids(fields, ids),
        Kind::Array(element) => collect_nested_ids(element, ids),
        Kind::Map { key, value } => {
            collect_nested_ids(key, ids)?;
            collect_nested_ids(value, ids)
        }
        _ => Ok(()),
    }
}

fn check_auto_increment(
    columns: &[Column],
    primary_key: Option<&PrimaryKey>,
    name: &str,
) -> Result<(), Error> {
    let Some(key) = primary_key else {
        return Err(Error::AutoIncrementWithoutPrimaryKey);
    };
    let column = columns
        .iter()
        .find(|column| column.name() == name)
        .ok_or_else(|| Error::UnknownColumn(name.to_owned()))?;
    if key.contains(name) {
        return Err(Error::AutoIncrementInPrimaryKey(name.to_owned()));
    }
    if !matches!(column.data_type().root(), Root::Int | Root::BigInt) {
        return Err(Error::AutoIncrementType(name.to_owned()));
    }

    Ok(())
}

#[derive(Debug, Default)]
pub struct SchemaBuilder {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
    auto_increment: Option<String>,
}

impl SchemaBuilder {
    pub fn column(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }

    pub fn primary_key(mut self, key: PrimaryKey) -> Self {
        self.primary_key = Some(key);
        self
    }

    pub fn auto_increment(mut self, column: impl Into<String>) -> Self {
        self.auto_increment = Some(column.into());
        self
    }

    pub fn build(self) -> Result<Schema, Error> {
        let fields = Fields::new(self.columns.iter().map(|c| c.field().clone()).collect())?;
        let mut next_field_id = 0;
        let fields = fields.assign_ids(&mut next_field_id);

        let columns = self
            .columns
            .into_iter()
            .zip(fields.into_inner())
            .map(|(column, field)| {
                let id = field.id().unwrap_or_else(|| unreachable!("assigned above"));
                column.with_data_type(field.data_type().clone()).with_id(id)
            })
            .collect();

        Schema::new(
            columns,
            self.primary_key,
            self.auto_increment,
            next_field_id,
        )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Repr {
    columns: Vec<Column>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    primary_key: Option<PrimaryKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auto_increment: Option<String>,
    next_field_id: u32,
}

impl TryFrom<Repr> for Schema {
    type Error = Error;

    fn try_from(repr: Repr) -> Result<Self, Self::Error> {
        Schema::new(
            repr.columns,
            repr.primary_key,
            repr.auto_increment,
            repr.next_field_id,
        )
    }
}

impl From<Schema> for Repr {
    fn from(schema: Schema) -> Self {
        Repr {
            columns: schema.columns,
            primary_key: schema.primary_key,
            auto_increment: schema.auto_increment,
            next_field_id: schema.next_field_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use mink_types::{Field, FieldId};
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::Aggregate;

    fn column(name: &str, data_type: DataType) -> Column {
        Column::new(name, data_type).unwrap()
    }

    fn key(columns: &[&str]) -> PrimaryKey {
        PrimaryKey::new(columns.iter().map(|c| (*c).to_owned()).collect()).unwrap()
    }

    fn nested() -> DataType {
        DataType::row(
            Fields::new(vec![
                Field::new("lat", DataType::double()).unwrap(),
                Field::new("lon", DataType::double()).unwrap(),
            ])
            .unwrap(),
        )
    }

    #[test]
    fn builder_assigns_ids_depth_first() {
        let schema = Schema::builder()
            .column(column("id", DataType::big_int()))
            .column(column("where", nested()))
            .column(column("tags", DataType::array(nested())))
            .build()
            .unwrap();

        let ids: Vec<u32> = schema.columns().iter().map(|c| c.id().unwrap().0).collect();
        assert_eq!(ids, [0, 1, 4]);
        let Kind::Row(inner) = schema.columns()[1].data_type().kind() else {
            panic!("expected row");
        };
        assert_eq!(inner[0].id(), Some(FieldId(2)));
        assert_eq!(inner[1].id(), Some(FieldId(3)));
        assert_eq!(schema.next_field_id(), 7);
        assert_eq!(
            schema.fields().names().collect::<Vec<_>>(),
            ["id", "where", "tags"]
        );
    }

    #[test]
    fn primary_key_columns_become_non_nullable() {
        let schema = Schema::builder()
            .column(column("id", DataType::big_int()))
            .column(column("name", DataType::string()))
            .primary_key(key(&["id"]))
            .build()
            .unwrap();
        assert!(!schema.column("id").unwrap().data_type().is_nullable());
        assert!(schema.column("name").unwrap().data_type().is_nullable());
        assert_eq!(schema.primary_key_indexes(), [0]);
    }

    #[test]
    fn primary_key_indexes_follow_key_order() {
        let schema = Schema::builder()
            .column(column("a", DataType::int()))
            .column(column("b", DataType::int()))
            .column(column("c", DataType::int()))
            .primary_key(key(&["c", "a"]))
            .build()
            .unwrap();
        assert_eq!(schema.primary_key_indexes(), [2, 0]);
    }

    #[test]
    fn rejects_invalid_shapes() {
        let err = Schema::builder()
            .column(column("id", DataType::int()))
            .column(column("id", DataType::int()))
            .build()
            .unwrap_err();
        assert_eq!(
            err,
            Error::Type(mink_types::Error::DuplicateFieldName("id".into()))
        );

        let err = Schema::builder()
            .column(column("id", DataType::int()))
            .primary_key(key(&["missing"]))
            .build()
            .unwrap_err();
        assert_eq!(err, Error::UnknownColumn("missing".into()));

        let err = Schema::builder()
            .column(column("id", DataType::int()).with_aggregate(Aggregate::Sum))
            .primary_key(key(&["id"]))
            .build()
            .unwrap_err();
        assert_eq!(err, Error::AggregateOnPrimaryKey("id".into()));
    }

    #[test]
    fn auto_increment_rules() {
        let base = || {
            Schema::builder()
                .column(column("id", DataType::string()))
                .column(column("seq", DataType::big_int()))
                .column(column("note", DataType::string()))
        };
        assert!(
            base()
                .primary_key(key(&["id"]))
                .auto_increment("seq")
                .build()
                .is_ok()
        );
        assert_eq!(
            base().auto_increment("seq").build().unwrap_err(),
            Error::AutoIncrementWithoutPrimaryKey
        );
        assert_eq!(
            base()
                .primary_key(key(&["id"]))
                .auto_increment("nope")
                .build()
                .unwrap_err(),
            Error::UnknownColumn("nope".into())
        );
        assert_eq!(
            base()
                .primary_key(key(&["seq"]))
                .auto_increment("seq")
                .build()
                .unwrap_err(),
            Error::AutoIncrementInPrimaryKey("seq".into())
        );
        assert_eq!(
            base()
                .primary_key(key(&["id"]))
                .auto_increment("note")
                .build()
                .unwrap_err(),
            Error::AutoIncrementType("note".into())
        );
    }

    #[test]
    fn new_checks_field_ids() {
        let with_id = |name: &str, id: u32| column(name, DataType::int()).with_id(FieldId(id));

        assert_eq!(
            Schema::new(vec![column("a", DataType::int())], None, None, 1).unwrap_err(),
            Error::MissingFieldId("a".into())
        );
        assert_eq!(
            Schema::new(vec![with_id("a", 0), with_id("b", 0)], None, None, 1).unwrap_err(),
            Error::DuplicateFieldId(0)
        );
        assert_eq!(
            Schema::new(vec![with_id("a", 5)], None, None, 5).unwrap_err(),
            Error::FieldIdRange { id: 5, next: 5 }
        );
        let nested_without_ids = column("where", nested()).with_id(FieldId(0));
        assert_eq!(
            Schema::new(vec![nested_without_ids], None, None, 1).unwrap_err(),
            Error::MissingFieldId("lat".into())
        );

        let gaps = Schema::new(vec![with_id("a", 3), with_id("b", 9)], None, None, 10).unwrap();
        assert_eq!(gaps.next_field_id(), 10);
    }

    #[test]
    fn json_round_trip_and_validation() {
        let schema = Schema::builder()
            .column(column("id", DataType::big_int()).with_description("key"))
            .column(column("total", DataType::big_int()).with_aggregate(Aggregate::Sum))
            .column(column("seq", DataType::int()))
            .primary_key(key(&["id"]))
            .auto_increment("seq")
            .build()
            .unwrap();
        let json = serde_json::to_value(&schema).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "columns": [
                    {"name": "id", "data_type": {"type": "BIGINT", "nullable": false}, "description": "key", "id": 0},
                    {"name": "total", "data_type": {"type": "BIGINT"}, "id": 1, "aggregate": "sum"},
                    {"name": "seq", "data_type": {"type": "INT"}, "id": 2},
                ],
                "primary_key": {"name": "PK_id", "columns": ["id"]},
                "auto_increment": "seq",
                "next_field_id": 3,
            })
        );
        assert_eq!(serde_json::from_value::<Schema>(json).unwrap(), schema);

        let broken = serde_json::json!({
            "columns": [{"name": "id", "data_type": {"type": "INT"}, "id": 0}],
            "next_field_id": 0,
        });
        assert!(serde_json::from_value::<Schema>(broken).is_err());
    }
}
