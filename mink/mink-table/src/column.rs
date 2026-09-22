//! A schema column: a typed field plus an optional aggregate, with its JSON shape.

use mink_types::{DataType, Field, FieldId};
use serde::{Deserialize, Serialize};

use crate::{Aggregate, Error};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Repr", into = "Repr")]
pub struct Column {
    field: Field,
    aggregate: Option<Aggregate>,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Result<Self, Error> {
        Ok(Column::from_field(Field::new(name, data_type)?))
    }

    pub fn from_field(field: Field) -> Self {
        Column {
            field,
            aggregate: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.field = self.field.with_description(description);
        self
    }

    pub fn with_aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }

    pub fn with_id(mut self, id: FieldId) -> Self {
        self.field = self.field.with_id(id);
        self
    }

    pub fn with_data_type(mut self, data_type: DataType) -> Self {
        self.field = self.field.with_data_type(data_type);
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Result<Self, Error> {
        self.field = self.field.with_name(name)?;
        Ok(self)
    }

    pub fn name(&self) -> &str {
        self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn description(&self) -> Option<&str> {
        self.field.description()
    }

    pub fn id(&self) -> Option<FieldId> {
        self.field.id()
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn aggregate(&self) -> Option<&Aggregate> {
        self.aggregate.as_ref()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Repr {
    name: String,
    data_type: DataType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    aggregate: Option<Aggregate>,
}

impl TryFrom<Repr> for Column {
    type Error = Error;

    fn try_from(repr: Repr) -> Result<Self, Self::Error> {
        let mut field = Field::new(repr.name, repr.data_type)?;
        if let Some(description) = repr.description {
            field = field.with_description(description);
        }
        if let Some(id) = repr.id {
            field = field.with_id(FieldId(id));
        }

        Ok(Column {
            field,
            aggregate: repr.aggregate,
        })
    }
}

impl From<Column> for Repr {
    fn from(column: Column) -> Self {
        Repr {
            name: column.field.name().to_owned(),
            description: column.field.description().map(str::to_owned),
            id: column.field.id().map(|id| id.0),
            data_type: column.field.data_type().clone(),
            aggregate: column.aggregate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_round_trip_keeps_every_part() {
        let column = Column::new("total", DataType::big_int().with_nullable(false))
            .unwrap()
            .with_description("running total")
            .with_aggregate(Aggregate::Sum)
            .with_id(FieldId(3));
        let json = serde_json::to_value(&column).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "name": "total",
                "data_type": {"type": "BIGINT", "nullable": false},
                "description": "running total",
                "id": 3,
                "aggregate": "sum",
            })
        );
        assert_eq!(serde_json::from_value::<Column>(json).unwrap(), column);
    }

    #[test]
    fn optional_parts_are_omitted() {
        let column = Column::new("id", DataType::int()).unwrap();
        let json = serde_json::to_value(&column).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"name": "id", "data_type": {"type": "INT"}})
        );
    }

    #[test]
    fn deserialization_validates_the_name() {
        let json = serde_json::json!({"name": "  ", "data_type": {"type": "INT"}});
        assert!(serde_json::from_value::<Column>(json).is_err());
    }
}
