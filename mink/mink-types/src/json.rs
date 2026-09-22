//! JSON serialization of data types and fields in the format the JVM implementation writes and reads.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{DataType, Decimal, Field, FieldId, Fields, Kind, Length, Precision, Root};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Repr {
    #[serde(rename = "type")]
    root: Root,
    #[serde(
        default = "nullable_default",
        skip_serializing_if = "is_nullable_default"
    )]
    nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    length: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    precision: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scale: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    element_type: Option<Box<Repr>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_type: Option<Box<Repr>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value_type: Option<Box<Repr>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fields: Option<Vec<FieldRepr>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldRepr {
    name: String,
    #[serde(rename = "field_type")]
    data_type: Repr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    field_id: Option<u32>,
}

fn nullable_default() -> bool {
    true
}

fn is_nullable_default(nullable: &bool) -> bool {
    *nullable
}

impl Repr {
    fn leaf(root: Root, nullable: bool) -> Self {
        Repr {
            root,
            nullable,
            length: None,
            precision: None,
            scale: None,
            element_type: None,
            key_type: None,
            value_type: None,
            fields: None,
        }
    }
}

impl From<&DataType> for Repr {
    fn from(data_type: &DataType) -> Self {
        let mut repr = Repr::leaf(data_type.root(), data_type.is_nullable());
        match data_type.kind() {
            Kind::Char(length) | Kind::Binary(length) => repr.length = Some(length.get()),
            Kind::Decimal(decimal) => {
                repr.precision = Some(decimal.precision());
                repr.scale = Some(decimal.scale());
            }
            Kind::Time(precision) | Kind::Timestamp(precision) | Kind::TimestampLtz(precision) => {
                repr.precision = Some(precision.get());
            }
            Kind::Array(element) => repr.element_type = Some(Box::new(element.as_ref().into())),
            Kind::Map { key, value } => {
                repr.key_type = Some(Box::new(key.as_ref().into()));
                repr.value_type = Some(Box::new(value.as_ref().into()));
            }
            Kind::Row(fields) => repr.fields = Some(fields.iter().map(FieldRepr::from).collect()),
            Kind::Boolean
            | Kind::TinyInt
            | Kind::SmallInt
            | Kind::Int
            | Kind::BigInt
            | Kind::Float
            | Kind::Double
            | Kind::String
            | Kind::Bytes
            | Kind::Date => {}
        }

        repr
    }
}

impl From<&Field> for FieldRepr {
    fn from(field: &Field) -> Self {
        FieldRepr {
            name: field.name().to_owned(),
            data_type: field.data_type().into(),
            description: field.description().map(str::to_owned),
            field_id: field.id().map(|FieldId(id)| id),
        }
    }
}

fn required<T>(value: Option<T>, root: Root, name: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("{root} requires `{name}`"))
}

fn build(repr: Repr) -> Result<DataType, String> {
    let root = repr.root;
    let kind = match root {
        Root::Boolean => Kind::Boolean,
        Root::TinyInt => Kind::TinyInt,
        Root::SmallInt => Kind::SmallInt,
        Root::Int => Kind::Int,
        Root::BigInt => Kind::BigInt,
        Root::Float => Kind::Float,
        Root::Double => Kind::Double,
        Root::String => Kind::String,
        Root::Bytes => Kind::Bytes,
        Root::Date => Kind::Date,
        Root::Char => Kind::Char(length(repr.length, root)?),
        Root::Binary => Kind::Binary(length(repr.length, root)?),
        Root::Decimal => {
            let precision = required(repr.precision, root, "precision")?;
            let scale = required(repr.scale, root, "scale")?;
            Kind::Decimal(Decimal::new(precision, scale).map_err(|e| e.to_string())?)
        }
        Root::Time => Kind::Time(precision(repr.precision, root)?),
        Root::Timestamp => Kind::Timestamp(precision(repr.precision, root)?),
        Root::TimestampLtz => Kind::TimestampLtz(precision(repr.precision, root)?),
        Root::Array => {
            let element = required(repr.element_type, root, "element_type")?;
            Kind::Array(Box::new(build(*element)?))
        }
        Root::Map => {
            let key = required(repr.key_type, root, "key_type")?;
            let value = required(repr.value_type, root, "value_type")?;
            Kind::Map {
                key: Box::new(build(*key)?),
                value: Box::new(build(*value)?),
            }
        }
        Root::Row => {
            let fields = required(repr.fields, root, "fields")?
                .into_iter()
                .map(build_field)
                .collect::<Result<Vec<_>, _>>()?;
            Kind::Row(Fields::new(fields).map_err(|e| e.to_string())?)
        }
    };
    Ok(DataType::new(kind, repr.nullable))
}

fn length(value: Option<u32>, root: Root) -> Result<Length, String> {
    Length::new(required(value, root, "length")?).map_err(|e| e.to_string())
}

fn precision(value: Option<u8>, root: Root) -> Result<Precision, String> {
    Precision::new(required(value, root, "precision")?).map_err(|e| e.to_string())
}

fn build_field(repr: FieldRepr) -> Result<Field, String> {
    let mut field = Field::new(repr.name, build(repr.data_type)?).map_err(|e| e.to_string())?;
    if let Some(description) = repr.description {
        field = field.with_description(description);
    }
    if let Some(id) = repr.field_id {
        field = field.with_id(FieldId(id));
    }
    Ok(field)
}

impl Serialize for DataType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Repr::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        build(Repr::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl Serialize for Field {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        FieldRepr::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        build_field(FieldRepr::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use crate::{DataType, Decimal, Field, FieldId, Fields, Length, Precision};

    fn to_json(data_type: &DataType) -> Value {
        serde_json::to_value(data_type).unwrap()
    }

    fn from_json(value: Value) -> Result<DataType, serde_json::Error> {
        serde_json::from_value(value)
    }

    #[test]
    fn nullable_is_omitted_when_true() {
        assert_eq!(to_json(&DataType::int()), json!({"type": "INT"}));
        assert_eq!(
            to_json(&DataType::int().with_nullable(false)),
            json!({"type": "INT", "nullable": false})
        );
    }

    #[test]
    fn parameters_are_flat() {
        assert_eq!(
            to_json(&DataType::char(Length::new(8).unwrap())),
            json!({"type": "CHAR", "length": 8})
        );
        assert_eq!(
            to_json(&DataType::decimal(Decimal::new(12, 3).unwrap())),
            json!({"type": "DECIMAL", "precision": 12, "scale": 3})
        );
        assert_eq!(
            to_json(&DataType::timestamp_ltz(Precision::MILLIS)),
            json!({"type": "TIMESTAMP_LTZ", "precision": 3})
        );
    }

    #[test]
    fn nested_shapes() {
        let row = DataType::row(
            Fields::new(vec![
                Field::new("id", DataType::big_int().with_nullable(false))
                    .unwrap()
                    .with_id(FieldId(0)),
                Field::new("tags", DataType::array(DataType::string()))
                    .unwrap()
                    .with_description("labels"),
            ])
            .unwrap(),
        );
        let map = DataType::map(DataType::string(), row);
        assert_eq!(
            to_json(&map),
            json!({
                "type": "MAP",
                "key_type": {"type": "STRING", "nullable": false},
                "value_type": {
                    "type": "ROW",
                    "fields": [
                        {"name": "id", "field_type": {"type": "BIGINT", "nullable": false}, "field_id": 0},
                        {"name": "tags", "field_type": {"type": "ARRAY", "element_type": {"type": "STRING"}}, "description": "labels"}
                    ]
                }
            })
        );
        assert_eq!(from_json(to_json(&map)).unwrap(), map);
    }

    #[test]
    fn missing_or_invalid_parameters_are_errors() {
        assert!(from_json(json!({"type": "DECIMAL", "precision": 5})).is_err());
        assert!(from_json(json!({"type": "CHAR", "length": 0})).is_err());
        assert!(from_json(json!({"type": "TIME", "precision": 12})).is_err());
        assert!(from_json(json!({"type": "ARRAY"})).is_err());
        assert!(from_json(json!({"type": "VARCHAR"})).is_err());
        assert!(from_json(json!({"type": "INT", "bogus": 1})).is_err());
        assert!(
            from_json(json!({"type": "ROW", "fields": [
                {"name": "a", "field_type": {"type": "INT"}},
                {"name": "a", "field_type": {"type": "INT"}}
            ]}))
            .is_err()
        );
    }

    #[test]
    fn map_key_is_normalized_on_read() {
        let map = from_json(json!({
            "type": "MAP",
            "key_type": {"type": "STRING"},
            "value_type": {"type": "INT"}
        }))
        .unwrap();
        assert!(!map.children()[0].is_nullable());
    }
}
