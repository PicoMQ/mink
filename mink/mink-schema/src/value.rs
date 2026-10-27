use iceberg::spec::{PrimitiveLiteral, PrimitiveType};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum PayloadValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal(i128),
    String(String),
    Binary(Vec<u8>),
    Uuid([u8; 16]),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    Struct(Vec<(String, PayloadValue)>),
    List(Vec<PayloadValue>),
    Map(Vec<(PayloadValue, PayloadValue)>),
}

impl PayloadValue {
    pub(crate) fn json_text(value: &Value) -> PayloadValue {
        let mut text = String::new();

        sorted_json(value, &mut text);

        PayloadValue::String(text)
    }

    pub fn from_literal(
        primitive: &PrimitiveType,
        literal: &PrimitiveLiteral,
    ) -> Option<PayloadValue> {
        let value = match (primitive, literal) {
            (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(value)) => {
                PayloadValue::Boolean(*value)
            }
            (PrimitiveType::Date, PrimitiveLiteral::Int(value)) => PayloadValue::Date(*value),
            (_, PrimitiveLiteral::Int(value)) => PayloadValue::Int(*value),
            (PrimitiveType::Time, PrimitiveLiteral::Long(value)) => PayloadValue::Time(*value),
            (
                PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
                PrimitiveLiteral::Long(value),
            ) => PayloadValue::Timestamp(*value),
            (_, PrimitiveLiteral::Long(value)) => PayloadValue::Long(*value),
            (_, PrimitiveLiteral::Float(value)) => PayloadValue::Float(value.0),
            (_, PrimitiveLiteral::Double(value)) => PayloadValue::Double(value.0),
            (_, PrimitiveLiteral::String(value)) => PayloadValue::String(value.clone()),
            (_, PrimitiveLiteral::Binary(value)) => PayloadValue::Binary(value.clone()),
            (_, PrimitiveLiteral::Int128(value)) => PayloadValue::Decimal(*value),
            (_, PrimitiveLiteral::UInt128(value)) => PayloadValue::Uuid(value.to_be_bytes()),
            _ => return None,
        };

        Some(value)
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            PayloadValue::Boolean(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_int(&self, primitive: &PrimitiveType) -> Option<i32> {
        match (primitive, self) {
            (PrimitiveType::Int, PayloadValue::Int(value))
            | (PrimitiveType::Date, PayloadValue::Date(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn as_long(&self, primitive: &PrimitiveType) -> Option<i64> {
        match (primitive, self) {
            (PrimitiveType::Long, PayloadValue::Int(value)) => Some(i64::from(*value)),
            (PrimitiveType::Long, PayloadValue::Long(value))
            | (PrimitiveType::Time, PayloadValue::Time(value))
            | (
                PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
                PayloadValue::Timestamp(value),
            ) => Some(*value),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f32> {
        match self {
            PayloadValue::Float(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            PayloadValue::Float(value) => Some(f64::from(*value)),
            PayloadValue::Double(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_decimal(&self) -> Option<i128> {
        match self {
            PayloadValue::Decimal(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            PayloadValue::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            PayloadValue::Binary(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_fixed(&self) -> Option<&[u8]> {
        match self {
            PayloadValue::Binary(value) => Some(value),
            PayloadValue::Uuid(value) => Some(value),
            _ => None,
        }
    }
}

fn sorted_json(value: &Value, out: &mut String) {
    match value {
        Value::Array(items) => {
            out.push('[');

            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }

                sorted_json(item, out);
            }

            out.push(']');
        }
        Value::Object(members) => {
            let mut sorted: Vec<(&String, &Value)> = members.iter().collect();
            sorted.sort_by_key(|(key, _)| *key);
            out.push('{');

            for (index, (name, member)) in sorted.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }

                out.push_str(&Value::from(name.as_str()).to_string());
                out.push(':');
                sorted_json(member, out);
            }

            out.push('}');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_columns_sort_their_keys() {
        let value: Value = serde_json::from_str(r#"{"b":[{"z":1,"y":null}],"a":"\"x"}"#).unwrap();

        let decoded = PayloadValue::json_text(&value);

        assert_eq!(
            decoded,
            PayloadValue::String(r#"{"a":"\"x","b":[{"y":null,"z":1}]}"#.to_owned())
        );
    }
}
