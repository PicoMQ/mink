use std::collections::HashMap;

use iceberg::spec::{PrimitiveType, Schema, Type};

use crate::merge::FieldIds;
use crate::types::path;
use crate::value::PayloadValue;

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Scalar(PayloadValue),
    Struct(HashMap<i32, ColumnValue>),
    List(Vec<ColumnValue>),
    Map(Vec<(ColumnValue, ColumnValue)>),
}

pub(crate) fn to_columns(
    value: PayloadValue,
    field_ids: &FieldIds,
    schema: &Schema,
) -> Option<HashMap<i32, ColumnValue>> {
    let top: Vec<(i32, ColumnValue)> = match (field_ids.get(""), value) {
        (Some(id), value) => vec![(id, column_value(value, "", field_ids)?)],
        (None, PayloadValue::Struct(children)) => children
            .into_iter()
            .map(|(key, child)| {
                let id = field_ids.get(&key)?;

                Some((id, column_value(child, &key, field_ids)?))
            })
            .collect::<Option<_>>()?,
        (None, _) => return None,
    };

    let fields = schema.as_struct().fields();
    let mut columns = HashMap::new();

    for (id, value) in top {
        let Some(field) = fields.iter().find(|field| field.id == id) else {
            continue;
        };

        if !fits(&value, &field.field_type) {
            return None;
        }

        columns.insert(id, value);
    }

    Some(columns)
}

fn column_value(value: PayloadValue, at: &str, field_ids: &FieldIds) -> Option<ColumnValue> {
    match value {
        PayloadValue::Struct(children) => {
            let mut columns = HashMap::with_capacity(children.len());

            for (key, child) in children {
                let child_path = path(at, &key);
                let id = field_ids.get(&child_path)?;

                columns.insert(id, column_value(child, &child_path, field_ids)?);
            }

            Some(ColumnValue::Struct(columns))
        }
        PayloadValue::List(items) => {
            let element = path(at, "element");

            items
                .into_iter()
                .map(|item| column_value(item, &element, field_ids))
                .collect::<Option<_>>()
                .map(ColumnValue::List)
        }
        PayloadValue::Map(entries) => {
            let (key, value) = (path(at, "key"), path(at, "value"));

            entries
                .into_iter()
                .map(|(entry_key, entry_value)| {
                    let entry_key = column_value(entry_key, &key, field_ids)?;
                    let entry_value = column_value(entry_value, &value, field_ids)?;

                    Some((entry_key, entry_value))
                })
                .collect::<Option<_>>()
                .map(ColumnValue::Map)
        }
        scalar => Some(ColumnValue::Scalar(scalar)),
    }
}

fn fits(value: &ColumnValue, field_type: &Type) -> bool {
    match (value, field_type) {
        (ColumnValue::Scalar(PayloadValue::Null), _) => true,
        (ColumnValue::Scalar(value), Type::Primitive(primitive)) => scalar_fits(value, primitive),
        (ColumnValue::Struct(children), Type::Struct(structure)) => {
            children.iter().all(|(id, child)| {
                structure
                    .field_by_id(*id)
                    .is_none_or(|field| fits(child, &field.field_type))
            })
        }
        (ColumnValue::List(items), Type::List(list)) => items
            .iter()
            .all(|item| fits(item, &list.element_field.field_type)),
        (ColumnValue::Map(entries), Type::Map(map)) => entries.iter().all(|(key, value)| {
            let key_present = !matches!(key, ColumnValue::Scalar(PayloadValue::Null));

            key_present
                && fits(key, &map.key_field.field_type)
                && fits(value, &map.value_field.field_type)
        }),
        _ => false,
    }
}

fn scalar_fits(value: &PayloadValue, primitive: &PrimitiveType) -> bool {
    match primitive {
        PrimitiveType::Boolean => value.as_boolean().is_some(),
        PrimitiveType::Int | PrimitiveType::Date => value.as_int(primitive).is_some(),
        PrimitiveType::Long
        | PrimitiveType::Time
        | PrimitiveType::Timestamp
        | PrimitiveType::Timestamptz => value.as_long(primitive).is_some(),
        PrimitiveType::Float => value.as_float().is_some(),
        PrimitiveType::Double => value.as_double().is_some(),
        PrimitiveType::Decimal { precision, .. } => value.as_decimal().is_some_and(|unscaled| {
            10_i128
                .checked_pow(*precision)
                .is_some_and(|limit| unscaled.unsigned_abs() < limit.unsigned_abs())
        }),
        PrimitiveType::String => value.as_string().is_some(),
        PrimitiveType::Binary => value.as_binary().is_some(),
        PrimitiveType::Uuid => matches!(value, PayloadValue::Uuid(_)),
        PrimitiveType::Fixed(length) => value
            .as_binary()
            .is_some_and(|bytes| bytes.len() as u64 == *length),
        _ => false,
    }
}
