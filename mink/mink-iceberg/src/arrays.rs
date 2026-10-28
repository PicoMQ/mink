use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, ListArray, MapArray, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use iceberg::spec::{PrimitiveType, Type};
use mink_schema::{ColumnValue, PayloadValue};

use crate::Error;

pub(crate) fn array(
    field: &Field,
    field_type: &Type,
    values: &[Option<&ColumnValue>],
) -> Result<ArrayRef, Error> {
    let nulls = || NullBuffer::from_iter(values.iter().map(present));

    match (field_type, field.data_type()) {
        (Type::Primitive(primitive), data_type) => primitive_array(primitive, data_type, values),
        (Type::Struct(structure), DataType::Struct(fields)) => {
            let mut children = Vec::with_capacity(fields.len());

            for (child, nested) in fields.iter().zip(structure.fields()) {
                let child_values: Vec<Option<&ColumnValue>> = values
                    .iter()
                    .map(|value| match value {
                        Some(ColumnValue::Struct(children)) => children.get(&nested.id),
                        _ => None,
                    })
                    .collect();

                children.push(array(child, &nested.field_type, &child_values)?);
            }

            let structure = StructArray::try_new(fields.clone(), children, Some(nulls()))?;
            Ok(Arc::new(structure))
        }
        (Type::List(list), DataType::List(element)) => {
            let lengths = values.iter().map(|value| items_of(value).len());
            let items: Vec<Option<&ColumnValue>> =
                values.iter().flat_map(items_of).map(Some).collect();

            let elements = array(element, &list.element_field.field_type, &items)?;

            let offsets = OffsetBuffer::from_lengths(lengths);
            let list = ListArray::try_new(element.clone(), offsets, elements, Some(nulls()))?;
            Ok(Arc::new(list))
        }
        (Type::Map(map), DataType::Map(entries, sorted)) => {
            let DataType::Struct(entry_fields) = entries.data_type() else {
                return Err(Error::Corrupt(
                    "a map's entries are not a struct".to_owned(),
                ));
            };
            let lengths = values.iter().map(|value| entries_of(value).len());
            let keys: Vec<Option<&ColumnValue>> = values
                .iter()
                .flat_map(entries_of)
                .map(|(key, _)| Some(key))
                .collect();
            let items: Vec<Option<&ColumnValue>> = values
                .iter()
                .flat_map(entries_of)
                .map(|(_, value)| Some(value))
                .collect();

            let key_array = array(&entry_fields[0], &map.key_field.field_type, &keys)?;
            let value_array = array(&entry_fields[1], &map.value_field.field_type, &items)?;
            let entry_array =
                StructArray::try_new(entry_fields.clone(), vec![key_array, value_array], None)?;

            let offsets = OffsetBuffer::from_lengths(lengths);
            let map = MapArray::try_new(
                entries.clone(),
                offsets,
                entry_array,
                Some(nulls()),
                *sorted,
            )?;
            Ok(Arc::new(map))
        }
        (field_type, data_type) => Err(Error::Corrupt(format!(
            "{field_type} does not convert to {data_type}"
        ))),
    }
}

fn primitive_array(
    primitive: &PrimitiveType,
    data_type: &DataType,
    values: &[Option<&ColumnValue>],
) -> Result<ArrayRef, Error> {
    let scalars: Vec<Option<&PayloadValue>> = values
        .iter()
        .map(|value| match value {
            Some(ColumnValue::Scalar(scalar)) => Some(scalar),
            _ => None,
        })
        .collect();
    let ints = || {
        scalars
            .iter()
            .map(|scalar| scalar.and_then(|value| value.as_int(primitive)))
    };
    let longs = || {
        scalars
            .iter()
            .map(|scalar| scalar.and_then(|value| value.as_long(primitive)))
    };

    let array: ArrayRef = match primitive {
        PrimitiveType::Boolean => Arc::new(BooleanArray::from_iter(
            scalars
                .iter()
                .map(|scalar| scalar.and_then(PayloadValue::as_boolean)),
        )),
        PrimitiveType::Int => Arc::new(Int32Array::from_iter(ints())),
        PrimitiveType::Date => Arc::new(Date32Array::from_iter(ints())),
        PrimitiveType::Long => Arc::new(Int64Array::from_iter(longs())),
        PrimitiveType::Time => Arc::new(Time64MicrosecondArray::from_iter(longs())),
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            let timezone = match data_type {
                DataType::Timestamp(_, timezone) => timezone.clone(),
                _ => None,
            };

            let timestamps = TimestampMicrosecondArray::from_iter(longs());
            Arc::new(timestamps.with_timezone_opt(timezone))
        }
        PrimitiveType::Float => Arc::new(Float32Array::from_iter(
            scalars
                .iter()
                .map(|scalar| scalar.and_then(PayloadValue::as_float)),
        )),
        PrimitiveType::Double => Arc::new(Float64Array::from_iter(
            scalars
                .iter()
                .map(|scalar| scalar.and_then(PayloadValue::as_double)),
        )),
        PrimitiveType::Decimal { precision, scale } => {
            let unscaled = scalars
                .iter()
                .map(|scalar| scalar.and_then(PayloadValue::as_decimal));

            let decimals = Decimal128Array::from_iter(unscaled);
            Arc::new(decimals.with_precision_and_scale(*precision as u8, *scale as i8)?)
        }
        PrimitiveType::String => Arc::new(StringArray::from_iter(
            scalars
                .iter()
                .map(|scalar| scalar.and_then(PayloadValue::as_string)),
        )),
        PrimitiveType::Binary => Arc::new(LargeBinaryArray::from_iter(
            scalars
                .iter()
                .map(|scalar| scalar.and_then(PayloadValue::as_binary)),
        )),
        PrimitiveType::Uuid | PrimitiveType::Fixed(_) => {
            let DataType::FixedSizeBinary(size) = data_type else {
                return Err(Error::Corrupt(format!(
                    "{primitive} is not fixed-size binary"
                )));
            };
            let bytes = scalars
                .iter()
                .map(|scalar| scalar.and_then(PayloadValue::as_fixed));

            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                bytes, *size,
            )?)
        }
        other => return Err(Error::Corrupt(format!("{other} columns are not written"))),
    };

    Ok(array)
}

fn items_of<'a>(value: &Option<&'a ColumnValue>) -> &'a [ColumnValue] {
    match value {
        Some(ColumnValue::List(items)) => items,
        _ => &[],
    }
}

fn entries_of<'a>(value: &Option<&'a ColumnValue>) -> &'a [(ColumnValue, ColumnValue)] {
    match value {
        Some(ColumnValue::Map(entries)) => entries,
        _ => &[],
    }
}

fn present(value: &Option<&ColumnValue>) -> bool {
    !matches!(value, None | Some(ColumnValue::Scalar(PayloadValue::Null)))
}
