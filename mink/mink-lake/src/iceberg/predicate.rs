//! Binds a pushed-down filter to an Iceberg predicate: columns are resolved through the table's current
//! schema to field ids and named as the scanned snapshot knows them, literals become datums of the
//! column's type. A column added after the snapshot is null in every file of it, so tests against
//! it fold to a constant.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, TimeUnit};
use iceberg::expr::{Predicate as Bound, Reference};
use iceberg::spec::{Datum, NestedFieldRef, Schema};

use crate::error::{Error, Result};
use crate::predicate::{Compare, Predicate};

pub struct Binder<'s> {
    current: &'s Schema,
    snapshot: &'s Schema,
}

impl<'s> Binder<'s> {
    pub fn new(current: &'s Schema, snapshot: &'s Schema) -> Self {
        Binder { current, snapshot }
    }

    pub fn bind(&self, predicate: &Predicate) -> Result<Bound> {
        match predicate {
            Predicate::Compare { column, op, value } => {
                let Some((reference, field)) = self.reference(column)? else {
                    return Ok(Bound::AlwaysFalse);
                };
                let datum = datum(value, 0, &field)?;
                Ok(match op {
                    Compare::Eq => reference.equal_to(datum),
                    Compare::NotEq => reference.not_equal_to(datum),
                    Compare::Lt => reference.less_than(datum),
                    Compare::LtEq => reference.less_than_or_equal_to(datum),
                    Compare::Gt => reference.greater_than(datum),
                    Compare::GtEq => reference.greater_than_or_equal_to(datum),
                })
            }
            Predicate::In {
                column,
                values,
                negated,
            } => {
                let Some((reference, field)) = self.reference(column)? else {
                    return Ok(Bound::AlwaysFalse);
                };
                let datums = (0..values.len())
                    .map(|row| datum(values, row, &field))
                    .collect::<Result<Vec<_>>>()?;
                Ok(if *negated {
                    reference.is_not_in(datums)
                } else {
                    reference.is_in(datums)
                })
            }
            Predicate::Null { column, negated } => {
                let Some((reference, _)) = self.reference(column)? else {
                    return Ok(if *negated {
                        Bound::AlwaysFalse
                    } else {
                        Bound::AlwaysTrue
                    });
                };
                Ok(if *negated {
                    reference.is_not_null()
                } else {
                    reference.is_null()
                })
            }
            Predicate::And(items) => self.fold(items, Bound::and),
            Predicate::Or(items) => self.fold(items, Bound::or),
            Predicate::Not(inner) => Ok(self.bind(inner)?.negate()),
        }
    }

    fn fold(&self, items: &[Predicate], join: fn(Bound, Bound) -> Bound) -> Result<Bound> {
        let mut bound = items.iter().map(|item| self.bind(item));
        let first = bound
            .next()
            .ok_or_else(|| Error::Other("an empty boolean combination".into()))??;
        bound.try_fold(first, |acc, next| Ok(join(acc, next?)))
    }

    fn reference(&self, column: &str) -> Result<Option<(Reference, NestedFieldRef)>> {
        let field = self
            .current
            .field_by_name(column)
            .ok_or_else(|| Error::Other(format!("column {column} is not in the lake schema")))?;

        Ok(self
            .snapshot
            .name_by_field_id(field.id)
            .map(|name| (Reference::new(name), field.clone())))
    }
}

fn datum(values: &ArrayRef, row: usize, field: &NestedFieldRef) -> Result<Datum> {
    if values.is_null(row) {
        return Err(Error::Other(format!(
            "a null literal cannot be pushed down for {}",
            field.name
        )));
    }
    let datum = match values.data_type() {
        DataType::Boolean => Datum::bool(values.as_boolean().value(row)),
        DataType::Int8 => Datum::int(values.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => Datum::int(values.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => Datum::int(values.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => Datum::long(values.as_primitive::<Int64Type>().value(row)),
        DataType::Float32 => Datum::float(values.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => Datum::double(values.as_primitive::<Float64Type>().value(row)),
        DataType::Utf8 => Datum::string(values.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Datum::string(values.as_string::<i64>().value(row)),
        DataType::Utf8View => Datum::string(values.as_string_view().value(row)),
        DataType::Binary => Datum::binary(values.as_binary::<i32>().value(row).iter().copied()),
        DataType::LargeBinary => {
            Datum::binary(values.as_binary::<i64>().value(row).iter().copied())
        }
        DataType::Date32 => Datum::date(values.as_primitive::<Date32Type>().value(row)),
        DataType::Timestamp(unit, zone) => {
            let micros = match unit {
                TimeUnit::Millisecond => values
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row)
                    .saturating_mul(1_000),
                TimeUnit::Microsecond => {
                    values.as_primitive::<TimestampMicrosecondType>().value(row)
                }
                TimeUnit::Nanosecond => values
                    .as_primitive::<TimestampNanosecondType>()
                    .value(row)
                    .div_euclid(1_000),
                TimeUnit::Second => {
                    return Err(unsupported(values.data_type(), &field.name));
                }
            };
            if zone.is_some() {
                Datum::timestamptz_micros(micros)
            } else {
                Datum::timestamp_micros(micros)
            }
        }
        other => return Err(unsupported(other, &field.name)),
    };

    Ok(datum.to(&field.field_type)?)
}

fn unsupported(data_type: &DataType, column: &str) -> Error {
    Error::Other(format!(
        "a {data_type} literal cannot be pushed down for {column}"
    ))
}
