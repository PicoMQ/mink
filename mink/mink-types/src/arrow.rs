//! Bidirectional conversion between logical types and Arrow types, fields and schemas,
//! carrying field ids through Arrow field metadata.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{
    DataType as ArrowType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
    TimeUnit,
};

use crate::{DataType, Decimal, Error, Field, FieldId, Fields, Kind, Length, Precision};

pub const FIELD_ID_METADATA: &str = "PARQUET:field_id";

const LIST_ELEMENT: &str = "element";
const MAP_ENTRIES: &str = "entries";
const MAP_KEY: &str = "key";
const MAP_VALUE: &str = "value";

fn unit(precision: Precision) -> TimeUnit {
    match precision.get() {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

fn precision(unit: TimeUnit) -> Precision {
    match unit {
        TimeUnit::Second => Precision::SECONDS,
        TimeUnit::Millisecond => Precision::MILLIS,
        TimeUnit::Microsecond => Precision::MICROS,
        TimeUnit::Nanosecond => Precision::NANOS,
    }
}

fn unsupported(arrow: &ArrowType) -> Error {
    Error::UnsupportedArrow(arrow.to_string())
}

impl From<&DataType> for ArrowType {
    fn from(data_type: &DataType) -> Self {
        match data_type.kind() {
            Kind::Boolean => ArrowType::Boolean,
            Kind::TinyInt => ArrowType::Int8,
            Kind::SmallInt => ArrowType::Int16,
            Kind::Int => ArrowType::Int32,
            Kind::BigInt => ArrowType::Int64,
            Kind::Float => ArrowType::Float32,
            Kind::Double => ArrowType::Float64,
            Kind::Char(_) | Kind::String => ArrowType::Utf8,
            Kind::Binary(length) => ArrowType::FixedSizeBinary(length.get() as i32),
            Kind::Bytes => ArrowType::Binary,
            Kind::Decimal(decimal) => {
                ArrowType::Decimal128(decimal.precision(), decimal.scale() as i8)
            }
            Kind::Date => ArrowType::Date32,
            Kind::Time(p) => match unit(*p) {
                unit @ (TimeUnit::Second | TimeUnit::Millisecond) => ArrowType::Time32(unit),
                unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond) => ArrowType::Time64(unit),
            },
            Kind::Timestamp(p) => ArrowType::Timestamp(unit(*p), None),
            Kind::TimestampLtz(p) => ArrowType::Timestamp(unit(*p), Some("UTC".into())),
            Kind::Array(element) => ArrowType::List(Arc::new(ArrowField::new(
                LIST_ELEMENT,
                element.as_ref().into(),
                element.is_nullable(),
            ))),
            Kind::Map { key, value } => {
                let entries = ArrowFields::from(vec![
                    ArrowField::new(MAP_KEY, key.as_ref().into(), false),
                    ArrowField::new(MAP_VALUE, value.as_ref().into(), value.is_nullable()),
                ]);
                ArrowType::Map(
                    Arc::new(ArrowField::new(
                        MAP_ENTRIES,
                        ArrowType::Struct(entries),
                        false,
                    )),
                    false,
                )
            }
            Kind::Row(fields) => ArrowType::Struct(fields.iter().map(ArrowField::from).collect()),
        }
    }
}

impl TryFrom<&ArrowType> for DataType {
    type Error = Error;

    fn try_from(arrow: &ArrowType) -> Result<Self, Self::Error> {
        let kind = match arrow {
            ArrowType::Boolean => Kind::Boolean,
            ArrowType::Int8 => Kind::TinyInt,
            ArrowType::Int16 => Kind::SmallInt,
            ArrowType::Int32 => Kind::Int,
            ArrowType::Int64 => Kind::BigInt,
            ArrowType::Float32 => Kind::Float,
            ArrowType::Float64 => Kind::Double,
            ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => Kind::String,
            ArrowType::Binary | ArrowType::LargeBinary | ArrowType::BinaryView => Kind::Bytes,
            ArrowType::FixedSizeBinary(width) => {
                let width = u32::try_from(*width).map_err(|_| unsupported(arrow))?;
                Kind::Binary(Length::new(width)?)
            }
            ArrowType::Decimal128(precision, scale) => {
                let scale = u8::try_from(*scale).map_err(|_| unsupported(arrow))?;
                Kind::Decimal(Decimal::new(*precision, scale)?)
            }
            ArrowType::Date32 => Kind::Date,
            ArrowType::Time32(unit) | ArrowType::Time64(unit) => Kind::Time(precision(*unit)),
            ArrowType::Timestamp(unit, None) => Kind::Timestamp(precision(*unit)),
            ArrowType::Timestamp(unit, Some(_)) => Kind::TimestampLtz(precision(*unit)),
            ArrowType::List(element) | ArrowType::LargeList(element) => {
                Kind::Array(Box::new(child(element)?))
            }
            ArrowType::Map(entries, _) => {
                let ArrowType::Struct(pair) = entries.data_type() else {
                    return Err(unsupported(arrow));
                };
                let [key, value] = pair.iter().collect::<Vec<_>>()[..] else {
                    return Err(unsupported(arrow));
                };
                Kind::Map {
                    key: Box::new(child(key)?),
                    value: Box::new(child(value)?),
                }
            }
            ArrowType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| Field::try_from(field.as_ref()))
                    .collect::<Result<Vec<_>, _>>()?;
                Kind::Row(Fields::new(fields)?)
            }
            other => return Err(unsupported(other)),
        };

        Ok(DataType::new(kind, true))
    }
}

fn child(field: &ArrowField) -> Result<DataType, Error> {
    Ok(DataType::try_from(field.data_type())?.with_nullable(field.is_nullable()))
}

impl From<&Field> for ArrowField {
    fn from(field: &Field) -> Self {
        let arrow = ArrowField::new(
            field.name(),
            field.data_type().into(),
            field.data_type().is_nullable(),
        );

        match field.id() {
            Some(FieldId(id)) => arrow.with_metadata(HashMap::from([(
                FIELD_ID_METADATA.to_owned(),
                id.to_string(),
            )])),
            None => arrow,
        }
    }
}

impl TryFrom<&ArrowField> for Field {
    type Error = Error;

    fn try_from(arrow: &ArrowField) -> Result<Self, Self::Error> {
        let field = Field::new(arrow.name(), child(arrow)?)?;

        match arrow.metadata().get(FIELD_ID_METADATA) {
            Some(raw) => {
                let id = raw.parse().map_err(|_| Error::FieldId(raw.clone()))?;
                Ok(field.with_id(FieldId(id)))
            }
            None => Ok(field),
        }
    }
}

impl From<&Fields> for ArrowSchema {
    fn from(fields: &Fields) -> Self {
        ArrowSchema::new(fields.iter().map(ArrowField::from).collect::<ArrowFields>())
    }
}

impl TryFrom<&ArrowSchema> for Fields {
    type Error = Error;

    fn try_from(schema: &ArrowSchema) -> Result<Self, Self::Error> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| Field::try_from(field.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Fields::new(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arrow(data_type: &DataType) -> ArrowType {
        data_type.into()
    }

    fn back(arrow: &ArrowType) -> DataType {
        DataType::try_from(arrow).unwrap()
    }

    #[test]
    fn leaves() {
        assert_eq!(arrow(&DataType::tiny_int()), ArrowType::Int8);
        assert_eq!(arrow(&DataType::big_int()), ArrowType::Int64);
        assert_eq!(arrow(&DataType::float()), ArrowType::Float32);
        assert_eq!(arrow(&DataType::string()), ArrowType::Utf8);
        assert_eq!(
            arrow(&DataType::char(Length::new(4).unwrap())),
            ArrowType::Utf8
        );
        assert_eq!(arrow(&DataType::bytes()), ArrowType::Binary);
        assert_eq!(
            arrow(&DataType::binary(Length::new(16).unwrap())),
            ArrowType::FixedSizeBinary(16)
        );
        assert_eq!(
            arrow(&DataType::decimal(Decimal::new(38, 10).unwrap())),
            ArrowType::Decimal128(38, 10)
        );
        assert_eq!(arrow(&DataType::date()), ArrowType::Date32);
    }

    #[test]
    fn temporal_precision_buckets() {
        let buckets = [
            (0, TimeUnit::Second),
            (1, TimeUnit::Millisecond),
            (3, TimeUnit::Millisecond),
            (4, TimeUnit::Microsecond),
            (6, TimeUnit::Microsecond),
            (7, TimeUnit::Nanosecond),
            (9, TimeUnit::Nanosecond),
        ];
        for (digits, unit) in buckets {
            let p = Precision::new(digits).unwrap();
            let expected_time = match unit {
                TimeUnit::Second | TimeUnit::Millisecond => ArrowType::Time32(unit),
                _ => ArrowType::Time64(unit),
            };
            assert_eq!(arrow(&DataType::time(p)), expected_time, "TIME({digits})");
            assert_eq!(
                arrow(&DataType::timestamp(p)),
                ArrowType::Timestamp(unit, None),
                "TIMESTAMP({digits})"
            );
            assert_eq!(
                arrow(&DataType::timestamp_ltz(p)),
                ArrowType::Timestamp(unit, Some("UTC".into())),
                "TIMESTAMP_LTZ({digits})"
            );
        }
    }

    #[test]
    fn timezone_distinguishes_ltz_on_the_way_back() {
        assert_eq!(
            back(&ArrowType::Timestamp(TimeUnit::Microsecond, None)),
            DataType::timestamp(Precision::MICROS)
        );
        assert_eq!(
            back(&ArrowType::Timestamp(
                TimeUnit::Millisecond,
                Some("+02:00".into())
            )),
            DataType::timestamp_ltz(Precision::MILLIS)
        );
    }

    #[test]
    fn list_element_carries_nullability() {
        let array = DataType::array(DataType::int().with_nullable(false));
        let ArrowType::List(element) = arrow(&array) else {
            panic!("expected list");
        };
        assert_eq!(element.name(), LIST_ELEMENT);
        assert!(!element.is_nullable());
        assert_eq!(back(&arrow(&array)), array);
        assert_eq!(back(&ArrowType::LargeList(element)), array);
    }

    #[test]
    fn map_layout_is_entries_of_key_value() {
        let map = DataType::map(DataType::string(), DataType::int());
        let ArrowType::Map(entries, sorted) = arrow(&map) else {
            panic!("expected map");
        };
        assert!(!sorted);
        assert_eq!(entries.name(), MAP_ENTRIES);
        assert!(!entries.is_nullable());
        let ArrowType::Struct(pair) = entries.data_type() else {
            panic!("expected struct");
        };
        assert_eq!(pair[0].name(), MAP_KEY);
        assert!(!pair[0].is_nullable());
        assert_eq!(pair[1].name(), MAP_VALUE);
        assert!(pair[1].is_nullable());
        assert_eq!(back(&arrow(&map)), map);
    }

    #[test]
    fn struct_fields_carry_ids_as_metadata() {
        let row = DataType::row(
            Fields::new(vec![
                Field::new("id", DataType::big_int().with_nullable(false))
                    .unwrap()
                    .with_id(FieldId(3)),
                Field::new("name", DataType::string()).unwrap(),
            ])
            .unwrap(),
        );
        let ArrowType::Struct(fields) = arrow(&row) else {
            panic!("expected struct");
        };
        assert_eq!(fields[0].metadata()[FIELD_ID_METADATA], "3");
        assert!(!fields[0].is_nullable());
        assert!(fields[1].metadata().is_empty());
        assert_eq!(back(&arrow(&row)), row);
    }

    #[test]
    fn schema_round_trip() {
        let fields = Fields::new(vec![
            Field::new("k", DataType::string().with_nullable(false))
                .unwrap()
                .with_id(FieldId(0)),
            Field::new("v", DataType::map(DataType::int(), DataType::bytes())).unwrap(),
        ])
        .unwrap();
        let schema = ArrowSchema::from(&fields);
        assert_eq!(Fields::try_from(&schema).unwrap(), fields);
    }

    #[test]
    fn unsupported_and_malformed_inputs() {
        assert!(matches!(
            DataType::try_from(&ArrowType::Float16),
            Err(Error::UnsupportedArrow(_))
        ));
        assert!(matches!(
            DataType::try_from(&ArrowType::Decimal256(50, 2)),
            Err(Error::UnsupportedArrow(_))
        ));
        assert!(matches!(
            DataType::try_from(&ArrowType::Decimal128(10, -2)),
            Err(Error::UnsupportedArrow(_))
        ));
        assert_eq!(
            DataType::try_from(&ArrowType::FixedSizeBinary(0)),
            Err(Error::Length)
        );
        let bad_id = ArrowField::new("a", ArrowType::Int32, true).with_metadata(HashMap::from([(
            FIELD_ID_METADATA.to_owned(),
            "x".to_owned(),
        )]));
        assert_eq!(
            Field::try_from(&bad_id),
            Err(Error::FieldId("x".to_owned()))
        );
    }
}
