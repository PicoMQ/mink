//! A borrowed scalar value, a normalized timestamp, and typed readers over Arrow arrays.

use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, PrimitiveArray, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, cast::AsArray,
};
use arrow_schema::{DataType as ArrowType, TimeUnit};
use mink_types::{DataType, Kind};

use crate::Error;

pub(crate) const COMPACT_DECIMAL_PRECISION: u8 = 18;
pub(crate) const COMPACT_TIMESTAMP_PRECISION: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timestamp {
    pub millis: i64,
    pub nanos: u32,
}

impl Timestamp {
    fn from_unit(value: i64, unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Timestamp {
                millis: value * 1000,
                nanos: 0,
            },
            TimeUnit::Millisecond => Timestamp {
                millis: value,
                nanos: 0,
            },
            TimeUnit::Microsecond => Timestamp {
                millis: value.div_euclid(1_000),
                nanos: value.rem_euclid(1_000) as u32 * 1_000,
            },
            TimeUnit::Nanosecond => Timestamp {
                millis: value.div_euclid(1_000_000),
                nanos: value.rem_euclid(1_000_000) as u32,
            },
        }
    }

    // Wraps like Java `long` arithmetic so extreme values hash the way the lakes hash them.
    pub fn micros(self) -> i64 {
        self.millis
            .wrapping_mul(1_000)
            .wrapping_add(i64::from(self.nanos / 1_000))
    }

    pub fn nanos(self) -> i64 {
        self.millis
            .wrapping_mul(1_000_000)
            .wrapping_add(i64::from(self.nanos))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scalar<'a> {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    String(&'a str),
    Bytes(&'a [u8]),
    Decimal { unscaled: i128, precision: u8 },
    Date(i32),
    Time(i64),
    Timestamp { at: Timestamp, precision: u8 },
}

#[derive(Debug, Clone, Copy)]
pub enum Reader<'a> {
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    Binary(&'a BinaryArray),
    FixedBinary(&'a FixedSizeBinaryArray),
    Decimal128(&'a Decimal128Array, u8),
    Date32(&'a Date32Array),
    Time32Second(&'a Time32SecondArray),
    Time32Millisecond(&'a Time32MillisecondArray),
    Time64Microsecond(&'a Time64MicrosecondArray),
    Time64Nanosecond(&'a Time64NanosecondArray),
    TimestampSecond(&'a TimestampSecondArray, u8),
    TimestampMillisecond(&'a TimestampMillisecondArray, u8),
    TimestampMicrosecond(&'a TimestampMicrosecondArray, u8),
    TimestampNanosecond(&'a TimestampNanosecondArray, u8),
}

impl<'a> Reader<'a> {
    pub fn new(array: &'a ArrayRef, data_type: &DataType, column: &str) -> Result<Self, Error> {
        let expected = ArrowType::from(data_type);
        let found = array.data_type();
        let mismatch = || Error::ColumnType {
            column: column.to_owned(),
            data_type: data_type.clone(),
            expected: expected.clone(),
            found: found.clone(),
        };
        let reader = match (&expected, found) {
            (ArrowType::Boolean, ArrowType::Boolean) => Reader::Boolean(array.as_boolean()),
            (ArrowType::Int8, ArrowType::Int8) => Reader::Int8(array.as_primitive()),
            (ArrowType::Int16, ArrowType::Int16) => Reader::Int16(array.as_primitive()),
            (ArrowType::Int32, ArrowType::Int32) => Reader::Int32(array.as_primitive()),
            (ArrowType::Int64, ArrowType::Int64) => Reader::Int64(array.as_primitive()),
            (ArrowType::Float32, ArrowType::Float32) => Reader::Float32(array.as_primitive()),
            (ArrowType::Float64, ArrowType::Float64) => Reader::Float64(array.as_primitive()),
            (ArrowType::Utf8, ArrowType::Utf8) => Reader::Utf8(array.as_string()),
            (ArrowType::Binary, ArrowType::Binary) => Reader::Binary(array.as_binary()),
            (ArrowType::FixedSizeBinary(want), ArrowType::FixedSizeBinary(got)) if want == got => {
                Reader::FixedBinary(array.as_fixed_size_binary())
            }
            (ArrowType::Decimal128(p, s), ArrowType::Decimal128(fp, fs)) if p == fp && s == fs => {
                Reader::Decimal128(array.as_primitive(), *p)
            }
            (ArrowType::Date32, ArrowType::Date32) => Reader::Date32(array.as_primitive()),
            (ArrowType::Time32(TimeUnit::Second), ArrowType::Time32(TimeUnit::Second)) => {
                Reader::Time32Second(array.as_primitive())
            }
            (
                ArrowType::Time32(TimeUnit::Millisecond),
                ArrowType::Time32(TimeUnit::Millisecond),
            ) => Reader::Time32Millisecond(array.as_primitive()),
            (
                ArrowType::Time64(TimeUnit::Microsecond),
                ArrowType::Time64(TimeUnit::Microsecond),
            ) => Reader::Time64Microsecond(array.as_primitive()),
            (ArrowType::Time64(TimeUnit::Nanosecond), ArrowType::Time64(TimeUnit::Nanosecond)) => {
                Reader::Time64Nanosecond(array.as_primitive())
            }
            (ArrowType::Timestamp(unit, tz), ArrowType::Timestamp(found_unit, found_tz))
                if unit == found_unit && tz.is_some() == found_tz.is_some() =>
            {
                let precision = match data_type.kind() {
                    Kind::Timestamp(p) | Kind::TimestampLtz(p) => p.get(),
                    _ => return Err(mismatch()),
                };
                match unit {
                    TimeUnit::Second => Reader::TimestampSecond(array.as_primitive(), precision),
                    TimeUnit::Millisecond => {
                        Reader::TimestampMillisecond(array.as_primitive(), precision)
                    }
                    TimeUnit::Microsecond => {
                        Reader::TimestampMicrosecond(array.as_primitive(), precision)
                    }
                    TimeUnit::Nanosecond => {
                        Reader::TimestampNanosecond(array.as_primitive(), precision)
                    }
                }
            }
            _ => return Err(mismatch()),
        };

        Ok(reader)
    }

    pub fn len(&self) -> usize {
        match self {
            Reader::Boolean(a) => a.len(),
            Reader::Int8(a) => a.len(),
            Reader::Int16(a) => a.len(),
            Reader::Int32(a) => a.len(),
            Reader::Int64(a) => a.len(),
            Reader::Float32(a) => a.len(),
            Reader::Float64(a) => a.len(),
            Reader::Utf8(a) => a.len(),
            Reader::Binary(a) => a.len(),
            Reader::FixedBinary(a) => a.len(),
            Reader::Decimal128(a, _) => a.len(),
            Reader::Date32(a) => a.len(),
            Reader::Time32Second(a) => a.len(),
            Reader::Time32Millisecond(a) => a.len(),
            Reader::Time64Microsecond(a) => a.len(),
            Reader::Time64Nanosecond(a) => a.len(),
            Reader::TimestampSecond(a, _) => a.len(),
            Reader::TimestampMillisecond(a, _) => a.len(),
            Reader::TimestampMicrosecond(a, _) => a.len(),
            Reader::TimestampNanosecond(a, _) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, row: usize) -> Option<Scalar<'a>> {
        use TimeUnit::*;

        let scalar = match self {
            Reader::Boolean(a) => Scalar::Boolean(a.is_valid(row).then(|| a.value(row))?),
            Reader::Int8(a) => Scalar::TinyInt(value(a, row)?),
            Reader::Int16(a) => Scalar::SmallInt(value(a, row)?),
            Reader::Int32(a) => Scalar::Int(value(a, row)?),
            Reader::Int64(a) => Scalar::BigInt(value(a, row)?),
            Reader::Float32(a) => Scalar::Float(value(a, row)?),
            Reader::Float64(a) => Scalar::Double(value(a, row)?),
            Reader::Utf8(a) => Scalar::String(a.is_valid(row).then(|| a.value(row))?),
            Reader::Binary(a) => Scalar::Bytes(a.is_valid(row).then(|| a.value(row))?),
            Reader::FixedBinary(a) => Scalar::Bytes(a.is_valid(row).then(|| a.value(row))?),
            Reader::Decimal128(a, precision) => Scalar::Decimal {
                unscaled: value(a, row)?,
                precision: *precision,
            },
            Reader::Date32(a) => Scalar::Date(value(a, row)?),
            Reader::Time32Second(a) => Scalar::Time(i64::from(value(a, row)?) * 1_000_000_000),
            Reader::Time32Millisecond(a) => Scalar::Time(i64::from(value(a, row)?) * 1_000_000),
            Reader::Time64Microsecond(a) => Scalar::Time(value(a, row)? * 1_000),
            Reader::Time64Nanosecond(a) => Scalar::Time(value(a, row)?),
            Reader::TimestampSecond(a, p) => timestamp(value(a, row)?, Second, *p),
            Reader::TimestampMillisecond(a, p) => timestamp(value(a, row)?, Millisecond, *p),
            Reader::TimestampMicrosecond(a, p) => timestamp(value(a, row)?, Microsecond, *p),
            Reader::TimestampNanosecond(a, p) => timestamp(value(a, row)?, Nanosecond, *p),
        };

        Some(scalar)
    }
}

fn value<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, row: usize) -> Option<T::Native> {
    array.is_valid(row).then(|| array.value(row))
}

fn timestamp<'a>(value: i64, unit: TimeUnit, precision: u8) -> Scalar<'a> {
    Scalar::Timestamp {
        at: Timestamp::from_unit(value, unit),
        precision,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mink_types::Precision;

    use super::*;

    #[test]
    fn timestamp_normalizes_every_unit_with_floor_semantics() {
        assert_eq!(
            Timestamp::from_unit(-1, TimeUnit::Microsecond),
            Timestamp {
                millis: -1,
                nanos: 999_000
            }
        );
        assert_eq!(
            Timestamp::from_unit(-1, TimeUnit::Nanosecond),
            Timestamp {
                millis: -1,
                nanos: 999_999
            }
        );
        assert_eq!(
            Timestamp::from_unit(1_700_000_000_123_456, TimeUnit::Microsecond),
            Timestamp {
                millis: 1_700_000_000_123,
                nanos: 456_000
            }
        );
        assert_eq!(
            Timestamp::from_unit(7, TimeUnit::Second),
            Timestamp {
                millis: 7_000,
                nanos: 0
            }
        );
        assert_eq!(Timestamp::from_unit(-1, TimeUnit::Nanosecond).micros(), -1);
    }

    #[test]
    fn reader_checks_the_arrow_type() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        let reader = Reader::new(&ints, &DataType::int(), "n").unwrap();
        assert_eq!(reader.len(), 2);
        assert_eq!(reader.get(0), Some(Scalar::Int(1)));
        assert_eq!(reader.get(1), None);

        let err = Reader::new(&ints, &DataType::big_int(), "n").unwrap_err();
        assert!(matches!(err, Error::ColumnType { column, .. } if column == "n"));
    }

    #[test]
    fn timestamp_zone_presence_must_match() {
        let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_000]));
        let ntz = DataType::timestamp(Precision::MILLIS);
        let ltz = DataType::timestamp_ltz(Precision::MILLIS);
        assert!(Reader::new(&array, &ntz, "t").is_ok());
        assert!(Reader::new(&array, &ltz, "t").is_err());

        let zoned: ArrayRef =
            Arc::new(TimestampMillisecondArray::from(vec![1_000]).with_timezone("+02:00"));
        assert!(Reader::new(&zoned, &ltz, "t").is_ok());
    }

    #[test]
    fn time_units_scale_to_nanoseconds() {
        let seconds: ArrayRef = Arc::new(Time32SecondArray::from(vec![1]));
        let millis: ArrayRef = Arc::new(Time32MillisecondArray::from(vec![1]));
        let micros: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![1]));
        assert_eq!(
            Reader::new(&seconds, &DataType::time(Precision::SECONDS), "t")
                .unwrap()
                .get(0),
            Some(Scalar::Time(1_000_000_000))
        );
        assert_eq!(
            Reader::new(&millis, &DataType::time(Precision::MILLIS), "t")
                .unwrap()
                .get(0),
            Some(Scalar::Time(1_000_000))
        );
        assert_eq!(
            Reader::new(&micros, &DataType::time(Precision::MICROS), "t")
                .unwrap()
                .get(0),
            Some(Scalar::Time(1_000))
        );
    }
}
