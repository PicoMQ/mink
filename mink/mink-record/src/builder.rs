//! Arrow column builders fed from scalars, and a row-oriented batch builder on top of them.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    StringBuilder, Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType as ArrowType, Schema as ArrowSchema, SchemaRef, TimeUnit};
use mink_types::{DataType, Fields};

use crate::{Error, Scalar};

pub enum Builder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    FixedBinary(FixedSizeBinaryBuilder),
    Decimal128(Decimal128Builder),
    Date32(Date32Builder),
    Time32Second(Time32SecondBuilder),
    Time32Millisecond(Time32MillisecondBuilder),
    Time64Microsecond(Time64MicrosecondBuilder),
    Time64Nanosecond(Time64NanosecondBuilder),
    TimestampSecond(TimestampSecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
}

impl Builder {
    pub fn new(data_type: &DataType, capacity: usize) -> Result<Self, Error> {
        let arrow = ArrowType::from(data_type);
        let builder = match &arrow {
            ArrowType::Boolean => Builder::Boolean(BooleanBuilder::with_capacity(capacity)),
            ArrowType::Int8 => Builder::Int8(Int8Builder::with_capacity(capacity)),
            ArrowType::Int16 => Builder::Int16(Int16Builder::with_capacity(capacity)),
            ArrowType::Int32 => Builder::Int32(Int32Builder::with_capacity(capacity)),
            ArrowType::Int64 => Builder::Int64(Int64Builder::with_capacity(capacity)),
            ArrowType::Float32 => Builder::Float32(Float32Builder::with_capacity(capacity)),
            ArrowType::Float64 => Builder::Float64(Float64Builder::with_capacity(capacity)),
            ArrowType::Utf8 => Builder::Utf8(StringBuilder::with_capacity(capacity, 0)),
            ArrowType::Binary => Builder::Binary(BinaryBuilder::with_capacity(capacity, 0)),
            ArrowType::FixedSizeBinary(len) => {
                Builder::FixedBinary(FixedSizeBinaryBuilder::with_capacity(capacity, *len))
            }
            ArrowType::Decimal128(precision, scale) => Builder::Decimal128(
                Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| Error::Ipc(e.to_string()))?,
            ),
            ArrowType::Date32 => Builder::Date32(Date32Builder::with_capacity(capacity)),
            ArrowType::Time32(TimeUnit::Second) => {
                Builder::Time32Second(Time32SecondBuilder::with_capacity(capacity))
            }
            ArrowType::Time32(TimeUnit::Millisecond) => {
                Builder::Time32Millisecond(Time32MillisecondBuilder::with_capacity(capacity))
            }
            ArrowType::Time64(TimeUnit::Microsecond) => {
                Builder::Time64Microsecond(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            ArrowType::Time64(TimeUnit::Nanosecond) => {
                Builder::Time64Nanosecond(Time64NanosecondBuilder::with_capacity(capacity))
            }
            ArrowType::Timestamp(unit, tz) => {
                let tz = tz.clone();
                match unit {
                    TimeUnit::Second => Builder::TimestampSecond(
                        TimestampSecondBuilder::with_capacity(capacity).with_timezone_opt(tz),
                    ),
                    TimeUnit::Millisecond => Builder::TimestampMillisecond(
                        TimestampMillisecondBuilder::with_capacity(capacity).with_timezone_opt(tz),
                    ),
                    TimeUnit::Microsecond => Builder::TimestampMicrosecond(
                        TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone_opt(tz),
                    ),
                    TimeUnit::Nanosecond => Builder::TimestampNanosecond(
                        TimestampNanosecondBuilder::with_capacity(capacity).with_timezone_opt(tz),
                    ),
                }
            }
            _ => return Err(Error::RowType(data_type.clone())),
        };

        Ok(builder)
    }

    pub fn append(&mut self, data_type: &DataType, value: Option<Scalar<'_>>) -> Result<(), Error> {
        let mismatch = |scalar: &Scalar<'_>| Error::ScalarType {
            data_type: data_type.clone(),
            scalar: format!("{scalar:?}"),
        };
        match (self, value) {
            (Builder::Boolean(b), Some(Scalar::Boolean(v))) => b.append_value(v),
            (Builder::Boolean(b), None) => b.append_null(),
            (Builder::Int8(b), Some(Scalar::TinyInt(v))) => b.append_value(v),
            (Builder::Int8(b), None) => b.append_null(),
            (Builder::Int16(b), Some(Scalar::SmallInt(v))) => b.append_value(v),
            (Builder::Int16(b), None) => b.append_null(),
            (Builder::Int32(b), Some(Scalar::Int(v))) => b.append_value(v),
            (Builder::Int32(b), None) => b.append_null(),
            (Builder::Int64(b), Some(Scalar::BigInt(v))) => b.append_value(v),
            (Builder::Int64(b), None) => b.append_null(),
            (Builder::Float32(b), Some(Scalar::Float(v))) => b.append_value(v),
            (Builder::Float32(b), None) => b.append_null(),
            (Builder::Float64(b), Some(Scalar::Double(v))) => b.append_value(v),
            (Builder::Float64(b), None) => b.append_null(),
            (Builder::Utf8(b), Some(Scalar::String(v))) => b.append_value(v),
            (Builder::Utf8(b), None) => b.append_null(),
            (Builder::Binary(b), Some(Scalar::Bytes(v))) => b.append_value(v),
            (Builder::Binary(b), None) => b.append_null(),
            (Builder::FixedBinary(b), Some(Scalar::Bytes(v))) => {
                b.append_value(v).map_err(|e| Error::Ipc(e.to_string()))?
            }
            (Builder::FixedBinary(b), None) => b.append_null(),
            (Builder::Decimal128(b), Some(Scalar::Decimal { unscaled, .. })) => {
                b.append_value(unscaled)
            }
            (Builder::Decimal128(b), None) => b.append_null(),
            (Builder::Date32(b), Some(Scalar::Date(v))) => b.append_value(v),
            (Builder::Date32(b), None) => b.append_null(),
            (Builder::Time32Second(b), Some(Scalar::Time(nanos))) => {
                b.append_value((nanos / 1_000_000_000) as i32)
            }
            (Builder::Time32Second(b), None) => b.append_null(),
            (Builder::Time32Millisecond(b), Some(Scalar::Time(nanos))) => {
                b.append_value((nanos / 1_000_000) as i32)
            }
            (Builder::Time32Millisecond(b), None) => b.append_null(),
            (Builder::Time64Microsecond(b), Some(Scalar::Time(nanos))) => {
                b.append_value(nanos / 1_000)
            }
            (Builder::Time64Microsecond(b), None) => b.append_null(),
            (Builder::Time64Nanosecond(b), Some(Scalar::Time(nanos))) => b.append_value(nanos),
            (Builder::Time64Nanosecond(b), None) => b.append_null(),
            (Builder::TimestampSecond(b), Some(Scalar::Timestamp { at, .. })) => {
                b.append_value(at.millis.div_euclid(1_000))
            }
            (Builder::TimestampSecond(b), None) => b.append_null(),
            (Builder::TimestampMillisecond(b), Some(Scalar::Timestamp { at, .. })) => {
                b.append_value(at.millis)
            }
            (Builder::TimestampMillisecond(b), None) => b.append_null(),
            (Builder::TimestampMicrosecond(b), Some(Scalar::Timestamp { at, .. })) => {
                b.append_value(at.micros())
            }
            (Builder::TimestampMicrosecond(b), None) => b.append_null(),
            (Builder::TimestampNanosecond(b), Some(Scalar::Timestamp { at, .. })) => {
                b.append_value(at.nanos())
            }
            (Builder::TimestampNanosecond(b), None) => b.append_null(),
            (_, Some(scalar)) => return Err(mismatch(&scalar)),
        }

        Ok(())
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::Boolean(b) => Arc::new(b.finish()),
            Builder::Int8(b) => Arc::new(b.finish()),
            Builder::Int16(b) => Arc::new(b.finish()),
            Builder::Int32(b) => Arc::new(b.finish()),
            Builder::Int64(b) => Arc::new(b.finish()),
            Builder::Float32(b) => Arc::new(b.finish()),
            Builder::Float64(b) => Arc::new(b.finish()),
            Builder::Utf8(b) => Arc::new(b.finish()),
            Builder::Binary(b) => Arc::new(b.finish()),
            Builder::FixedBinary(b) => Arc::new(b.finish()),
            Builder::Decimal128(b) => Arc::new(b.finish()),
            Builder::Date32(b) => Arc::new(b.finish()),
            Builder::Time32Second(b) => Arc::new(b.finish()),
            Builder::Time32Millisecond(b) => Arc::new(b.finish()),
            Builder::Time64Microsecond(b) => Arc::new(b.finish()),
            Builder::Time64Nanosecond(b) => Arc::new(b.finish()),
            Builder::TimestampSecond(b) => Arc::new(b.finish()),
            Builder::TimestampMillisecond(b) => Arc::new(b.finish()),
            Builder::TimestampMicrosecond(b) => Arc::new(b.finish()),
            Builder::TimestampNanosecond(b) => Arc::new(b.finish()),
        }
    }
}

pub struct Rows {
    schema: SchemaRef,
    types: Vec<DataType>,
    columns: Vec<Builder>,
    len: usize,
}

impl Rows {
    pub fn new(fields: &Fields, capacity: usize) -> Result<Self, Error> {
        let types: Vec<DataType> = fields.iter().map(|f| f.data_type().clone()).collect();
        let columns = types
            .iter()
            .map(|t| Builder::new(t, capacity))
            .collect::<Result<_, _>>()?;

        Ok(Rows {
            schema: Arc::new(ArrowSchema::from(fields)),
            types,
            columns,
            len: 0,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push(&mut self, row: &[Option<Scalar<'_>>]) -> Result<(), Error> {
        if row.len() != self.types.len() {
            return Err(Error::FieldCount {
                expected: self.types.len(),
                found: row.len(),
            });
        }
        for ((builder, data_type), value) in self.columns.iter_mut().zip(&self.types).zip(row) {
            builder.append(data_type, *value)?;
        }
        self.len += 1;

        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch, Error> {
        let columns = self.columns.iter_mut().map(Builder::finish).collect();
        self.len = 0;

        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|e| Error::Ipc(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use mink_types::{Decimal, Field, Precision};

    use super::*;
    use crate::{Reader, Timestamp};

    fn fields() -> Fields {
        Fields::new(vec![
            Field::new("id", DataType::big_int().with_nullable(false)).unwrap(),
            Field::new("name", DataType::string()).unwrap(),
            Field::new("amount", DataType::decimal(Decimal::new(10, 2).unwrap())).unwrap(),
            Field::new("ts", DataType::timestamp_ltz(Precision::new(6).unwrap())).unwrap(),
            Field::new("t", DataType::time(Precision::new(3).unwrap())).unwrap(),
        ])
        .unwrap()
    }

    #[test]
    fn round_trips_through_reader() {
        let fields = fields();
        let rows: Vec<Vec<Option<Scalar<'_>>>> = vec![
            vec![
                Some(Scalar::BigInt(1)),
                Some(Scalar::String("a")),
                Some(Scalar::Decimal {
                    unscaled: 150,
                    precision: 10,
                }),
                Some(Scalar::Timestamp {
                    at: Timestamp {
                        millis: 1_000,
                        nanos: 5_000,
                    },
                    precision: 6,
                }),
                Some(Scalar::Time(3_600 * 1_000_000_000)),
            ],
            vec![Some(Scalar::BigInt(2)), None, None, None, None],
        ];
        let mut builder = Rows::new(&fields, 2).unwrap();
        for row in &rows {
            builder.push(row).unwrap();
        }
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(builder.is_empty());

        let readers: Vec<Reader<'_>> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| Reader::new(batch.column(i), f.data_type(), f.name()).unwrap())
            .collect();
        for (r, row) in rows.iter().enumerate() {
            let read: Vec<_> = readers.iter().map(|reader| reader.get(r)).collect();
            assert_eq!(&read, row);
        }
    }

    #[test]
    fn rejects_rows_of_the_wrong_width() {
        let fields = fields();
        let mut builder = Rows::new(&fields, 1).unwrap();
        assert!(matches!(
            builder.push(&[Some(Scalar::BigInt(1))]),
            Err(Error::FieldCount { .. })
        ));
        assert!(matches!(
            builder.push(&[None; 6]),
            Err(Error::FieldCount { .. })
        ));
    }

    #[test]
    fn rejects_wrong_scalar() {
        let mut builder = Builder::new(&DataType::int(), 1).unwrap();
        assert!(matches!(
            builder.append(&DataType::int(), Some(Scalar::BigInt(1))),
            Err(Error::ScalarType { .. })
        ));
    }
}
