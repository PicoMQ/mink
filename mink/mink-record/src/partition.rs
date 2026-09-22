//! Derives partition values from Arrow rows, rendering each scalar the way the JVM implementation names partitions.

use arrow_array::RecordBatch;
use chrono::DateTime;
use mink_table::{Name, PartitionName, PartitionSpec};
use mink_types::{DataType, Fields, Root};

use crate::scalar::{Reader, Scalar, Timestamp};
use crate::{Error, KeyColumn, key};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionGetter {
    columns: Vec<KeyColumn>,
}

impl PartitionGetter {
    pub fn new(fields: &Fields, keys: &[String]) -> Result<Self, Error> {
        if keys.is_empty() {
            return Err(Error::EmptyKey);
        }
        let columns = key::resolve(fields, keys, supports, |column, data_type| {
            Error::PartitionType { column, data_type }
        })?;

        Ok(PartitionGetter { columns })
    }

    pub fn bind<'a>(&'a self, batch: &'a RecordBatch) -> Result<BoundPartition<'a>, Error> {
        Ok(BoundPartition {
            getter: self,
            readers: key::readers(&self.columns, batch)?,
            rows: batch.num_rows(),
        })
    }
}

fn supports(data_type: &DataType) -> bool {
    matches!(
        data_type.root(),
        Root::Char
            | Root::String
            | Root::Boolean
            | Root::Binary
            | Root::Bytes
            | Root::TinyInt
            | Root::SmallInt
            | Root::Int
            | Root::BigInt
            | Root::Date
            | Root::Time
            | Root::Float
            | Root::Double
            | Root::Timestamp
            | Root::TimestampLtz
    )
}

#[derive(Debug, Clone)]
pub struct BoundPartition<'a> {
    getter: &'a PartitionGetter,
    readers: Vec<Reader<'a>>,
    rows: usize,
}

impl BoundPartition<'_> {
    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn spec(&self, row: usize) -> Result<PartitionSpec, Error> {
        if row >= self.rows {
            return Err(Error::RowIndex {
                row,
                rows: self.rows,
            });
        }

        let entries = self
            .getter
            .columns
            .iter()
            .zip(&self.readers)
            .map(|(column, reader)| {
                let scalar = reader
                    .get(row)
                    .ok_or_else(|| Error::NullKey(column.name.clone()))?;
                let text = render(&scalar);
                let value = Name::new(text.as_str()).map_err(|_| Error::PartitionValue {
                    column: column.name.clone(),
                    value: text,
                })?;
                Ok((column.name.clone(), value))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        PartitionSpec::new(entries).map_err(|e| Error::Projection(e.to_string()))
    }

    pub fn name(&self, row: usize) -> Result<PartitionName, Error> {
        Ok(self.spec(row)?.name())
    }
}

fn render(scalar: &Scalar<'_>) -> String {
    match scalar {
        Scalar::String(s) => (*s).to_owned(),
        Scalar::Boolean(b) => b.to_string(),
        Scalar::Bytes(bytes) => hex(bytes),
        Scalar::TinyInt(v) => v.to_string(),
        Scalar::SmallInt(v) => v.to_string(),
        Scalar::Int(v) => v.to_string(),
        Scalar::BigInt(v) => v.to_string(),
        Scalar::Date(days) => day_to_string(*days),
        Scalar::Time(nanos) => milli_to_string((nanos / 1_000_000) as i32),
        Scalar::Float(v) => java_float(f64::from(*v), true),
        Scalar::Double(v) => java_float(*v, false),
        Scalar::Timestamp { at, .. } => timestamp_to_string(at),
        Scalar::Decimal { .. } => unreachable!("decimal is not a partition type"),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn day_to_string(days: i32) -> String {
    let date = DateTime::from_timestamp(i64::from(days) * 86_400, 0)
        .expect("day offsets fit in chrono's range")
        .date_naive();
    date.format("%Y-%m-%d").to_string()
}

fn milli_to_string(milli: i32) -> String {
    let hour = milli.div_euclid(3_600_000);
    let min = milli.rem_euclid(3_600_000).div_euclid(60_000);
    let sec = milli.rem_euclid(60_000).div_euclid(1_000);
    format!("{hour:02}-{min:02}-{sec:02}_{:03}", milli.rem_euclid(1_000))
}

fn timestamp_to_string(at: &Timestamp) -> String {
    let datetime = DateTime::from_timestamp_millis(at.millis)
        .expect("millis fit in chrono's range")
        .naive_utc();
    let mut out = datetime.format("%Y-%m-%d-%H-%M-%S_").to_string();
    let nanos = (at.millis.rem_euclid(1_000) as u32) * 1_000_000 + at.nanos;
    if nanos != 0 {
        let digits = format!("{nanos:09}");
        out.push_str(digits.trim_end_matches('0'));
    }

    out
}

// Java `Float.toString` / `Double.toString` with `.` as `_`: plain decimal in 1e-3..1e7, else `d.dddE±n`.
fn java_float(v: f64, single: bool) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Inf" } else { "-Inf" }.into();
    }
    let digits = |v: f64| -> String {
        if single {
            format!("{:e}", v as f32)
        } else {
            format!("{v:e}")
        }
    };
    let magnitude = v.abs();
    let text = if v == 0.0 {
        if v.is_sign_negative() { "-0.0" } else { "0.0" }.to_owned()
    } else if (1e-3..1e7).contains(&magnitude) {
        let plain = if single {
            format!("{}", v as f32)
        } else {
            format!("{v}")
        };
        if plain.contains('.') {
            plain
        } else {
            format!("{plain}.0")
        }
    } else {
        let scientific = digits(v);
        let (mantissa, exponent) = scientific
            .split_once('e')
            .expect("`{:e}` always has an exponent");
        let mantissa = if mantissa.contains('.') {
            mantissa.to_owned()
        } else {
            format!("{mantissa}.0")
        };
        format!("{mantissa}E{exponent}")
    };

    text.replace('.', "_")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
        StringArray, Time64NanosecondArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
    use mink_types::{Decimal, Field, Precision};

    use super::*;

    fn keys(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn java_float_formatting() {
        assert_eq!(java_float(1.5, true), "1_5");
        assert_eq!(java_float(1.0, true), "1_0");
        assert_eq!(java_float(100.0, false), "100_0");
        assert_eq!(java_float(0.001, false), "0_001");
        assert_eq!(java_float(0.0001, false), "1_0E-4");
        assert_eq!(java_float(1e10, false), "1_0E10");
        assert_eq!(java_float(12_345_678.0, true), "1_2345678E7");
        assert_eq!(java_float(9_999_999.0, false), "9999999_0");
        assert_eq!(java_float(-2.5e-5, false), "-2_5E-5");
        assert_eq!(java_float(0.0, false), "0_0");
        assert_eq!(java_float(-0.0, false), "-0_0");
        assert_eq!(java_float(f64::NAN, false), "NaN");
        assert_eq!(java_float(f64::INFINITY, true), "Inf");
        assert_eq!(java_float(f64::NEG_INFINITY, false), "-Inf");
    }

    #[test]
    fn temporal_formatting() {
        assert_eq!(day_to_string(0), "1970-01-01");
        assert_eq!(day_to_string(19_723), "2024-01-01");
        assert_eq!(day_to_string(-1), "1969-12-31");
        assert_eq!(milli_to_string(0), "00-00-00_000");
        assert_eq!(milli_to_string(3_600_000 + 61_000 + 5), "01-01-01_005");
        let at = |millis, nanos| Timestamp { millis, nanos };
        assert_eq!(timestamp_to_string(&at(0, 0)), "1970-01-01-00-00-00_");
        assert_eq!(
            timestamp_to_string(&at(1_704_067_200_123, 0)),
            "2024-01-01-00-00-00_123"
        );
        assert_eq!(
            timestamp_to_string(&at(1_704_067_200_123, 450_000)),
            "2024-01-01-00-00-00_12345"
        );
    }

    #[test]
    fn rows_resolve_to_names_and_nulls_are_rejected() {
        let fields = Fields::new(vec![
            Field::new("region", DataType::string()).unwrap(),
            Field::new("day", DataType::date()).unwrap(),
            Field::new("n", DataType::int()).unwrap(),
            Field::new("big", DataType::big_int()).unwrap(),
            Field::new("ok", DataType::boolean()).unwrap(),
            Field::new("raw", DataType::bytes()).unwrap(),
            Field::new("f", DataType::float()).unwrap(),
            Field::new("d", DataType::double()).unwrap(),
            Field::new("t", DataType::time(Precision::NANOS)).unwrap(),
            Field::new("ts", DataType::timestamp(Precision::MICROS)).unwrap(),
            Field::new("amount", DataType::decimal(Decimal::new(10, 2).unwrap())).unwrap(),
        ])
        .unwrap();
        let schema = Arc::new(ArrowSchema::from(&fields));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("us"), None])),
                Arc::new(Date32Array::from(vec![19_723, 0])),
                Arc::new(Int32Array::from(vec![-7, 0])),
                Arc::new(Int64Array::from(vec![1 << 40, 0])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(BinaryArray::from(vec![&b"\x00\xff\x10"[..], &b""[..]])),
                Arc::new(Float32Array::from(vec![1.5, 0.0])),
                Arc::new(Float64Array::from(vec![1e10, 0.0])),
                Arc::new(Time64NanosecondArray::from(vec![3_661_005_000_000, 0])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_704_067_200_123_450,
                    0,
                ])),
                Arc::new(
                    arrow_array::Decimal128Array::from(vec![100, 0])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();

        let getter = PartitionGetter::new(
            &fields,
            &keys(&[
                "region", "day", "n", "big", "ok", "raw", "f", "d", "t", "ts",
            ]),
        )
        .unwrap();
        let bound = getter.bind(&batch).unwrap();
        assert_eq!(
            bound.name(0).unwrap().as_str(),
            "us$2024-01-01$-7$1099511627776$true$00ff10$1_5$1_0E10$01-01-01_005$2024-01-01-00-00-00_12345"
        );
        assert_eq!(bound.name(1).unwrap_err(), Error::NullKey("region".into()));
        assert_eq!(
            bound.name(2).unwrap_err(),
            Error::RowIndex { row: 2, rows: 2 }
        );

        let raw = PartitionGetter::new(&fields, &keys(&["raw"])).unwrap();
        assert!(matches!(
            raw.bind(&batch).unwrap().name(1).unwrap_err(),
            Error::PartitionValue { column, .. } if column == "raw"
        ));

        assert!(matches!(
            PartitionGetter::new(&fields, &keys(&["amount"])).unwrap_err(),
            Error::PartitionType { column, .. } if column == "amount"
        ));
        assert_eq!(
            PartitionGetter::new(&fields, &[]).unwrap_err(),
            Error::EmptyKey
        );
        assert_eq!(
            PartitionGetter::new(&fields, &keys(&["nope"])).unwrap_err(),
            Error::UnknownColumn("nope".into())
        );
    }

    #[test]
    fn projected_batches_are_rejected() {
        let fields = Fields::new(vec![
            Field::new("a", DataType::int()).unwrap(),
            Field::new("b", DataType::int()).unwrap(),
        ])
        .unwrap();
        let getter = PartitionGetter::new(&fields, &keys(&["b"])).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "a",
                arrow_schema::DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        assert_eq!(
            getter.bind(&batch).unwrap_err(),
            Error::ColumnIndex { index: 1, found: 1 }
        );
    }
}
