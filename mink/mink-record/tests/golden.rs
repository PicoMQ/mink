//! Checks key encodings against fixtures produced by the JVM key encoders.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    StringArray, Time32MillisecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow_schema::Schema as ArrowSchema;
use mink_record::KeyEncoder;
use mink_table::Bucketing;
use mink_types::{DataType, Field, Fields, Kind};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Fixture {
    columns: Vec<Column>,
    iceberg_supported: Vec<bool>,
    rows: Vec<Row>,
}

#[derive(Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
}

#[derive(Deserialize)]
struct Row {
    values: Vec<Value>,
    compacted: Multi,
    paimon: Multi,
    iceberg: Single,
}

#[derive(Deserialize)]
struct Multi {
    all: String,
    pair: String,
    each: Vec<String>,
}

#[derive(Deserialize)]
struct Single {
    each: Vec<Option<String>>,
}

// `fixtures/keys.json` comes from the Java key encoders; regenerate it from Java only.
fn fixture() -> Fixture {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/keys.json");
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn fields(fixture: &Fixture) -> Fields {
    Fields::new(
        fixture
            .columns
            .iter()
            .map(|column| {
                Field::new(&column.name, column.data_type.parse::<DataType>().unwrap()).unwrap()
            })
            .collect(),
    )
    .unwrap()
}

fn timestamp_micros(value: &Value) -> i64 {
    value["millis"].as_i64().unwrap() * 1_000 + value["nanos"].as_i64().unwrap() / 1_000
}

fn array(data_type: &DataType, values: Vec<&Value>) -> ArrayRef {
    match data_type.kind() {
        Kind::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|v| v.as_bool().unwrap())
                .collect::<Vec<_>>(),
        )),
        Kind::TinyInt => Arc::new(Int8Array::from(
            values
                .iter()
                .map(|v| v.as_i64().unwrap() as i8)
                .collect::<Vec<_>>(),
        )),
        Kind::SmallInt => Arc::new(Int16Array::from(
            values
                .iter()
                .map(|v| v.as_i64().unwrap() as i16)
                .collect::<Vec<_>>(),
        )),
        Kind::Int => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.as_i64().unwrap() as i32)
                .collect::<Vec<_>>(),
        )),
        Kind::BigInt => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|v| v.as_i64().unwrap())
                .collect::<Vec<_>>(),
        )),
        Kind::Float => Arc::new(Float32Array::from(
            values
                .iter()
                .map(|v| v.as_f64().unwrap() as f32)
                .collect::<Vec<_>>(),
        )),
        Kind::Double => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|v| v.as_f64().unwrap())
                .collect::<Vec<_>>(),
        )),
        Kind::Char(_) | Kind::String => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>(),
        )),
        Kind::Bytes => Arc::new(BinaryArray::from_iter_values(
            values
                .iter()
                .map(|v| hex::decode(v.as_str().unwrap()).unwrap()),
        )),
        Kind::Binary(_) => Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                values
                    .iter()
                    .map(|v| hex::decode(v.as_str().unwrap()).unwrap()),
            )
            .unwrap(),
        ),
        Kind::Decimal(decimal) => Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|v| v.as_str().unwrap().parse::<i128>().unwrap())
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(decimal.precision(), decimal.scale() as i8)
            .unwrap(),
        ),
        Kind::Date => Arc::new(Date32Array::from(
            values
                .iter()
                .map(|v| v.as_i64().unwrap() as i32)
                .collect::<Vec<_>>(),
        )),
        Kind::Time(_) => Arc::new(Time32MillisecondArray::from(
            values
                .iter()
                .map(|v| v.as_i64().unwrap() as i32)
                .collect::<Vec<_>>(),
        )),
        Kind::Timestamp(p) if p.get() <= 3 => Arc::new(TimestampMillisecondArray::from(
            values
                .iter()
                .map(|v| v["millis"].as_i64().unwrap())
                .collect::<Vec<_>>(),
        )),
        Kind::Timestamp(_) => Arc::new(TimestampMicrosecondArray::from(
            values
                .iter()
                .map(|v| timestamp_micros(v))
                .collect::<Vec<_>>(),
        )),
        Kind::TimestampLtz(p) if p.get() <= 3 => Arc::new(
            TimestampMillisecondArray::from(
                values
                    .iter()
                    .map(|v| v["millis"].as_i64().unwrap())
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        Kind::TimestampLtz(_) => Arc::new(
            TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|v| timestamp_micros(v))
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        Kind::Array(_) | Kind::Map { .. } | Kind::Row(_) => unreachable!("not in the fixture"),
    }
}

fn batch(fixture: &Fixture, fields: &Fields) -> RecordBatch {
    let columns = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            array(
                field.data_type(),
                fixture.rows.iter().map(|row| &row.values[index]).collect(),
            )
        })
        .collect();
    RecordBatch::try_new(Arc::new(ArrowSchema::from(fields)), columns).unwrap()
}

fn names(fields: &Fields, indices: &[usize]) -> Vec<String> {
    indices
        .iter()
        .map(|&i| fields[i].name().to_owned())
        .collect()
}

fn check(
    fields: &Fields,
    batch: &RecordBatch,
    keys: &[String],
    format: Bucketing,
    expected: impl Fn(usize) -> String,
) {
    let encoder = KeyEncoder::new(fields, keys, format).unwrap();
    let bound = encoder.bind(batch).unwrap();
    for row in 0..batch.num_rows() {
        let got = hex::encode(bound.encode_vec(row).unwrap());
        assert_eq!(got, expected(row), "{format:?} keys={keys:?} row={row}");
    }
}

#[test]
fn every_column_alone_matches_java() {
    let fixture = fixture();
    let fields = fields(&fixture);
    let batch = batch(&fixture, &fields);
    for (index, field) in fields.iter().enumerate() {
        let key = vec![field.name().to_owned()];
        check(&fields, &batch, &key, Bucketing::Native, |row| {
            fixture.rows[row].compacted.each[index].clone()
        });
        check(&fields, &batch, &key, Bucketing::Paimon, |row| {
            fixture.rows[row].paimon.each[index].clone()
        });
        if fixture.iceberg_supported[index] {
            check(&fields, &batch, &key, Bucketing::Iceberg, |row| {
                fixture.rows[row].iceberg.each[index].clone().unwrap()
            });
        }
    }
}

#[test]
fn all_columns_together_match_java() {
    let fixture = fixture();
    let fields = fields(&fixture);
    let batch = batch(&fixture, &fields);
    let all = names(&fields, &(0..fields.len()).collect::<Vec<_>>());
    check(&fields, &batch, &all, Bucketing::Native, |row| {
        fixture.rows[row].compacted.all.clone()
    });
    check(&fields, &batch, &all, Bucketing::Paimon, |row| {
        fixture.rows[row].paimon.all.clone()
    });
}

#[test]
fn inline_and_spilled_strings_share_a_row() {
    let fixture = fixture();
    let fields = fields(&fixture);
    let batch = batch(&fixture, &fields);
    let pair = names(&fields, &[7, 9]);
    check(&fields, &batch, &pair, Bucketing::Native, |row| {
        fixture.rows[row].compacted.pair.clone()
    });
    check(&fields, &batch, &pair, Bucketing::Paimon, |row| {
        fixture.rows[row].paimon.pair.clone()
    });
}

#[test]
fn fixture_exercises_the_boundaries() {
    let fixture = fixture();
    let strings: Vec<usize> = fixture
        .rows
        .iter()
        .map(|row| row.values[7].as_str().unwrap().len())
        .collect();
    assert!(strings.contains(&0), "empty string");
    assert!(strings.contains(&7), "largest inline string");
    assert!(strings.contains(&8), "smallest spilled string");
    assert!(strings.iter().any(|len| *len > 8));
    let negatives = fixture
        .rows
        .iter()
        .filter(|row| row.values[3].as_i64().unwrap() < 0)
        .count();
    assert!(negatives >= 2, "negative varints");
}
