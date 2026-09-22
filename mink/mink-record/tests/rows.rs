//! Checks compacted row encoding against fixtures produced by the JVM row encoder.

use std::fs;
use std::path::Path;

use mink_record::{CompactedRow, RowCodec, Scalar, Timestamp};
use mink_types::{DataType, Field, Fields, Kind};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Fixture {
    columns: Vec<Column>,
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
    compacted: String,
}

// `fixtures/rows.json` comes from the Java compacted row encoder; regenerate it from Java only.
fn fixture() -> Fixture {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rows.json");
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

enum Owned {
    Text(String),
    Blob(Vec<u8>),
    None,
}

fn owned(data_type: &DataType, value: &Value) -> Owned {
    match data_type.kind() {
        _ if value.is_null() => Owned::None,
        Kind::Char(_) | Kind::String => Owned::Text(value.as_str().unwrap().to_owned()),
        Kind::Bytes | Kind::Binary(_) => Owned::Blob(hex::decode(value.as_str().unwrap()).unwrap()),
        _ => Owned::None,
    }
}

fn scalar<'a>(data_type: &DataType, value: &Value, owned: &'a Owned) -> Option<Scalar<'a>> {
    if value.is_null() {
        return None;
    }
    let timestamp = |precision: u8| Scalar::Timestamp {
        at: Timestamp {
            millis: value["millis"].as_i64().unwrap(),
            nanos: value["nanos"].as_u64().unwrap() as u32,
        },
        precision,
    };
    Some(match (data_type.kind(), owned) {
        (Kind::Boolean, _) => Scalar::Boolean(value.as_bool().unwrap()),
        (Kind::TinyInt, _) => Scalar::TinyInt(value.as_i64().unwrap() as i8),
        (Kind::SmallInt, _) => Scalar::SmallInt(value.as_i64().unwrap() as i16),
        (Kind::Int, _) => Scalar::Int(value.as_i64().unwrap() as i32),
        (Kind::BigInt, _) => Scalar::BigInt(value.as_i64().unwrap()),
        (Kind::Float, _) => Scalar::Float(value.as_f64().unwrap() as f32),
        (Kind::Double, _) => Scalar::Double(value.as_f64().unwrap()),
        (Kind::Char(_) | Kind::String, Owned::Text(s)) => Scalar::String(s),
        (Kind::Bytes | Kind::Binary(_), Owned::Blob(b)) => Scalar::Bytes(b),
        (Kind::Decimal(d), _) => Scalar::Decimal {
            unscaled: value.as_str().unwrap().parse().unwrap(),
            precision: d.precision(),
        },
        (Kind::Date, _) => Scalar::Date(value.as_i64().unwrap() as i32),
        (Kind::Time(_), _) => Scalar::Time(value.as_i64().unwrap() * 1_000_000),
        (Kind::Timestamp(p) | Kind::TimestampLtz(p), _) => timestamp(p.get()),
        _ => unreachable!("not in the fixture"),
    })
}

#[test]
fn compacted_rows_match_java() {
    let fixture = fixture();
    let fields = fields(&fixture);
    let codec = CompactedRow::new(&fields).unwrap();
    for (index, row) in fixture.rows.iter().enumerate() {
        let owned: Vec<Owned> = fields
            .iter()
            .zip(&row.values)
            .map(|(f, v)| self::owned(f.data_type(), v))
            .collect();
        let scalars: Vec<Option<Scalar<'_>>> = fields
            .iter()
            .zip(&row.values)
            .zip(&owned)
            .map(|((f, v), o)| scalar(f.data_type(), v, o))
            .collect();

        let mut encoded = Vec::new();
        codec.encode(&scalars, &mut encoded).unwrap();
        assert_eq!(hex::encode(&encoded), row.compacted, "encode row {index}");

        let bytes = hex::decode(&row.compacted).unwrap();
        let decoded = codec.decode(&bytes).unwrap();
        assert_eq!(decoded, scalars, "decode row {index}");
    }
}
