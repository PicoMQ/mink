use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, NaiveDate, NaiveTime, SecondsFormat};
use iceberg::spec::{Datum, NestedFieldRef, PrimitiveType, Schema, Type};
use mink_core::Record;
use mink_iceberg::Decoded;
use mink_schema::{
    ColumnValue, HEADERS_ID, KEY_ID, OFFSET_ID, PARTITION_ID, PayloadValue, TIMESTAMP_ID, VALUE_ID,
};
use serde_json::{Map, Value, json};

const SHOWN_BYTES: usize = 4096;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MILLI: i64 = 1000;

pub(super) fn bytes(data: Option<&[u8]>) -> Value {
    let Some(data) = data else {
        return Value::Null;
    };

    let readable = std::str::from_utf8(data).ok().filter(|text| {
        !text
            .chars()
            .any(|char| char.is_control() && !matches!(char, '\t' | '\n' | '\r'))
    });

    let (text, encoding, shown) = match readable {
        Some(text) => {
            let end = (0..=text.len().min(SHOWN_BYTES))
                .rev()
                .find(|end| text.is_char_boundary(*end))
                .unwrap_or(0);

            (text[..end].to_owned(), "utf8", end)
        }
        None => {
            let end = data.len().min(SHOWN_BYTES);

            (STANDARD.encode(&data[..end]), "base64", end)
        }
    };

    json!({
        "data": text,
        "encoding": encoding,
        "bytes": data.len(),
        "truncated": shown < data.len(),
    })
}

pub(super) fn row(schema: &Schema, partition: i32, record: &Record, decoded: &Decoded) -> Value {
    let mut row = Map::new();

    for field in schema.as_struct().fields() {
        let value = match field.id {
            PARTITION_ID => json!(partition),
            OFFSET_ID => json!(record.offset().to_string()),
            TIMESTAMP_ID => timestamp(record.timestamp_ms().saturating_mul(MICROS_PER_MILLI)),
            KEY_ID => bytes(decoded.key()),
            VALUE_ID => bytes(decoded.value()),
            HEADERS_ID => headers(record),
            id => decoded
                .column(id)
                .map_or(Value::Null, |value| column(&field.field_type, value)),
        };

        row.insert(field.name.clone(), value);
    }

    Value::Object(row)
}

pub(super) fn headers(record: &Record) -> Value {
    record
        .headers()
        .iter()
        .map(|header| json!({ "key": header.key(), "value": bytes(header.value().map(AsRef::as_ref)) }))
        .collect()
}

pub(super) fn bound(field: &NestedFieldRef, bytes: &[u8]) -> Value {
    let Type::Primitive(primitive) = field.field_type.as_ref() else {
        return Value::Null;
    };
    let Ok(datum) = Datum::try_from_bytes(bytes, primitive.clone()) else {
        return Value::Null;
    };

    match PayloadValue::from_literal(primitive, datum.literal()) {
        Some(value) => scalar(primitive, &value),
        None => Value::Null,
    }
}

fn column(field_type: &Type, value: &ColumnValue) -> Value {
    match (field_type, value) {
        (Type::Primitive(primitive), ColumnValue::Scalar(value)) => scalar(primitive, value),
        (Type::Struct(fields), ColumnValue::Struct(children)) => {
            let members = fields.fields().iter().map(|field| {
                let value = children
                    .get(&field.id)
                    .map_or(Value::Null, |child| column(&field.field_type, child));

                (field.name.clone(), value)
            });

            Value::Object(members.collect())
        }
        (Type::List(list), ColumnValue::List(items)) => items
            .iter()
            .map(|item| column(&list.element_field.field_type, item))
            .collect(),
        (Type::Map(map), ColumnValue::Map(entries)) => entries
            .iter()
            .map(|(key, value)| {
                json!({
                    "key": column(&map.key_field.field_type, key),
                    "value": column(&map.value_field.field_type, value),
                })
            })
            .collect(),
        _ => Value::Null,
    }
}

fn scalar(primitive: &PrimitiveType, value: &PayloadValue) -> Value {
    let shown = match primitive {
        PrimitiveType::Boolean => value.as_boolean().map(Value::from),
        PrimitiveType::Int => value.as_int(primitive).map(Value::from),
        PrimitiveType::Long => value.as_long(primitive).map(|long| long.to_string().into()),
        PrimitiveType::Float => value.as_float().map(|float| number(f64::from(float))),
        PrimitiveType::Double => value.as_double().map(number),
        PrimitiveType::Decimal { scale, .. } => value
            .as_decimal()
            .map(|unscaled| decimal(unscaled, *scale).into()),
        PrimitiveType::Date => value.as_int(primitive).map(date),
        PrimitiveType::Time => value.as_long(primitive).map(time),
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            value.as_long(primitive).map(timestamp)
        }
        PrimitiveType::String => value.as_string().map(Value::from),
        PrimitiveType::Uuid => value
            .as_fixed()
            .and_then(|bytes| uuid::Uuid::from_slice(bytes).ok())
            .map(|uuid| uuid.to_string().into()),
        PrimitiveType::Binary => value.as_binary().map(|data| bytes(Some(data))),
        PrimitiveType::Fixed(_) => value.as_fixed().map(|data| bytes(Some(data))),
        _ => None,
    };

    shown.unwrap_or(Value::Null)
}

fn number(value: f64) -> Value {
    if value.is_finite() {
        json!(value)
    } else {
        json!(value.to_string())
    }
}

fn decimal(unscaled: i128, scale: u32) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let sign = if unscaled < 0 { "-" } else { "" };
    let scale = scale as usize;

    if scale == 0 {
        return format!("{sign}{digits}");
    }

    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    format!("{sign}{whole}.{fraction}")
}

fn date(days: i32) -> Value {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a date");

    epoch
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .map_or(Value::Null, |date| {
            date.format("%Y-%m-%d").to_string().into()
        })
}

fn time(micros: i64) -> Value {
    let seconds = u32::try_from(micros.div_euclid(MICROS_PER_SECOND)).ok();
    let nanos = u32::try_from(micros.rem_euclid(MICROS_PER_SECOND) * 1000).ok();

    seconds
        .zip(nanos)
        .and_then(|(seconds, nanos)| NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos))
        .map_or(Value::Null, |time| {
            time.format("%H:%M:%S%.6f").to_string().into()
        })
}

fn timestamp(micros: i64) -> Value {
    DateTime::from_timestamp_micros(micros).map_or(Value::Null, |time| {
        time.to_rfc3339_opts(SecondsFormat::Micros, true).into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_written_as_the_admin_api_says() {
        let long = PrimitiveType::Long;
        let money = PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        };
        let moment = PrimitiveType::Timestamptz;

        assert_eq!(
            scalar(&long, &PayloadValue::Long(9_007_199_254_740_993)),
            json!("9007199254740993")
        );
        assert_eq!(scalar(&money, &PayloadValue::Decimal(-5)), json!("-0.05"));
        assert_eq!(
            scalar(&money, &PayloadValue::Decimal(12345)),
            json!("123.45")
        );
        assert_eq!(
            scalar(&PrimitiveType::Date, &PayloadValue::Date(19_000)),
            json!("2022-01-08")
        );
        assert_eq!(
            scalar(&moment, &PayloadValue::Timestamp(1_500_001)),
            json!("1970-01-01T00:00:01.500001Z")
        );
        assert_eq!(
            scalar(&PrimitiveType::Time, &PayloadValue::Time(3_600_000_001)),
            json!("01:00:00.000001")
        );
        assert_eq!(
            scalar(&PrimitiveType::Double, &PayloadValue::Double(f64::NAN)),
            json!("NaN")
        );
        assert_eq!(
            scalar(&long, &PayloadValue::String("x".to_owned())),
            Value::Null
        );
    }

    #[test]
    fn bytes_show_as_readable_text_or_base64_and_at_most_4_kib() {
        let long = vec![b'a'; 5000];

        assert_eq!(bytes(None), Value::Null);
        assert_eq!(
            bytes(Some(b"hi")),
            json!({"data": "hi", "encoding": "utf8", "bytes": 2, "truncated": false})
        );
        assert_eq!(
            bytes(Some(b"a\tb\r\n")),
            json!({"data": "a\tb\r\n", "encoding": "utf8", "bytes": 5, "truncated": false})
        );
        assert_eq!(
            bytes(Some(&[0xff, 0x00])),
            json!({"data": "/wA=", "encoding": "base64", "bytes": 2, "truncated": false})
        );
        assert_eq!(
            bytes(Some(&[0, 0, 0, 0, 4, b'{', b'}'])),
            json!({"data": "AAAAAAR7fQ==", "encoding": "base64", "bytes": 7, "truncated": false})
        );
        assert_eq!(bytes(Some(&long))["truncated"], json!(true));
        assert_eq!(bytes(Some(&long))["data"].as_str().unwrap().len(), 4096);
    }
}
