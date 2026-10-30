use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
    Time64MicrosecondType, TimestampMicrosecondType,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use bytes::Bytes;
use iceberg::spec::{PrimitiveType, Schema, Type};
use mink_conformance::{ExpectedMerge, PayloadCase, PayloadFixture, glue_fixture, payload_fixture};
use mink_core::{Part, Partition, Record};
use mink_schema::{
    Decoders, FieldIds, Merge, PartSchemaId, PayloadSchema, RegistrySchema, Rejection,
    SchemaFormat, SchemaId, SchemaKey, SchemaSet, binary_schema, merge, schema_ids, subject,
};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::rows::rows;
use crate::schema::{FileSchema, TypedSchema};

struct FixtureTable {
    steps: Vec<Step>,
    decoders: Decoders,
    typed: HashMap<(Part, i32), (Arc<PayloadSchema>, FieldIds)>,
}

struct Step {
    merged: Result<Merge, Rejection>,
    table: Schema,
}

impl FixtureTable {
    fn latest(&self) -> &Schema {
        self.steps
            .last()
            .map_or_else(|| binary_schema(), |step| &step.table)
    }

    fn insert_glue_decoder(
        &mut self,
        fixture: &PayloadFixture,
        part: Part,
        version: Uuid,
        id: i32,
    ) {
        let key = SchemaKey::Glue(version);
        let confluent = fixture.schema(id).and_then(|entry| entry.schema()).unwrap();
        let schema =
            RegistrySchema::new(key, confluent.format(), confluent.definition(), Vec::new());
        let payload = PayloadSchema::parse(&SchemaSet::new(schema, Vec::new())).unwrap();
        let (_, field_ids) = &self.typed[&(part, id)];

        self.decoders.insert_typed(
            PartSchemaId::new(part, SchemaId::Key(key)),
            Arc::new(payload),
            field_ids.clone(),
        );
    }

    fn rows(self, records: &[Record]) -> RecordBatch {
        let latest = self.latest().clone();
        let typed = TypedSchema::new(latest, self.decoders).unwrap();
        let partition = Partition::new("payload", 0).unwrap();

        rows(&partition, records, &FileSchema::Typed(Box::new(typed))).unwrap()
    }
}

async fn merge_fixture(fixture: &PayloadFixture, case: &PayloadCase) -> FixtureTable {
    let mut latest = binary_schema().clone();
    let mut identity_field_ids: HashMap<Part, FieldIds> = HashMap::new();
    let mut payloads: HashMap<SchemaKey, Arc<PayloadSchema>> = HashMap::new();
    let mut typed: HashMap<(Part, i32), (Arc<PayloadSchema>, FieldIds)> = HashMap::new();
    let mut steps = Vec::new();

    for step in case.merges() {
        let (part, id) = (step.part(), step.id());
        let subjects = fixture.schema(id).unwrap().subjects();
        let registered_under_subject = subjects.contains(&subject(case.topic(), part));

        let payload = match schema_set(fixture, id).await {
            Some(_) if !registered_under_subject => Err(Rejection::NotAccepted),
            Some(set) => PayloadSchema::parse(&set).map_err(|_| Rejection::Unsupported),
            None => Err(Rejection::Missing),
        };
        let ids = identity_field_ids.entry(part).or_default();
        let merged = payload.and_then(|payload| {
            let key = SchemaKey::Confluent(id);
            let merged = merge(&latest, ids, part, &payload, key);

            payloads.insert(key, Arc::new(payload));

            merged
        });

        if let Ok(merged) = &merged {
            if let Some(schema) = merged.schema() {
                latest = schema.clone();
            }

            *ids = merged.identity_field_ids().clone();
        }

        if let Ok(merged) = &merged {
            let payload = payloads[&merged.registry_key()].clone();

            typed.insert((part, id), (payload, merged.field_ids().clone()));
        }

        steps.push(Step {
            merged,
            table: latest.clone(),
        });
    }

    let mut decoders = Decoders::default();

    for schema_id in schema_ids(&records(case)) {
        let id = match schema_id.schema_id() {
            SchemaId::Key(SchemaKey::Confluent(id)) => Some(id),
            SchemaId::ConfluentGuid(guid) => fixture.guid(guid),
            SchemaId::Key(SchemaKey::Glue(_)) => None,
        };

        match id.and_then(|id| typed.get(&(schema_id.part(), id))) {
            Some((payload, field_ids)) => {
                decoders.insert_typed(schema_id, payload.clone(), field_ids.clone());
            }
            None => decoders.insert_raw(schema_id),
        }
    }

    FixtureTable {
        steps,
        decoders,
        typed,
    }
}

async fn schema_set(fixture: &PayloadFixture, id: i32) -> Option<SchemaSet> {
    let found = |key| {
        let schema = match key {
            SchemaKey::Confluent(id) => {
                fixture.schema(id).and_then(|entry| entry.schema()).cloned()
            }
            SchemaKey::Glue(_) => unreachable!("payload fixtures name Confluent schemas"),
        };

        async move { schema.ok_or(()) }
    };

    SchemaSet::load(SchemaKey::Confluent(id), found).await.ok()
}

fn records(case: &PayloadCase) -> Vec<Record> {
    case.records()
        .iter()
        .map(|record| record.record().clone())
        .collect()
}

fn glue_record(source: &Record, part: Part, bytes: &Bytes) -> Record {
    let named = format!("__{}_schema_id", part.name());
    let headers = source
        .headers()
        .iter()
        .filter(|header| header.key() != named)
        .cloned()
        .collect();
    let (key, value) = match part {
        Part::Key => (Some(bytes.clone()), source.value().cloned()),
        Part::Value => (source.key().cloned(), Some(bytes.clone())),
    };

    Record::new(source.offset(), source.timestamp_ms(), key, value, headers).unwrap()
}

async fn assert_glue_rows(format: SchemaFormat) {
    let fixture = payload_fixture(format);
    let glue = glue_fixture(format);

    for case in fixture.cases() {
        let written: Vec<_> = glue
            .records()
            .iter()
            .filter(|record| record.case() == case.name())
            .collect();
        if written.is_empty() {
            continue;
        }

        let mut table = merge_fixture(&fixture, case).await;
        for record in &written {
            table.insert_glue_decoder(
                &fixture,
                record.part(),
                record.version(),
                record.confluent_id(),
            );
        }
        let records: Vec<Record> = written
            .iter()
            .map(|record| {
                let source = case.records()[record.record()].record();
                glue_record(source, record.part(), record.bytes())
            })
            .collect();
        let schema = table.latest().clone();

        let rows = table.rows(&records);

        for (index, record) in written.iter().enumerate() {
            let actual = row(&rows, &schema, index);
            let expected = case.records()[record.record()].row();

            for (column, expected) in expected.as_object().unwrap() {
                assert_eq!(
                    &actual[column],
                    expected,
                    "{} record {} {} column {column}",
                    case.name(),
                    record.record(),
                    record.part().name(),
                );
            }
        }
    }
}

async fn assert_tables(format: SchemaFormat) {
    let fixture = payload_fixture(format);

    for case in fixture.cases() {
        let table = merge_fixture(&fixture, case).await;

        for (merge, step) in case.merges().iter().zip(&table.steps) {
            match merge.expected() {
                ExpectedMerge::Table(expected) => {
                    let expected: Schema = serde_json::from_value(expected.clone()).unwrap();

                    assert!(step.merged.is_ok(), "{}: {:?}", case.name(), step.merged);
                    assert_eq!(step.table, expected, "{}", case.name());
                }
                ExpectedMerge::Rejected(rejection) => {
                    assert_eq!(step.merged, Err(*rejection), "{}", case.name());
                }
            }
        }
    }
}

async fn assert_rows(format: SchemaFormat) {
    let fixture = payload_fixture(format);

    for case in fixture.cases() {
        let table = merge_fixture(&fixture, case).await;
        let schema = table.latest().clone();

        let rows = table.rows(&records(case));

        for (index, record) in case.records().iter().enumerate() {
            let actual = row(&rows, &schema, index);

            for (column, expected) in record.row().as_object().unwrap() {
                assert_eq!(
                    &actual[column],
                    expected,
                    "{} record {index} column {column}",
                    case.name()
                );
            }
        }
    }
}

#[tokio::test]
async fn avro_fixtures_merge_to_their_table_schemas() {
    assert_tables(SchemaFormat::Avro).await;
}

#[tokio::test]
async fn avro_fixtures_decode_to_their_rows() {
    assert_rows(SchemaFormat::Avro).await;
}

#[tokio::test]
async fn protobuf_fixtures_merge_to_their_table_schemas() {
    assert_tables(SchemaFormat::Protobuf).await;
}

#[tokio::test]
async fn protobuf_fixtures_decode_to_their_rows() {
    assert_rows(SchemaFormat::Protobuf).await;
}

#[tokio::test]
async fn json_fixtures_merge_to_their_table_schemas() {
    assert_tables(SchemaFormat::Json).await;
}

#[tokio::test]
async fn json_fixtures_decode_to_their_rows() {
    assert_rows(SchemaFormat::Json).await;
}

#[tokio::test]
async fn avro_written_through_glue_decodes_to_the_same_rows() {
    assert_glue_rows(SchemaFormat::Avro).await;
}

#[tokio::test]
async fn protobuf_written_through_glue_decodes_to_the_same_rows() {
    assert_glue_rows(SchemaFormat::Protobuf).await;
}

#[tokio::test]
async fn json_written_through_glue_decodes_to_the_same_rows() {
    assert_glue_rows(SchemaFormat::Json).await;
}

fn row(rows: &RecordBatch, schema: &Schema, index: usize) -> Value {
    let mut row = Map::new();

    for (field, column) in schema.as_struct().fields().iter().zip(rows.columns()) {
        row.insert(field.name.clone(), value(&field.field_type, column, index));
    }

    Value::Object(row)
}

fn value(field_type: &Type, column: &ArrayRef, index: usize) -> Value {
    if column.is_null(index) {
        return Value::Null;
    }

    match field_type {
        Type::Primitive(primitive) => primitive_value(primitive, column, index),
        Type::Struct(structure) => {
            let children = column.as_struct();

            let object = structure
                .fields()
                .iter()
                .zip(children.columns())
                .map(|(field, child)| (field.name.clone(), value(&field.field_type, child, index)))
                .collect();
            Value::Object(object)
        }
        Type::List(list) => {
            let items = column.as_list::<i32>().value(index);

            let values = (0..items.len())
                .map(|item| value(&list.element_field.field_type, &items, item))
                .collect();
            Value::Array(values)
        }
        Type::Map(map) => {
            let entries = column.as_map().value(index);
            let (keys, values) = (entries.column(0), entries.column(1));

            let pairs = (0..entries.len())
                .map(|entry| {
                    json!([
                        value(&map.key_field.field_type, keys, entry),
                        value(&map.value_field.field_type, values, entry),
                    ])
                })
                .collect();
            Value::Array(pairs)
        }
    }
}

fn primitive_value(primitive: &PrimitiveType, column: &ArrayRef, index: usize) -> Value {
    match primitive {
        PrimitiveType::Boolean => json!(column.as_boolean().value(index)),
        PrimitiveType::Int => json!(column.as_primitive::<Int32Type>().value(index)),
        PrimitiveType::Date => json!(column.as_primitive::<Date32Type>().value(index)),
        PrimitiveType::Long => json!(column.as_primitive::<Int64Type>().value(index)),
        PrimitiveType::Time => json!(column.as_primitive::<Time64MicrosecondType>().value(index)),
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            json!(
                column
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(index)
            )
        }
        PrimitiveType::Float => json!(column.as_primitive::<Float32Type>().value(index)),
        PrimitiveType::Double => json!(column.as_primitive::<Float64Type>().value(index)),
        PrimitiveType::Decimal { scale, .. } => {
            let unscaled = column.as_primitive::<Decimal128Type>().value(index);
            let divisor = 10_i128.pow(*scale);

            let fraction = format!(
                "{:0width$}",
                (unscaled % divisor).abs(),
                width = *scale as usize
            );
            match scale {
                0 => json!(unscaled.to_string()),
                _ => json!(format!("{}.{fraction}", unscaled / divisor)),
            }
        }
        PrimitiveType::String => json!(column.as_string::<i32>().value(index)),
        PrimitiveType::Uuid => {
            let bytes = column.as_fixed_size_binary().value(index);

            json!(Uuid::from_slice(bytes).unwrap().to_string())
        }
        PrimitiveType::Fixed(_) => json!(hex::encode(column.as_fixed_size_binary().value(index))),
        PrimitiveType::Binary => json!(hex::encode(column.as_binary::<i64>().value(index))),
        other => panic!("{other} is not written"),
    }
}
