use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
    Time64MicrosecondType, TimestampMicrosecondType,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use iceberg::spec::{PrimitiveType, Schema, Type};
use mink_conformance::{ExpectedMerge, PayloadCase, PayloadFixture, payload_fixture};
use mink_core::{Part, Partition, Record};
use mink_schema::{
    Decoders, FieldIds, Merge, PayloadSchema, Rejection, SchemaFormat, SchemaKey, SchemaRef,
    SchemaSet, accept, binary_schema, merge, schema_refs, subject,
};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::rows::rows;
use crate::schema::{FileSchema, TypedSchema};

struct FixtureTable {
    steps: Vec<Step>,
    decoders: Decoders,
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
    let mut merged_by_id: HashMap<(Part, i32), Result<Merge, Rejection>> = HashMap::new();
    let mut steps = Vec::new();

    for step in case.merges() {
        let (part, id) = (step.part(), step.id());
        let subjects = fixture.schema(id).unwrap().subjects();
        let registered_under_subject = subjects.contains(&subject(case.topic(), part));

        let payload = match schema_set(fixture, id).await {
            Some(set) => accept(&set, registered_under_subject),
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

        merged_by_id.insert((part, id), merged.clone());

        steps.push(Step {
            merged,
            table: latest.clone(),
        });
    }

    let mut decoders = Decoders::default();

    for schema_ref in schema_refs(&records(case)) {
        let id = match schema_ref.schema_ref() {
            SchemaRef::ConfluentId(id) => Some(id),
            SchemaRef::ConfluentGuid(guid) => fixture.guid(guid),
        };

        match id.and_then(|id| merged_by_id.get(&(schema_ref.part(), id))) {
            Some(Ok(merged)) => {
                let payload = payloads[&merged.registry_key()].clone();

                decoders.insert_typed(schema_ref, payload, merged.field_ids().clone());
            }
            _ => decoders.insert_raw(schema_ref),
        }
    }

    FixtureTable { steps, decoders }
}

async fn schema_set(fixture: &PayloadFixture, id: i32) -> Option<SchemaSet> {
    let found = |key| {
        let schema = match key {
            SchemaKey::Confluent(id) => {
                fixture.schema(id).and_then(|entry| entry.schema()).cloned()
            }
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
