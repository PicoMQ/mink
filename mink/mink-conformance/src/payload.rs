use std::fs;

use bytes::Bytes;
use mink_core::{Header, Part, Record};
use mink_schema::{Reference, RegistrySchema, Rejection, SchemaFormat, SchemaKey};
use serde_json::Value;
use uuid::Uuid;

use crate::fixtures;

pub struct PayloadFixture {
    registry: Vec<RegistryFixture>,
    guids: Vec<GuidFixture>,
    cases: Vec<PayloadCase>,
}

pub struct RegistryFixture {
    id: i32,
    schema: Option<RegistrySchema>,
    subjects: Vec<String>,
}

pub struct GuidFixture {
    guid: Uuid,
    id: Option<i32>,
}

pub struct PayloadCase {
    name: String,
    topic: String,
    merges: Vec<MergeStep>,
    records: Vec<PayloadRecord>,
}

pub struct MergeStep {
    part: Part,
    id: i32,
    expected: ExpectedMerge,
}

pub enum ExpectedMerge {
    Table(Value),
    Rejected(Rejection),
}

pub struct PayloadRecord {
    record: Record,
    row: Value,
}

impl PayloadFixture {
    pub fn guid(&self, guid: Uuid) -> Option<i32> {
        self.guids
            .iter()
            .find(|fixture| fixture.guid == guid)
            .and_then(|fixture| fixture.id)
    }

    pub fn cases(&self) -> &[PayloadCase] {
        &self.cases
    }

    pub fn schema(&self, id: i32) -> Option<&RegistryFixture> {
        self.registry.iter().find(|entry| entry.id == id)
    }
}

impl RegistryFixture {
    pub fn schema(&self) -> Option<&RegistrySchema> {
        self.schema.as_ref()
    }

    pub fn subjects(&self) -> &[String] {
        &self.subjects
    }
}

impl PayloadCase {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn merges(&self) -> &[MergeStep] {
        &self.merges
    }

    pub fn records(&self) -> &[PayloadRecord] {
        &self.records
    }
}

impl MergeStep {
    pub fn part(&self) -> Part {
        self.part
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn expected(&self) -> &ExpectedMerge {
        &self.expected
    }
}

impl PayloadRecord {
    pub fn record(&self) -> &Record {
        &self.record
    }

    pub fn row(&self) -> &Value {
        &self.row
    }
}

pub fn payload_fixture(format: SchemaFormat) -> PayloadFixture {
    let path = fixtures("payload").join(format.name()).join("cases.json");
    let json: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();

    let registry = json["registry"]
        .as_array()
        .unwrap()
        .iter()
        .map(registry_fixture)
        .collect();
    let guids = json["guids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|guid| GuidFixture {
            guid: Uuid::parse_str(guid["guid"].as_str().unwrap()).unwrap(),
            id: guid["id"].as_i64().map(|id| i32::try_from(id).unwrap()),
        })
        .collect();
    let cases = json["cases"]
        .as_array()
        .unwrap()
        .iter()
        .map(payload_case)
        .collect();

    PayloadFixture {
        registry,
        guids,
        cases,
    }
}

fn registry_fixture(json: &Value) -> RegistryFixture {
    let id = i32::try_from(json["id"].as_i64().unwrap()).unwrap();
    let subjects = json["subjects"]
        .as_array()
        .map(|subjects| {
            subjects
                .iter()
                .map(|subject| subject.as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default();

    let references = json["references"]
        .as_array()
        .map(|references| {
            references
                .iter()
                .map(|reference| {
                    let id = i32::try_from(reference["id"].as_i64().unwrap()).unwrap();

                    Reference::new(
                        reference["name"].as_str().unwrap(),
                        SchemaKey::Confluent(id),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let schema = json["definition"].as_str().map(|definition| {
        let format = SchemaFormat::from_name(json["format"].as_str().unwrap()).unwrap();

        RegistrySchema::new(SchemaKey::Confluent(id), format, definition, references)
    });

    RegistryFixture {
        id,
        schema,
        subjects,
    }
}

fn payload_case(json: &Value) -> PayloadCase {
    let merges = json["merges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|merge| {
            let expected = match merge["rejection"].as_str() {
                Some(rejection) => {
                    ExpectedMerge::Rejected(Rejection::from_name(rejection).unwrap())
                }
                None => ExpectedMerge::Table(merge["table"].clone()),
            };

            MergeStep {
                part: Part::from_name(merge["part"].as_str().unwrap()).unwrap(),
                id: i32::try_from(merge["id"].as_i64().unwrap()).unwrap(),
                expected,
            }
        })
        .collect();
    let records = json["records"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(offset, record)| payload_record(offset as i64, record))
        .collect();

    PayloadCase {
        name: json["name"].as_str().unwrap().to_owned(),
        topic: json["topic"].as_str().unwrap().to_owned(),
        merges,
        records,
    }
}

fn payload_record(offset: i64, json: &Value) -> PayloadRecord {
    let bytes = |value: &Value| {
        value
            .as_str()
            .map(|hex| Bytes::from(hex::decode(hex).unwrap()))
    };
    let headers = json["headers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|header| Header::new(header[0].as_str().unwrap(), bytes(&header[1])))
        .collect();

    let record = Record::new(
        offset,
        0,
        bytes(&json["key"]),
        bytes(&json["value"]),
        headers,
    )
    .unwrap();

    PayloadRecord {
        record,
        row: json["row"].clone(),
    }
}
