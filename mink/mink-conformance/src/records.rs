use std::fs;

use bytes::Bytes;
use kafka_protocol::messages::ProducerId;
use kafka_protocol::messages::fetch_response::AbortedTransaction;
use mink_core::{Header, Record};
use serde_json::Value;

use crate::fixtures;

pub struct RecordFixture {
    name: String,
    batches: Bytes,
    fetch_offset: i64,
    aborted: Vec<AbortedTransaction>,
    expected: Vec<Record>,
    corrupt_at: Option<i64>,
}

impl RecordFixture {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn batches(&self) -> &Bytes {
        &self.batches
    }

    pub fn fetch_offset(&self) -> i64 {
        self.fetch_offset
    }

    pub fn aborted(&self) -> &[AbortedTransaction] {
        &self.aborted
    }

    pub fn expected(&self) -> &[Record] {
        &self.expected
    }

    pub fn corrupt_at(&self) -> Option<i64> {
        self.corrupt_at
    }
}

pub fn record_fixtures() -> Vec<RecordFixture> {
    let directory = fixtures("records");
    let cases: Value =
        serde_json::from_str(&fs::read_to_string(directory.join("cases.json")).unwrap()).unwrap();

    let fixtures: Vec<_> = cases["cases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|fixture| {
            let name = fixture["name"].as_str().unwrap().to_owned();
            let batches = Bytes::from(fs::read(directory.join(format!("{name}.bin"))).unwrap());

            RecordFixture {
                name,
                batches,
                fetch_offset: fixture["fetch_offset"].as_i64().unwrap(),
                aborted: fixture["aborted"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(aborted)
                    .collect(),
                expected: fixture["records"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(record)
                    .collect(),
                corrupt_at: corrupt_at(&fixture["error"]),
            }
        })
        .collect();
    assert!(!fixtures.is_empty(), "no record fixtures");

    fixtures
}

fn aborted(transaction: &Value) -> AbortedTransaction {
    AbortedTransaction::default()
        .with_producer_id(ProducerId(transaction["producer_id"].as_i64().unwrap()))
        .with_first_offset(transaction["first_offset"].as_i64().unwrap())
}

fn record(record: &Value) -> Record {
    let headers = record["headers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|header| Header::new(header["key"].as_str().unwrap(), bytes(&header["value_hex"])))
        .collect();

    Record::new(
        record["offset"].as_i64().unwrap(),
        record["timestamp_ms"].as_i64().unwrap(),
        bytes(&record["key_hex"]),
        bytes(&record["value_hex"]),
        headers,
    )
    .unwrap()
}

fn bytes(value: &Value) -> Option<Bytes> {
    value
        .as_str()
        .map(|hex| Bytes::from(hex::decode(hex).unwrap()))
}

fn corrupt_at(error: &Value) -> Option<i64> {
    match error["kind"].as_str() {
        None => None,
        Some("corrupt_batch") => error["base_offset"].as_i64(),
        Some(kind) => panic!("unknown fixture error kind {kind}"),
    }
}
