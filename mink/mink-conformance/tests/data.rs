use std::process::Command;

use bytes::Bytes;
use mink_core::Record;
use serde_json::{Value, json};

fn query(duckdb: &str, sql: &str) -> Vec<Value> {
    let output = Command::new(duckdb)
        .args(["-json", "-c", sql])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let rows = String::from_utf8(output.stdout).unwrap();
    if rows.trim().is_empty() {
        return Vec::new();
    }

    serde_json::from_str(&rows).unwrap()
}

fn row(partition: i32, record: &Record) -> Value {
    let hex = |bytes: Option<&Bytes>| bytes.map(hex::encode);
    let headers: Vec<_> = record
        .headers()
        .iter()
        .map(|header| json!({ "key": header.key(), "value": hex(header.value()) }))
        .collect();

    json!({
        "partition": partition,
        "offset": record.offset(),
        "timestamp_ms": record.timestamp_ms(),
        "key": hex(record.key()),
        "headers": headers,
        "value": hex(record.value()),
    })
}

#[test]
fn data_fixtures_read_in_duckdb() {
    let Some(duckdb) = mink_conformance::service("TEST_DUCKDB") else {
        return;
    };
    let fixtures = mink_conformance::data_fixtures();
    assert!(!fixtures.is_empty(), "no data fixtures");

    for fixture in fixtures {
        let path = fixture.path();
        let path = path.display();

        let rows = query(
            &duckdb,
            &format!(
                "SELECT _kafka_partition AS partition, _kafka_offset AS offset, epoch_ms(_kafka_timestamp) AS timestamp_ms, \
                 lower(hex(_kafka_key)) AS key, \
                 to_json(list_transform(_kafka_headers, h -> {{'key': h.key, 'value': lower(hex(h.value))}})) AS headers, \
                 lower(hex(_kafka_value)) AS value \
                 FROM read_parquet('{path}') ORDER BY _kafka_offset"
            ),
        );
        let ids = query(
            &duckdb,
            &format!(
                "SELECT field_id FROM parquet_schema('{path}') WHERE field_id IS NOT NULL ORDER BY field_id"
            ),
        );

        let partition = fixture.file().partition().index();
        let expected: Vec<_> = fixture
            .records()
            .iter()
            .map(|record| row(partition, record))
            .collect();
        assert_eq!(rows, expected, "{}", fixture.name());
        assert_eq!(
            ids,
            (1..=9)
                .map(|id| json!({ "field_id": id }))
                .collect::<Vec<_>>(),
            "{}",
            fixture.name()
        );
    }
}
