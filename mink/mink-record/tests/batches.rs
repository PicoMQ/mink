//! Checks batch encoding against fixtures produced by the JVM builder.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use mink_record::{Batch, ChangeType, Compression, Projection, Spec, build, codec, header::MAGIC};
use mink_table::{LogFormat, SchemaId};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    rows: Vec<Row>,
    batches: Vec<Case>,
}

#[derive(Deserialize)]
struct Row {
    id: i32,
    name: String,
    score: Option<f64>,
    ts_micros: i64,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    compression: String,
    append_only: bool,
    base_offset: i64,
    last_offset: i64,
    commit_timestamp: i64,
    leader_epoch: i32,
    crc: u32,
    schema_id: u32,
    writer_id: i64,
    batch_sequence: i32,
    record_count: i32,
    size: usize,
    changes: Vec<u8>,
    bytes: String,
}

// `fixtures/batches.json` holds batches the Java builder produced; regenerate it from Java only.
fn fixture() -> Fixture {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/batches.json");
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]))
}

fn rows(fixture: &Fixture, count: usize) -> RecordBatch {
    let rows = &fixture.rows[..count];
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.id))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| &r.name))),
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.score))),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                rows.iter().map(|r| r.ts_micros),
            )),
        ],
    )
    .unwrap()
}

fn compression(name: &str) -> Compression {
    match name {
        "none" => Compression::None,
        "lz4_frame" => Compression::Lz4Frame,
        "zstd" => Compression::Zstd,
        other => panic!("unknown compression {other}"),
    }
}

fn parse(case: &Case) -> Batch {
    Batch::parse(Bytes::from(hex::decode(&case.bytes).unwrap())).unwrap()
}

#[test]
fn reads_java_headers_and_crcs() {
    for case in fixture().batches {
        let batch = parse(&case);
        let header = batch.header();
        assert_eq!(batch.bytes()[12], MAGIC, "{}", case.name);
        assert_eq!(header.size, case.size, "{}", case.name);
        assert_eq!(header.base_offset, case.base_offset, "{}", case.name);
        assert_eq!(header.last_offset(), case.last_offset, "{}", case.name);
        assert_eq!(
            header.commit_timestamp, case.commit_timestamp,
            "{}",
            case.name
        );
        assert_eq!(header.leader_epoch, case.leader_epoch, "{}", case.name);
        assert_eq!(header.crc, case.crc, "{}", case.name);
        assert_eq!(header.schema_id, SchemaId(case.schema_id), "{}", case.name);
        assert_eq!(header.append_only, case.append_only, "{}", case.name);
        assert_eq!(header.writer_id, case.writer_id, "{}", case.name);
        assert_eq!(header.batch_sequence, case.batch_sequence, "{}", case.name);
        assert_eq!(header.record_count, case.record_count, "{}", case.name);
        batch
            .ensure_valid()
            .unwrap_or_else(|e| panic!("{}: {e}", case.name));
    }
}

#[test]
fn reads_java_rows_and_change_types() {
    let fixture = fixture();
    for case in &fixture.batches {
        let batch = parse(case);
        let codec = codec(LogFormat::Arrow, compression(&case.compression));
        let records = batch.records(codec.as_ref(), schema(), None).unwrap();
        assert_eq!(
            records.batch,
            rows(&fixture, case.record_count as usize),
            "{}",
            case.name
        );
        assert_eq!(
            records.changes.iter().map(|c| c.byte()).collect::<Vec<_>>(),
            case.changes,
            "{}",
            case.name
        );
    }
}

#[test]
fn mink_batches_read_back_like_java_ones() {
    let fixture = fixture();
    for case in &fixture.batches {
        let compression = compression(&case.compression);
        let codec = codec(LogFormat::Arrow, compression);
        let changes: Vec<ChangeType> = case
            .changes
            .iter()
            .map(|b| ChangeType::from_byte(*b).unwrap())
            .collect();
        let spec = Spec::new(SchemaId(case.schema_id), case.append_only)
            .with_base_offset(case.base_offset)
            .with_writer(case.writer_id, case.batch_sequence);
        let count = case.record_count as usize;
        let bytes = build(spec, &changes, &rows(&fixture, count), codec.as_ref()).unwrap();
        let ours = Batch::parse(Bytes::from(bytes)).unwrap();
        ours.ensure_valid().unwrap();
        let theirs = parse(case);

        assert_eq!(ours.header().record_count, theirs.header().record_count);
        assert_eq!(
            ours.header().last_offset_delta,
            theirs.header().last_offset_delta
        );
        assert_eq!(ours.header().append_only, theirs.header().append_only);
        assert_eq!(
            ours.bytes()[29..32],
            theirs.bytes()[29..32],
            "schema id + attributes"
        );
        assert_eq!(
            ours.bytes()[32..52],
            theirs.bytes()[32..52],
            "delta, writer, sequence, count"
        );
        if compression == Compression::None {
            assert_eq!(
                ours.size(),
                theirs.size(),
                "{}: uncompressed sizes",
                case.name
            );
        }

        let records = ours.records(codec.as_ref(), schema(), None).unwrap();
        assert_eq!(records.batch, rows(&fixture, count), "{}", case.name);
    }
}

#[test]
fn projecting_java_batches_keeps_the_selected_rows() {
    let fixture = fixture();
    for case in &fixture.batches {
        let batch = parse(case);
        let codec = codec(LogFormat::Arrow, compression(&case.compression));
        for columns in [vec![0usize], vec![1, 3], vec![0, 1, 2, 3], vec![2]] {
            let projection = Projection::new(&schema(), &columns).unwrap();
            let projected = Batch::parse(Bytes::from(projection.apply(&batch).unwrap())).unwrap();
            assert_eq!(projected.header().record_count, case.record_count);
            assert_eq!(projected.header().base_offset, case.base_offset);
            assert!(projected.size() <= batch.size());
            let expected = rows(&fixture, case.record_count as usize)
                .project(&columns)
                .unwrap();
            let projected_schema = Arc::new(schema().project(&columns).unwrap());
            let records = projected
                .records(codec.as_ref(), projected_schema, None)
                .unwrap();
            assert_eq!(records.batch, expected, "{} {columns:?}", case.name);
            assert_eq!(
                records.changes.iter().map(|c| c.byte()).collect::<Vec<_>>(),
                case.changes
            );
        }
    }
}

#[test]
fn projection_rejects_bad_selections() {
    let schema = schema();
    assert!(Projection::new(&schema, &[4]).is_err());
    assert!(Projection::new(&schema, &[1, 1]).is_err());
    assert!(Projection::new(&schema, &[2, 1]).is_err());
    assert!(Projection::new(&schema, &[]).is_ok());
}
