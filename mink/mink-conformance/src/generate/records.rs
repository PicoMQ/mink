use std::fs;
use std::ops::Range;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use bytes::Bytes;
use kafka_protocol::records::RecordBatchDecoder;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{Value, json};
use uuid::Uuid;

use super::broker::Broker;

const TIMEOUT: Duration = Duration::from_secs(30);
const TRUNCATED_BYTES: usize = 7;
const BASE_OFFSET: Range<usize> = 0..8;
const BATCH_LENGTH: Range<usize> = 8..12;
const PREFIX_SIZE: usize = 12;
const IDLE: Duration = Duration::from_secs(3);

#[derive(Clone, Copy)]
enum Workload {
    Plain,
    Headers,
    Committed,
    Aborted,
    Interleaved,
}

struct Case {
    name: &'static str,
    compression: &'static str,
    log_append_time: bool,
    workload: Workload,
    fetch_offset: i64,
}

struct Fixture {
    name: String,
    fetch_offset: i64,
    records: Bytes,
    aborted: Vec<Value>,
    expected: Vec<Value>,
    error: Value,
}

const BASE: Case = case("codec_none", "none", Workload::Plain, 0);

const CASES: [Case; 10] = [
    case("codec_gzip", "gzip", Workload::Plain, 0),
    case("codec_snappy", "snappy", Workload::Plain, 0),
    case("codec_lz4", "lz4", Workload::Plain, 0),
    case("codec_zstd", "zstd", Workload::Plain, 0),
    Case {
        log_append_time: true,
        ..case("log_append_time", "none", Workload::Plain, 0)
    },
    case("fetch_inside_a_batch", "none", Workload::Plain, 3),
    case("headers_and_nulls", "none", Workload::Headers, 0),
    case("committed_transaction", "none", Workload::Committed, 0),
    case("aborted_transaction", "none", Workload::Aborted, 0),
    case("interleaved_producers", "none", Workload::Interleaved, 0),
];

const fn case(
    name: &'static str,
    compression: &'static str,
    workload: Workload,
    fetch_offset: i64,
) -> Case {
    Case {
        name,
        compression,
        log_append_time: false,
        workload,
        fetch_offset,
    }
}

impl Fixture {
    fn case(&self) -> Value {
        json!({
            "name": self.name,
            "fetch_offset": self.fetch_offset,
            "aborted": self.aborted,
            "records": self.expected,
            "error": self.error,
        })
    }
}

pub async fn generate(bootstrap: &str, kafka_image: &str, bless: bool) -> anyhow::Result<()> {
    let run = Uuid::new_v4().simple().to_string();
    let topic = |case: &Case| format!("conformance-{}-{}", &run[..8], case.name);

    let base = capture(bootstrap, &topic(&BASE), &BASE)
        .await
        .context(BASE.name)?;
    let derived = [truncated(&base)?, corrupt(&base)?];
    let mut fixtures = vec![base];
    fixtures.extend(derived);
    for case in &CASES {
        fixtures.push(
            capture(bootstrap, &topic(case), case)
                .await
                .context(case.name)?,
        );
    }

    let cases = json!({
        "kafka": kafka_image,
        "cases": fixtures.iter().map(Fixture::case).collect::<Vec<_>>(),
    });
    let directory = mink_conformance::fixtures("records");
    if !bless {
        println!(
            "captured {} cases; rerun with --bless to write {}",
            fixtures.len(),
            directory.display()
        );
        return Ok(());
    }

    fs::create_dir_all(&directory)?;
    for fixture in &fixtures {
        fs::write(
            directory.join(format!("{}.bin", fixture.name)),
            &fixture.records,
        )?;
    }

    fs::write(
        directory.join("cases.json"),
        serde_json::to_string_pretty(&cases)? + "\n",
    )?;
    println!("wrote {} cases to {}", fixtures.len(), directory.display());

    Ok(())
}

async fn capture(bootstrap: &str, topic: &str, case: &Case) -> anyhow::Result<Fixture> {
    create(bootstrap, topic, case.log_append_time).await?;
    produce(bootstrap, topic, case)?;

    let expected = consume(bootstrap, topic, case.fetch_offset)?;
    let mut broker = Broker::connect(bootstrap).await?;
    let end = broker.last_stable_offset(topic).await?;
    let data = broker.fetch(topic, case.fetch_offset).await?;
    let records = data.records.unwrap_or_default();

    let covered = last_offset(&records)?;
    if covered + 1 < end {
        bail!("{topic}: one fetch covered offsets up to {covered}, the log ends at {end}");
    }

    let aborted = data
        .aborted_transactions
        .unwrap_or_default()
        .into_iter()
        .map(|transaction| json!({ "producer_id": transaction.producer_id.0, "first_offset": transaction.first_offset }))
        .collect();

    Ok(Fixture {
        name: case.name.to_owned(),
        fetch_offset: case.fetch_offset,
        records,
        aborted,
        expected,
        error: Value::Null,
    })
}

async fn create(bootstrap: &str, topic: &str, log_append_time: bool) -> anyhow::Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()?;
    let timestamp_type = if log_append_time {
        "LogAppendTime"
    } else {
        "CreateTime"
    };
    let new = NewTopic::new(topic, 1, TopicReplication::Fixed(1))
        .set("message.timestamp.type", timestamp_type);

    for result in admin.create_topics([&new], &AdminOptions::new()).await? {
        result.map_err(|(topic, code)| anyhow::anyhow!("create {topic}: {code}"))?;
    }

    Ok(())
}

fn producer(
    bootstrap: &str,
    compression: &str,
    transactional_id: Option<&str>,
) -> anyhow::Result<BaseProducer> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap)
        .set("compression.type", compression)
        .set("linger.ms", "50")
        .set("batch.num.messages", "1000");
    if let Some(id) = transactional_id {
        config.set("transactional.id", id);
    }

    let producer: BaseProducer = config.create()?;
    if transactional_id.is_some() {
        producer.init_transactions(TIMEOUT)?;
    }

    Ok(producer)
}

fn send(producer: &BaseProducer, topic: &str, index: usize) -> anyhow::Result<()> {
    let key = format!("k{index}");
    let value = format!("v{index}");
    let headers = OwnedHeaders::new().insert(Header {
        key: "i",
        value: Some(&index.to_string()),
    });
    let record = BaseRecord::to(topic)
        .partition(0)
        .key(&key)
        .payload(&value)
        .headers(headers);

    producer.send(record).map_err(|(error, _)| error)?;
    producer.poll(Duration::ZERO);

    Ok(())
}

fn produce(bootstrap: &str, topic: &str, case: &Case) -> anyhow::Result<()> {
    match case.workload {
        Workload::Plain => {
            let producer = producer(bootstrap, case.compression, None)?;
            for round in 0..2 {
                for index in 0..10 {
                    send(&producer, topic, round * 10 + index)?;
                }

                producer.flush(TIMEOUT)?;
            }
        }
        Workload::Headers => {
            let producer = producer(bootstrap, case.compression, None)?;
            let repeated = OwnedHeaders::new()
                .insert(Header {
                    key: "a",
                    value: Some("1"),
                })
                .insert(Header {
                    key: "a",
                    value: None::<&str>,
                })
                .insert(Header {
                    key: "b",
                    value: Some(""),
                });
            producer
                .send(
                    BaseRecord::to(topic)
                        .partition(0)
                        .key("k")
                        .payload("v")
                        .headers(repeated),
                )
                .map_err(|(error, _)| error)?;
            producer
                .send(
                    BaseRecord::<str, str>::to(topic)
                        .partition(0)
                        .payload("no key"),
                )
                .map_err(|(error, _)| error)?;
            producer
                .send(
                    BaseRecord::<str, str>::to(topic)
                        .partition(0)
                        .key("no value"),
                )
                .map_err(|(error, _)| error)?;
            producer
                .send(BaseRecord::to(topic).partition(0).key("empty").payload(""))
                .map_err(|(error, _)| error)?;
            producer.flush(TIMEOUT)?;
        }
        Workload::Committed => {
            let producer = producer(bootstrap, case.compression, Some(&format!("{topic}-p1")))?;
            producer.begin_transaction()?;
            for index in 0..5 {
                send(&producer, topic, index)?;
            }

            producer.commit_transaction(TIMEOUT)?;
        }
        Workload::Aborted => {
            let transactional =
                producer(bootstrap, case.compression, Some(&format!("{topic}-p1")))?;
            transactional.begin_transaction()?;
            for index in 0..5 {
                send(&transactional, topic, index)?;
            }

            transactional.flush(TIMEOUT)?;
            transactional.abort_transaction(TIMEOUT)?;

            let plain = producer(bootstrap, case.compression, None)?;
            for index in 5..8 {
                send(&plain, topic, index)?;
            }

            plain.flush(TIMEOUT)?;
        }
        Workload::Interleaved => {
            let first = producer(bootstrap, case.compression, Some(&format!("{topic}-p1")))?;
            let second = producer(bootstrap, case.compression, Some(&format!("{topic}-p2")))?;
            first.begin_transaction()?;
            second.begin_transaction()?;
            for index in 0..6 {
                let producer = if index % 2 == 0 { &first } else { &second };
                send(producer, topic, index)?;
                producer.flush(TIMEOUT)?;
            }

            first.commit_transaction(TIMEOUT)?;
            second.abort_transaction(TIMEOUT)?;

            first.begin_transaction()?;
            for index in 6..8 {
                send(&first, topic, index)?;
            }

            first.commit_transaction(TIMEOUT)?;
        }
    }

    Ok(())
}

fn consume(bootstrap: &str, topic: &str, fetch_offset: i64) -> anyhow::Result<Vec<Value>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("{topic}-reader"))
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create()?;
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, 0, Offset::Offset(fetch_offset))?;
    consumer.assign(&assignment)?;

    let mut records = Vec::new();
    let mut last = Instant::now();
    while last.elapsed() < IDLE {
        let Some(message) = consumer.poll(Duration::from_millis(200)) else {
            continue;
        };
        let message = message?;
        last = Instant::now();

        let headers: Vec<Value> = message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|header| json!({ "key": header.key, "value_hex": header.value.map(hex::encode) }))
                    .collect()
            })
            .unwrap_or_default();
        records.push(json!({
            "offset": message.offset(),
            "timestamp_ms": message.timestamp().to_millis(),
            "key_hex": message.key().map(hex::encode),
            "value_hex": message.payload().map(hex::encode),
            "headers": headers,
        }));
    }

    Ok(records)
}

fn last_offset(records: &Bytes) -> anyhow::Result<i64> {
    let batches = RecordBatchDecoder::decode_batch_info(&mut records.clone())?;

    Ok(batches.last().map_or(-1, |batch| {
        batch.min_offset + i64::from(batch.record_count) - 1
    }))
}

fn truncated(base: &Fixture) -> anyhow::Result<Fixture> {
    let batches = RecordBatchDecoder::decode_batch_info(&mut base.records.clone())?;
    let [_, .., last] = &batches[..] else {
        bail!(
            "{} needs at least two batches to truncate the last one",
            base.name
        )
    };

    let records = base.records.slice(..base.records.len() - TRUNCATED_BYTES);
    let expected = base
        .expected
        .iter()
        .filter(|record| {
            record["offset"]
                .as_i64()
                .is_some_and(|offset| offset < last.min_offset)
        })
        .cloned()
        .collect();

    Ok(Fixture {
        name: "truncated_last_batch".to_owned(),
        fetch_offset: base.fetch_offset,
        records,
        aborted: Vec::new(),
        expected,
        error: Value::Null,
    })
}

fn corrupt(base: &Fixture) -> anyhow::Result<Fixture> {
    let mut bytes = base.records.to_vec();
    let length = i32::from_be_bytes(bytes[BATCH_LENGTH].try_into()?);
    let first_batch_end = PREFIX_SIZE + usize::try_from(length)?;
    let first_base_offset = i64::from_be_bytes(bytes[BASE_OFFSET].try_into()?);

    bytes[first_batch_end - 1] ^= 0xff;

    Ok(Fixture {
        name: "corrupt_crc".to_owned(),
        fetch_offset: base.fetch_offset,
        records: Bytes::from(bytes),
        aborted: Vec::new(),
        expected: Vec::new(),
        error: json!({ "kind": "corrupt_batch", "base_offset": first_base_offset }),
    })
}
