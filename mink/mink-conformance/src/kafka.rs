use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};

const TIMEOUT: Duration = Duration::from_secs(30);

pub async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let new =
        NewTopic::new(topic, partitions, TopicReplication::Fixed(1)).set("retention.ms", "-1");

    for result in admin
        .create_topics([&new], &AdminOptions::new())
        .await
        .unwrap()
    {
        result.unwrap();
    }
}

pub fn producer(bootstrap: &str) -> BaseProducer {
    client(bootstrap).create().unwrap()
}

pub fn transactional_producer(bootstrap: &str, transactional_id: &str) -> BaseProducer {
    let producer: BaseProducer = client(bootstrap)
        .set("transactional.id", transactional_id)
        .create()
        .unwrap();

    producer.init_transactions(TIMEOUT).unwrap();
    producer
}

pub fn produce_values(bootstrap: &str, topic: &str, partition: i32, values: &[&str]) {
    let values: Vec<&[u8]> = values.iter().map(|value| value.as_bytes()).collect();

    produce_bytes(bootstrap, topic, partition, &values);
}

pub fn produce_bytes(bootstrap: &str, topic: &str, partition: i32, values: &[&[u8]]) {
    let producer = producer(bootstrap);

    send(&producer, topic, partition, values);
}

pub fn produce_at(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    values: &[&str],
    timestamp_ms: i64,
) {
    let producer = producer(bootstrap);

    for value in values {
        let record = BaseRecord::<(), str>::to(topic)
            .partition(partition)
            .payload(*value)
            .timestamp(timestamp_ms);
        producer.send(record).map_err(|(error, _)| error).unwrap();
    }

    producer.flush(TIMEOUT).unwrap();
}

pub fn produce_with_header(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    value: &[u8],
    (key, header): (&str, &[u8]),
) {
    let producer = producer(bootstrap);
    let headers = OwnedHeaders::new().insert(Header {
        key,
        value: Some(header),
    });

    let record = BaseRecord::<(), [u8]>::to(topic)
        .partition(partition)
        .payload(value)
        .headers(headers);
    producer.send(record).map_err(|(error, _)| error).unwrap();

    producer.flush(TIMEOUT).unwrap();
}

pub fn produce_aborted(bootstrap: &str, topic: &str, partition: i32, values: &[&str]) {
    let producer = transactional_producer(bootstrap, &format!("{topic}-aborted-{partition}"));

    let values: Vec<&[u8]> = values.iter().map(|value| value.as_bytes()).collect();

    producer.begin_transaction().unwrap();
    send(&producer, topic, partition, &values);
    producer.abort_transaction(TIMEOUT).unwrap();
}

pub async fn delete_records(bootstrap: &str, topic: &str, partition: i32, before: i64) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let mut offsets = TopicPartitionList::new();
    offsets
        .add_partition_offset(topic, partition, Offset::Offset(before))
        .unwrap();

    admin
        .delete_records(&offsets, &AdminOptions::new())
        .await
        .unwrap();
}

fn client(bootstrap: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap)
        .set("linger.ms", "5");

    config
}

fn send(producer: &BaseProducer, topic: &str, partition: i32, values: &[&[u8]]) {
    for value in values {
        let record = BaseRecord::<(), [u8]>::to(topic)
            .partition(partition)
            .payload(value);
        producer.send(record).map_err(|(error, _)| error).unwrap();
    }

    producer.flush(TIMEOUT).unwrap();
}
