use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use mink_core::{Header, Offsets, Partition, Record};
use mink_kafka::{Cluster, Config};
use mink_metrics::Metrics;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{self, Headers, Message, OwnedHeaders};
use rdkafka::producer::{BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};

const PARTITIONS: i32 = 3;
const RECORDS: usize = 300;
const TIMEOUT: Duration = Duration::from_secs(30);
const IDLE: Duration = Duration::from_secs(3);

fn produce(bootstrap: &str, topic: &str, seed: u64) {
    let mut random = fastrand::Rng::with_seed(seed);
    let plain = mink_conformance::producer(bootstrap);
    let transactional: Vec<_> = (0..2)
        .map(|index| {
            mink_conformance::transactional_producer(bootstrap, &format!("{topic}-{index}"))
        })
        .collect();
    let mut open = [false, false];

    for index in 0..RECORDS {
        let choice = random.usize(0..=transactional.len());
        let key = format!("k{index}");
        let value = format!("v{index}");
        let headers = OwnedHeaders::new()
            .insert(message::Header {
                key: "i",
                value: Some(&index.to_string()),
            })
            .insert(message::Header {
                key: "i",
                value: None::<&str>,
            });
        let record = BaseRecord::to(topic)
            .partition(random.i32(0..PARTITIONS))
            .key(&key)
            .payload(&value)
            .headers(headers);

        if choice == transactional.len() {
            plain.send(record).map_err(|(error, _)| error).unwrap();
            plain.poll(Duration::ZERO);
            continue;
        }

        let producer = &transactional[choice];
        if !open[choice] {
            producer.begin_transaction().unwrap();
            open[choice] = true;
        }

        producer.send(record).map_err(|(error, _)| error).unwrap();
        producer.poll(Duration::ZERO);

        if random.u8(0..10) == 0 {
            producer.flush(TIMEOUT).unwrap();
            match random.bool() {
                true => producer.commit_transaction(TIMEOUT).unwrap(),
                false => producer.abort_transaction(TIMEOUT).unwrap(),
            }

            open[choice] = false;
        }
    }

    plain.flush(TIMEOUT).unwrap();
    for (producer, open) in transactional.iter().zip(open) {
        if open {
            producer.commit_transaction(TIMEOUT).unwrap();
        }
    }
}

fn consumed(bootstrap: &str, topic: &str) -> BTreeMap<Partition, Vec<Record>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("{topic}-reader"))
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    for partition in 0..PARTITIONS {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .unwrap();
    }

    consumer.assign(&assignment).unwrap();

    let mut records: BTreeMap<Partition, Vec<Record>> = BTreeMap::new();
    let mut last = Instant::now();
    while last.elapsed() < IDLE {
        let Some(message) = consumer.poll(Duration::from_millis(200)) else {
            continue;
        };
        let message = message.unwrap();
        last = Instant::now();

        let headers = message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|header| Header::new(header.key, header.value.map(Bytes::copy_from_slice)))
                    .collect()
            })
            .unwrap_or_default();
        let record = Record::new(
            message.offset(),
            message.timestamp().to_millis().unwrap(),
            message.key().map(Bytes::copy_from_slice),
            message.payload().map(Bytes::copy_from_slice),
            headers,
        )
        .unwrap();

        let partition = Partition::new(topic, message.partition()).unwrap();
        records.entry(partition).or_default().push(record);
    }

    records
}

async fn read(cluster: &mut Cluster, topic: &str) -> BTreeMap<Partition, Vec<Record>> {
    let mut records = BTreeMap::new();

    for index in 0..PARTITIONS {
        let partition = Partition::new(topic, index).unwrap();
        let start = cluster.earliest(&partition).await.unwrap();
        let end = cluster.last_stable_offset(&partition).await.unwrap();

        let offsets = Offsets::new(partition.clone(), start, end).unwrap();
        let read = cluster.read(&offsets).await.unwrap();

        if !read.is_empty() {
            records.insert(partition, read);
        }
    }

    records
}

type SubRange = (Partition, Offsets, Vec<Record>, Vec<Record>);

async fn sub_ranges(
    cluster: &mut Cluster,
    topic: &str,
    expected: &BTreeMap<Partition, Vec<Record>>,
    seed: u64,
) -> Vec<SubRange> {
    let mut random = fastrand::Rng::with_seed(seed);
    let mut ranges = Vec::new();

    for index in 0..PARTITIONS {
        let partition = Partition::new(topic, index).unwrap();
        let end = cluster.last_stable_offset(&partition).await.unwrap();
        let start = random.i64(0..=end);
        let offsets = Offsets::new(partition.clone(), start, random.i64(start..=end)).unwrap();

        let read = cluster.read(&offsets).await.unwrap();

        let wanted = expected
            .get(&partition)
            .into_iter()
            .flatten()
            .filter(|record| offsets.range().includes(record.offset()))
            .cloned()
            .collect();
        ranges.push((partition, offsets, read, wanted));
    }

    ranges
}

#[tokio::test]
async fn kafka_reads_match_a_read_committed_consumer() {
    let Some(bootstrap) = mink_conformance::service("TEST_KAFKA_BOOTSTRAP") else {
        return;
    };
    let topic = format!("e2e-{}", fastrand::u64(..));
    let seed = fastrand::u64(..);
    let config = Config::new(vec![bootstrap.clone()])
        .unwrap()
        .with_max_wait(Duration::from_millis(100))
        .unwrap()
        .with_max_bytes(1)
        .unwrap();
    mink_conformance::create_topic(&bootstrap, &topic, PARTITIONS).await;
    produce(&bootstrap, &topic, seed);

    let expected = consumed(&bootstrap, &topic);
    let mut cluster = Cluster::connect(config, Metrics::new()).await.unwrap();
    let actual = read(&mut cluster, &topic).await;
    let ranges = sub_ranges(&mut cluster, &topic, &expected, seed).await;
    let topics = cluster.topics().await.unwrap();

    let found = topics.find(&topic).expect("topic discovered");
    assert_eq!(found.partition_count(), PARTITIONS);
    assert!(!found.uuid().is_nil());
    assert!(
        !expected.is_empty(),
        "seed {seed}: the workload committed nothing"
    );
    assert_eq!(actual, expected, "seed {seed}");
    for (partition, offsets, read, wanted) in ranges {
        assert_eq!(read, wanted, "seed {seed}: {partition:?} {offsets:?}");
    }
}

#[tokio::test]
async fn offsets_at_a_timestamp_follow_record_timestamps() {
    let Some(bootstrap) = mink_conformance::service("TEST_KAFKA_BOOTSTRAP") else {
        return;
    };
    let topic = format!("timestamps-{}", fastrand::u64(..));
    mink_conformance::create_topic(&bootstrap, &topic, 1).await;
    let producer = mink_conformance::producer(&bootstrap);
    for (index, timestamp_ms) in [1_000, 2_000, 3_000].into_iter().enumerate() {
        let record = BaseRecord::<(), str>::to(&topic)
            .partition(0)
            .timestamp(timestamp_ms)
            .payload(["a", "b", "c"][index]);
        producer.send(record).map_err(|(error, _)| error).unwrap();
    }
    producer.flush(TIMEOUT).unwrap();
    let mut cluster = Cluster::connect(Config::new(vec![bootstrap]).unwrap(), Metrics::new())
        .await
        .unwrap();
    let partition = Partition::new(&topic, 0).unwrap();

    let before = cluster.offset_at(&partition, 0).await.unwrap();
    let between = cluster.offset_at(&partition, 1_500).await.unwrap();
    let exact = cluster.offset_at(&partition, 3_000).await.unwrap();
    let after = cluster.offset_at(&partition, 4_000).await.unwrap();
    let negative = cluster.offset_at(&partition, -1).await;

    assert_eq!(
        (before, between, exact, after),
        (Some(0), Some(1), Some(2), None)
    );
    assert!(
        matches!(negative, Err(mink_kafka::Error::InvalidTimestamp(-1))),
        "{negative:?}"
    );
}
