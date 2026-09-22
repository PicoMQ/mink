//! A real Kafka client (librdkafka) against the nodes' Kafka listeners.

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, TopicPartitionList};

pub struct Kafka {
    bootstrap: String,
}

impl Kafka {
    pub fn new(bootstrap: impl Into<String>) -> Kafka {
        Kafka {
            bootstrap: bootstrap.into(),
        }
    }

    fn config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap)
            .set("socket.timeout.ms", "10000")
            .set("message.timeout.ms", "30000")
            .set("request.timeout.ms", "10000")
            .set("metadata.max.age.ms", "2000")
            .set("api.version.request", "true")
            .set("log_level", "3");
        config
    }

    pub async fn create_topic(&self, topic: &str, partitions: i32) {
        let admin: AdminClient<DefaultClientContext> = self.config().create().expect("admin");
        let results = admin
            .create_topics(
                [&NewTopic::new(
                    topic,
                    partitions,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(20))),
            )
            .await
            .expect("create_topics");
        for result in results {
            match result {
                Ok(_) => {}
                Err((_, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((name, code)) => panic!("create topic {name}: {code}"),
            }
        }
    }

    pub fn producer(&self) -> FutureProducer {
        self.config()
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("linger.ms", "5")
            .create()
            .expect("producer")
    }

    pub async fn produce(&self, topic: &str, records: &[(&str, &str)]) -> Vec<(i32, i64)> {
        let producer = self.producer();
        let mut out = Vec::with_capacity(records.len());
        for (key, value) in records {
            let delivery = producer
                .send(
                    FutureRecord::to(topic).key(*key).payload(*value),
                    Duration::from_secs(30),
                )
                .await
                .unwrap_or_else(|(e, _)| panic!("produce to {topic}: {e}"));
            out.push((delivery.partition, delivery.offset));
        }
        out
    }

    pub fn consume_all(
        &self,
        topic: &str,
        expected: usize,
        within: Duration,
    ) -> Vec<(String, String)> {
        let consumer: BaseConsumer = self
            .config()
            .set("group.id", crate::unique("e2e"))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .create()
            .expect("consumer");
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .expect("metadata");
        let partitions = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .map(|t| t.partitions().len() as i32)
            .expect("topic in metadata");
        let mut assignment = TopicPartitionList::new();
        for p in 0..partitions {
            assignment
                .add_partition_offset(topic, p, rdkafka::Offset::Beginning)
                .unwrap();
        }
        consumer.assign(&assignment).expect("assign");

        let deadline = std::time::Instant::now() + within;
        let mut out = Vec::new();
        while out.len() < expected && std::time::Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(500)) {
                Some(Ok(message)) => {
                    let key = message
                        .key()
                        .map(|k| String::from_utf8_lossy(k).into_owned())
                        .unwrap_or_default();
                    let value = message
                        .payload()
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                        .unwrap_or_default();
                    out.push((key, value));
                }
                Some(Err(e)) => tracing::warn!(error = %e, "kafka poll"),
                None => {}
            }
        }
        out
    }

    pub fn consume_group(
        &self,
        topic: &str,
        group: &str,
        expected: usize,
        within: Duration,
    ) -> Vec<(String, String)> {
        let consumer: BaseConsumer = self
            .config()
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            .create()
            .expect("consumer");
        consumer.subscribe(&[topic]).expect("subscribe");
        let deadline = std::time::Instant::now() + within;
        let mut out = Vec::new();
        while out.len() < expected && std::time::Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(500)) {
                Some(Ok(message)) => {
                    let key = message
                        .key()
                        .map(|k| String::from_utf8_lossy(k).into_owned())
                        .unwrap_or_default();
                    let value = message
                        .payload()
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                        .unwrap_or_default();
                    out.push((key, value));
                }
                Some(Err(e)) => tracing::warn!(error = %e, "kafka poll"),
                None => {}
            }
        }
        consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
            .expect("commit");
        out
    }

    pub fn partitions(&self, topic: &str) -> usize {
        let consumer: BaseConsumer = self.config().create().expect("consumer");
        consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .expect("metadata")
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .map(|t| t.partitions().len())
            .unwrap_or(0)
    }
}
