use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::pin::Pin;

use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::{BrokerId, MetadataResponse};
use mink_core::{Offsets, Partition, Record, Topic};
use mink_metrics::Metrics;

use crate::Error;
use crate::batch::Batch;
use crate::codes::NONE;
use crate::config::Config;
use crate::connection::{Connection, EARLIEST, LATEST};
use crate::connector::Connector;
use crate::listing::Listing;
use crate::retry::{Backoff, retriable};
use crate::transaction::committed;

const NO_LEADER: BrokerId = BrokerId(-1);

type Call<'c, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'c>>;

pub struct Cluster {
    config: Config,
    connector: Connector,
    metrics: Metrics,
    brokers: HashMap<BrokerId, String>,
    leaders: HashMap<Partition, Option<BrokerId>>,
    connections: HashMap<BrokerId, Connection>,
}

impl Cluster {
    pub async fn connect(config: Config, metrics: Metrics) -> Result<Cluster, Error> {
        let connector = Connector::new(&config)?;
        let mut cluster = Cluster {
            config,
            connector,
            metrics,
            brokers: HashMap::new(),
            leaders: HashMap::new(),
            connections: HashMap::new(),
        };

        cluster.refresh().await?;

        Ok(cluster)
    }

    pub async fn topics(&mut self) -> Result<Listing, Error> {
        self.refresh().await
    }

    pub async fn earliest(&mut self, partition: &Partition) -> Result<i64, Error> {
        self.offset(partition, EARLIEST).await
    }

    pub async fn last_stable_offset(&mut self, partition: &Partition) -> Result<i64, Error> {
        self.offset(partition, LATEST).await
    }

    pub async fn offset_at(
        &mut self,
        partition: &Partition,
        timestamp_ms: i64,
    ) -> Result<Option<i64>, Error> {
        if timestamp_ms < 0 {
            return Err(Error::InvalidTimestamp(timestamp_ms));
        }

        let offset = self.offset(partition, timestamp_ms).await?;

        Ok((offset >= 0).then_some(offset))
    }

    async fn offset(&mut self, partition: &Partition, timestamp: i64) -> Result<i64, Error> {
        self.on_leader(partition, move |connection, partition| {
            Box::pin(connection.offset(partition, timestamp))
        })
        .await
    }

    pub async fn read(&mut self, offsets: &Offsets) -> Result<Vec<Record>, Error> {
        let (partition, end) = (offsets.partition(), offsets.end_offset());

        let stable = self.last_stable_offset(partition).await?;
        if end > stable {
            return Err(Error::BeyondStableOffset { end, stable });
        }

        let mut records = Vec::new();
        let mut offset = offsets.start_offset();
        let mut stall = Backoff::new(self.config.retry_deadline());

        while offset < end {
            let data = self.fetch(partition, offset).await?;
            let aborted = data.aborted_transactions.unwrap_or_default();
            let batches = Batch::parse(data.records.unwrap_or_default())?;

            let next = batches.last().map(|batch| batch.last_offset + 1);
            let Some(next) = next.filter(|next| *next > offset) else {
                if !stall.wait().await {
                    return Err(Error::Stalled { offset });
                }

                continue;
            };

            stall = Backoff::new(self.config.retry_deadline());
            let kept = committed(batches, &aborted, offset);
            records.extend(kept.into_iter().take_while(|record| record.offset() < end));
            offset = next;
        }

        Ok(records)
    }

    async fn fetch(&mut self, partition: &Partition, offset: i64) -> Result<PartitionData, Error> {
        let max_wait_ms = self.config.max_wait_ms();
        let max_bytes = self.config.max_bytes();

        self.on_leader(partition, move |connection, partition| {
            Box::pin(connection.fetch(partition, offset, max_wait_ms, max_bytes))
        })
        .await
    }

    async fn on_leader<T>(
        &mut self,
        partition: &Partition,
        call: impl for<'c> Fn(&'c mut Connection, &'c Partition) -> Call<'c, T> + Send,
    ) -> Result<T, Error> {
        if !self.leaders.contains_key(partition) {
            self.refresh().await?;
        }

        let mut backoff = Backoff::new(self.config.retry_deadline());

        loop {
            let result = match self.leader_connection(partition).await {
                Ok(connection) => call(connection, partition).await,
                Err(error) => Err(error),
            };

            let error = match result {
                Err(error) if retriable(&error) => error,
                result => return result,
            };

            if !backoff.wait().await {
                return Err(error);
            }

            if let Err(error) = self.refresh_once().await
                && !retriable(&error)
            {
                return Err(error);
            }
        }
    }

    async fn refresh(&mut self) -> Result<Listing, Error> {
        let mut backoff = Backoff::new(self.config.retry_deadline());

        loop {
            let error = match self.refresh_once().await {
                Ok(topics) => return Ok(topics),
                Err(error) => error,
            };

            if !retriable(&error) || !backoff.wait().await {
                return Err(error);
            }
        }
    }

    async fn leader_connection(&mut self, partition: &Partition) -> Result<&mut Connection, Error> {
        let broker = self.leader(partition)?;

        self.connection(broker).await
    }

    fn leader(&self, partition: &Partition) -> Result<BrokerId, Error> {
        match self.leaders.get(partition) {
            None => Err(Error::UnknownPartition(partition.clone())),
            Some(None) => Err(Error::NoLeader(partition.clone())),
            Some(Some(broker)) => Ok(*broker),
        }
    }

    async fn connection(&mut self, broker: BrokerId) -> Result<&mut Connection, Error> {
        if self
            .connections
            .get(&broker)
            .is_some_and(|connection| connection.closed() || connection.expired())
        {
            self.connections.remove(&broker);
        }

        match self.connections.entry(broker) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let address = self
                    .brokers
                    .get(&broker)
                    .ok_or(Error::UnknownBroker(broker))?;
                let connection = self
                    .connector
                    .connect(address, self.metrics.clone())
                    .await?;

                Ok(entry.insert(connection))
            }
        }
    }

    async fn refresh_once(&mut self) -> Result<Listing, Error> {
        let mut failures = Vec::new();

        let brokers: Vec<BrokerId> = self.brokers.keys().copied().collect();
        for broker in brokers {
            let metadata = match self.connection(broker).await {
                Ok(connection) => connection.metadata().await,
                Err(error) => Err(error),
            };

            match metadata {
                Ok(metadata) => return Ok(self.apply_metadata(metadata)),
                Err(error) => failures.push((format!("broker {}", broker.0), error)),
            }
        }

        for address in self.config.bootstrap().to_vec() {
            let connected = self.connector.connect(&address, self.metrics.clone()).await;
            let metadata = match connected {
                Ok(mut connection) => connection.metadata().await,
                Err(error) => Err(error),
            };

            match metadata {
                Ok(metadata) => return Ok(self.apply_metadata(metadata)),
                Err(error) => failures.push((address, error)),
            }
        }

        Err(Error::Unreachable(failures))
    }

    fn apply_metadata(&mut self, metadata: MetadataResponse) -> Listing {
        self.brokers = metadata
            .brokers
            .into_iter()
            .map(|broker| (broker.node_id, format!("{}:{}", broker.host, broker.port)))
            .collect();
        self.connections
            .retain(|broker, _| self.brokers.contains_key(broker));
        self.leaders.clear();

        let mut topics = Vec::new();
        let mut unavailable = Vec::new();
        for topic in metadata.topics {
            let Some(name) = topic.name.map(|name| name.0.to_string()) else {
                continue;
            };
            for partition in &topic.partitions {
                let leader = (partition.leader_id != NO_LEADER).then_some(partition.leader_id);
                if let Ok(partition) = Partition::new(name.clone(), partition.partition_index) {
                    self.leaders.insert(partition, leader);
                }
            }

            if topic.is_internal {
                continue;
            }
            if topic.error_code != NONE {
                unavailable.push(name);
                continue;
            }

            if let Ok(partition_count) = i32::try_from(topic.partitions.len())
                && let Ok(topic) = Topic::new(name, topic.topic_id, partition_count)
            {
                topics.push(topic);
            }
        }

        Listing::new(topics, unavailable)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::fetch_response::FetchableTopicResponse;
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::{
        ApiKey, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
        SaslAuthenticateResponse, SaslHandshakeResponse, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, StrBytes, decode_request_header_from_buffer};
    use kafka_protocol::records::{
        Compression, Record as Encoded, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    use uuid::Uuid;

    use super::*;
    use crate::codes::{LEADER_NOT_AVAILABLE, UNSUPPORTED_SASL_MECHANISM};
    use crate::connection::tests::{TIMEOUT, broker, frame};
    use crate::sasl::{Mechanism, Sasl};

    const UNREACHABLE: &str = "127.0.0.1:1";

    #[derive(Clone, Copy, Default)]
    enum Fetches {
        #[default]
        AtTheRequestedOffset,
        AlwaysAtOffsetZero,
        Empty,
        WithoutThePartition,
    }

    #[derive(Default)]
    struct Script {
        leaderless_metadata_calls: usize,
        stable: i64,
        advertise_unreachable: bool,
        fetches: Fetches,
    }

    struct Scripted {
        cluster: Cluster,
        requests: Arc<Mutex<Vec<(ApiKey, i16, Bytes)>>>,
    }

    fn name(topic: &'static str) -> TopicName {
        TopicName(StrBytes::from_static_str(topic))
    }

    fn topic(topic_name: &'static str, id: u128, leader: i32) -> MetadataResponseTopic {
        MetadataResponseTopic::default()
            .with_name(Some(name(topic_name)))
            .with_topic_id(Uuid::from_u128(id))
            .with_partitions(vec![
                MetadataResponsePartition::default()
                    .with_partition_index(0)
                    .with_leader_id(BrokerId(leader)),
            ])
    }

    fn metadata(address: &str, leader: i32) -> MetadataResponse {
        let (host, port) = address.rsplit_once(':').unwrap();

        MetadataResponse::default()
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId(0))
                    .with_host(StrBytes::from_string(host.to_owned()))
                    .with_port(port.parse().unwrap()),
            ])
            .with_topics(vec![
                topic("orders", 7, leader),
                topic("__consumer_offsets", 8, leader).with_is_internal(true),
                topic("creating", 9, leader).with_error_code(LEADER_NOT_AVAILABLE),
                topic("bad name", 10, leader),
                topic("empty", 11, leader).with_partitions(Vec::new()),
            ])
    }

    fn offset(value: i64) -> ListOffsetsResponse {
        ListOffsetsResponse::default().with_topics(vec![
            ListOffsetsTopicResponse::default()
                .with_name(name("orders"))
                .with_partitions(vec![
                    ListOffsetsPartitionResponse::default()
                        .with_partition_index(0)
                        .with_offset(value),
                ]),
        ])
    }

    fn batch(offset: i64) -> Bytes {
        let record = Encoded {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: -1,
            timestamp: offset,
            key: None,
            value: Some(Bytes::from(offset.to_string())),
            headers: IndexMap::new(),
        };
        let mut buffer = BytesMut::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };

        RecordBatchEncoder::encode(&mut buffer, [&record], &options).unwrap();

        buffer.freeze()
    }

    fn fetch(fetches: Fetches, offset: i64) -> FetchResponse {
        let records = match fetches {
            Fetches::AtTheRequestedOffset => batch(offset),
            Fetches::AlwaysAtOffsetZero => batch(0),
            Fetches::Empty => Bytes::new(),
            Fetches::WithoutThePartition => return FetchResponse::default(),
        };

        FetchResponse::default().with_responses(vec![
            FetchableTopicResponse::default()
                .with_topic(name("orders"))
                .with_partitions(vec![
                    PartitionData::default()
                        .with_partition_index(0)
                        .with_records(Some(records)),
                ]),
        ])
    }

    fn fetch_offset(request: &Bytes) -> i64 {
        let mut request = request.clone();
        let header = decode_request_header_from_buffer(&mut request).unwrap();
        let fetch = FetchRequest::decode(&mut request, header.request_api_version).unwrap();

        fetch.topics[0].partitions[0].fetch_offset
    }

    async fn scripted(script: Script) -> Scripted {
        let metadata_calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let address = Arc::new(OnceLock::<String>::new());
        let own = address.clone();
        let seen = requests.clone();

        let bound = broker(move |key, version, correlation, request| {
            seen.lock().unwrap().push((key, version, request.clone()));
            let address = match script.advertise_unreachable {
                true => UNREACHABLE,
                false => own.get().unwrap().as_str(),
            };

            let body = match key {
                ApiKey::Metadata => {
                    let call = metadata_calls.fetch_add(1, Ordering::SeqCst);
                    let leader = if call < script.leaderless_metadata_calls {
                        -1
                    } else {
                        0
                    };
                    frame(correlation, key, version, &metadata(address, leader))
                }
                ApiKey::Fetch => {
                    let response = fetch(script.fetches, fetch_offset(&request));
                    frame(correlation, key, version, &response)
                }
                _ => frame(correlation, key, version, &offset(script.stable)),
            };

            Some(body)
        })
        .await;
        address.set(bound.clone()).unwrap();
        let config = Config::new(vec![bound])
            .unwrap()
            .with_retry_deadline(Duration::from_secs(2))
            .with_max_wait(TIMEOUT / 10)
            .unwrap();

        Scripted {
            cluster: Cluster::connect(config, Metrics::new()).await.unwrap(),
            requests,
        }
    }

    async fn cluster(script: Script) -> Cluster {
        scripted(script).await.cluster
    }

    fn orders_offsets(start: i64, end: i64) -> Offsets {
        Offsets::new(orders(), start, end).unwrap()
    }

    fn orders() -> Partition {
        Partition::new("orders", 0).unwrap()
    }

    fn offsets(records: &[Record]) -> Vec<i64> {
        records.iter().map(Record::offset).collect()
    }

    #[tokio::test]
    async fn a_partition_without_a_leader_is_retried_until_it_has_one() {
        let mut cluster = cluster(Script {
            leaderless_metadata_calls: 2,
            stable: 42,
            ..Script::default()
        })
        .await;

        let latest = cluster.last_stable_offset(&orders()).await;

        assert_eq!(latest.unwrap(), 42);
    }

    #[tokio::test]
    async fn a_partition_missing_from_metadata_is_unknown() {
        let mut cluster = cluster(Script::default()).await;
        let users = Partition::new("users", 0).unwrap();

        let latest = cluster.last_stable_offset(&users).await;

        assert!(
            matches!(latest, Err(Error::UnknownPartition(_))),
            "{latest:?}"
        );
    }

    #[tokio::test]
    async fn reads_never_go_past_the_last_stable_offset() {
        let mut cluster = cluster(Script {
            stable: 10,
            ..Script::default()
        })
        .await;

        let read = cluster.read(&orders_offsets(0, 11)).await;

        assert!(
            matches!(
                read,
                Err(Error::BeyondStableOffset {
                    end: 11,
                    stable: 10
                })
            ),
            "{read:?}"
        );
    }

    #[tokio::test]
    async fn a_read_moves_across_fetch_responses_and_stops_at_its_end() {
        let mut cluster = cluster(Script {
            stable: 10,
            ..Script::default()
        })
        .await;

        let read = cluster.read(&orders_offsets(2, 5)).await.unwrap();

        assert_eq!(offsets(&read), [2, 3, 4]);
    }

    #[tokio::test]
    async fn a_read_whose_fetches_stay_empty_stalls_at_the_deadline() {
        let mut cluster = cluster(Script {
            stable: 10,
            fetches: Fetches::Empty,
            ..Script::default()
        })
        .await;

        let read = cluster.read(&orders_offsets(0, 10)).await;

        assert!(
            matches!(read, Err(Error::Stalled { offset: 0 })),
            "{read:?}"
        );
    }

    #[tokio::test]
    async fn a_read_whose_fetches_never_move_forward_stalls() {
        let mut cluster = cluster(Script {
            stable: 10,
            fetches: Fetches::AlwaysAtOffsetZero,
            ..Script::default()
        })
        .await;

        let read = cluster.read(&orders_offsets(1, 10)).await;

        assert!(
            matches!(read, Err(Error::Stalled { offset: 1 })),
            "{read:?}"
        );
    }

    #[tokio::test]
    async fn a_fetch_response_without_the_partition_is_missing_partition() {
        let mut cluster = cluster(Script {
            stable: 10,
            fetches: Fetches::WithoutThePartition,
            ..Script::default()
        })
        .await;

        let read = cluster.read(&orders_offsets(0, 10)).await;

        assert!(matches!(read, Err(Error::MissingPartition(_))), "{read:?}");
    }

    #[tokio::test]
    async fn metadata_falls_back_to_the_bootstrap_addresses() {
        let mut cluster = cluster(Script {
            advertise_unreachable: true,
            ..Script::default()
        })
        .await;

        let topics = cluster.topics().await;

        assert_eq!(topics.unwrap().topics().len(), 1);
    }

    #[tokio::test]
    async fn only_external_topics_without_errors_and_with_legal_names_are_listed() {
        let mut cluster = cluster(Script::default()).await;

        let listing = cluster.topics().await.unwrap();

        assert_eq!(
            listing.topics(),
            [Topic::new("orders", Uuid::from_u128(7), 1).unwrap()]
        );
        assert!(listing.unavailable("creating"));
        assert!(!listing.unavailable("orders"));
    }

    #[tokio::test]
    async fn requests_carry_the_fields_the_spec_names() {
        let Scripted {
            mut cluster,
            requests,
        } = scripted(Script {
            stable: 1,
            ..Script::default()
        })
        .await;

        cluster.earliest(&orders()).await.unwrap();
        cluster.read(&orders_offsets(0, 1)).await.unwrap();

        let requests = requests.lock().unwrap().clone();
        let decoded = |key: ApiKey| {
            requests
                .iter()
                .filter(move |(seen, _, _)| *seen == key)
                .map(|(_, _, request)| {
                    let mut request = request.clone();
                    let header = decode_request_header_from_buffer(&mut request).unwrap();
                    (header, request)
                })
        };
        let timestamps: Vec<_> = decoded(ApiKey::ListOffsets)
            .map(|(header, mut body)| {
                let list =
                    ListOffsetsRequest::decode(&mut body, header.request_api_version).unwrap();
                assert_eq!(list.isolation_level, 1);
                assert_eq!(list.replica_id, BrokerId(-1));
                list.topics[0].partitions[0].timestamp
            })
            .collect();
        let (header, mut body) = decoded(ApiKey::Fetch).next().unwrap();
        let fetch = FetchRequest::decode(&mut body, header.request_api_version).unwrap();
        assert_eq!(timestamps, [-2, -1]);
        assert_eq!(header.client_id.as_deref(), Some("mink"));
        assert_eq!(fetch.isolation_level, 1);
        assert_eq!(fetch.replica_id, BrokerId(-1));
        assert_eq!((fetch.session_id, fetch.session_epoch), (0, -1));
        assert_eq!(fetch.min_bytes, 1);
        assert_eq!(fetch.max_wait_ms, (TIMEOUT / 10).as_millis() as i32);
        assert_eq!(
            fetch.max_bytes,
            fetch.topics[0].partitions[0].partition_max_bytes
        );
    }

    async fn refusing_broker() -> String {
        broker(|key, version, correlation, _| {
            let refused = SaslHandshakeResponse::default()
                .with_error_code(UNSUPPORTED_SASL_MECHANISM)
                .with_mechanisms(vec![StrBytes::from_static_str("PLAIN")]);

            (key == ApiKey::SaslHandshake).then(|| frame(correlation, key, version, &refused))
        })
        .await
    }

    #[tokio::test]
    async fn an_authentication_failure_ends_connect_without_retrying() {
        let address = refusing_broker().await;
        let sasl = Sasl::new(Mechanism::ScramSha512, "mink".to_owned(), "mink".to_owned());
        let config = Config::new(vec![address]).unwrap().with_sasl(sasl);
        let started = std::time::Instant::now();

        let connected = Cluster::connect(config, Metrics::new()).await;

        assert!(
            matches!(&connected, Err(Error::Unreachable(failures)) if matches!(&failures[..], [(_, Error::Authentication(message))] if message.contains("only PLAIN"))),
            "{:?}",
            connected.err()
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_broker_that_refuses_does_not_stop_the_next_from_being_tried() {
        let refusing = refusing_broker().await;
        let accepting = broker(|key, version, correlation, _| match key {
            ApiKey::SaslHandshake => Some(frame(
                correlation,
                key,
                version,
                &SaslHandshakeResponse::default(),
            )),
            ApiKey::SaslAuthenticate => Some(frame(
                correlation,
                key,
                version,
                &SaslAuthenticateResponse::default(),
            )),
            ApiKey::Metadata => Some(frame(
                correlation,
                key,
                version,
                &MetadataResponse::default(),
            )),
            _ => None,
        })
        .await;
        let sasl = Sasl::new(Mechanism::Plain, "mink".to_owned(), "mink".to_owned());
        let config = Config::new(vec![refusing, accepting])
            .unwrap()
            .with_sasl(sasl);

        let connected = Cluster::connect(config, Metrics::new()).await;

        assert!(connected.is_ok(), "{:?}", connected.err());
    }
}
