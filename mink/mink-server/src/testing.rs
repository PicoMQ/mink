use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use iceberg::TableIdent;
use mink_core::{CatalogTable, ExternalTable, Granularity, Partition, Scope, TierSchedule, Topic};
use mink_store::Lease;
use serde_json::json;
use uuid::Uuid;

use crate::buckets::Buckets;
use crate::config::{Credentials, ExternalCatalog, History, Kafka, Storage};
use crate::external::{self, Attachment, Source};
use crate::indexer::Indexer;
use crate::services::Services;
use crate::tier::Tier;
use crate::warehouse::Warehouse;
use crate::writer::Writer;
use crate::{Config, Node, Role};

pub(crate) const TTL: Duration = Duration::from_secs(60);

static WAREHOUSES: LazyLock<Mutex<HashMap<String, Warehouse>>> = LazyLock::new(Mutex::default);

pub(crate) async fn config(roles: &[Role]) -> Option<Config> {
    let url = mink_conformance::service("TEST_POSTGRES_URL")?;
    let postgres = mink_conformance::fresh_schema(&url).await;
    let kafka_bootstrap =
        std::env::var("TEST_KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_owned());

    Some(Config {
        postgres,
        catalog_address: SocketAddr::from(([127, 0, 0, 1], 0)),
        s3_address: SocketAddr::from(([127, 0, 0, 1], 0)),
        admin_address: SocketAddr::from(([127, 0, 0, 1], 0)),
        roles: roles.to_vec(),
        kafka: Kafka {
            bootstrap: vec![kafka_bootstrap],
            tls: None,
            sasl: None,
        },
        granularity: Granularity::None,
        ..Config::default()
    })
}

pub(crate) async fn node(roles: &[Role]) -> Option<Node> {
    let config = config(roles).await?;

    Some(node_with(config).await)
}

pub(crate) async fn node_with(config: Config) -> Node {
    Node::serve(in_memory(config).await).await.unwrap()
}

pub(crate) fn storage(endpoint: String) -> Storage {
    Storage {
        endpoint: Some(endpoint),
        region: "us-east-1".to_owned(),
        credentials: Some(Credentials {
            access_key_id: "mink".to_owned(),
            secret_access_key: "minkminkmink".to_owned(),
        }),
    }
}

pub(crate) fn kafka() -> Option<String> {
    mink_conformance::service("TEST_KAFKA_BOOTSTRAP")
}

pub(crate) async fn services(change: impl FnOnce(&mut Config)) -> Option<Arc<Services>> {
    let mut config = config(&Role::ALL).await?;
    change(&mut config);

    Some(in_memory(config).await)
}

async fn in_memory(config: Config) -> Arc<Services> {
    let warehouse = WAREHOUSES
        .lock()
        .expect("the test warehouses are never poisoned")
        .entry(config.postgres.clone())
        .or_insert_with(|| Warehouse::in_memory(&config.warehouse).unwrap())
        .clone();

    let buckets = Buckets::new(config.storage.clone());

    Services::new(config, warehouse, buckets).await.unwrap()
}

pub(crate) async fn registered(services: &Services, partitions: i32) -> Topic {
    let topic = Topic::new(
        format!("t{}", Uuid::new_v4().simple()),
        Uuid::new_v4(),
        partitions,
    )
    .unwrap();

    services
        .store
        .topics()
        .add(&topic, services.config.granularity)
        .await
        .unwrap();
    topic
}

pub(crate) async fn partition_lease(services: &Services, topic: &Topic, index: i32) -> Lease {
    let scope = Scope::Indexer(Partition::new(topic.name(), index).unwrap());

    services
        .store
        .leases()
        .acquire(&scope, Uuid::from_u128(1), TTL)
        .await
        .unwrap()
        .unwrap()
}

const DAY_MS: i64 = 86_400_000;

pub(crate) struct Tiering {
    pub(crate) services: Arc<Services>,
    pub(crate) topic: Topic,
    pub(crate) bootstrap: String,
    pub(crate) catalog: Arc<external::Catalog>,
    pub(crate) identifier: TableIdent,
}

pub(crate) async fn unattached() -> Option<Tiering> {
    let bootstrap = kafka()?;
    let endpoint = mink_conformance::service("TEST_S3_ENDPOINT")?;
    let uri = mink_conformance::service("TEST_ICEBERG_REST")?;
    let services = services(|config| {
        config.history = History::None;
        config.granularity = Granularity::Hour;
        config.file_window = Duration::from_millis(1);
        config.assembly_delay = Duration::from_millis(1);
        config.storage = storage(endpoint);
        config.external_catalogs = BTreeMap::from([(
            "fixture".to_owned(),
            ExternalCatalog {
                uri: uri.clone(),
                warehouse: None,
                auth: None,
            },
        )]);
    })
    .await?;
    let topic = registered(&services, 1).await;
    mink_conformance::create_topic(&bootstrap, topic.name(), 1).await;
    reqwest::Client::new()
        .post(format!("{uri}/v1/namespaces"))
        .json(&json!({ "namespace": ["mink"] }))
        .send()
        .await
        .unwrap();

    let catalog = services.external_catalogs["fixture"].clone();
    let identifier = TableIdent::from_strs(["mink", topic.name()]).unwrap();
    Some(Tiering {
        services,
        topic,
        bootstrap,
        catalog,
        identifier,
    })
}

pub(crate) async fn tiering(interval: Duration) -> Option<Tiering> {
    let tiering = unattached().await?;
    let source = Source::new(
        false,
        Some(vec!["day(_kafka_timestamp)".parse().unwrap()]),
        vec!["_kafka_offset desc".parse().unwrap()],
        HashMap::new(),
        None,
        BTreeMap::new(),
    )
    .unwrap();

    tiering
        .attach(tiering.attachment(interval, source))
        .await
        .unwrap();
    Some(tiering)
}

impl Tiering {
    pub(crate) fn attachment(&self, interval: Duration, source: Source) -> Attachment {
        Attachment {
            topic: self.topic.name().to_owned(),
            table: CatalogTable::new("fixture", "mink", self.topic.name()),
            schedule: TierSchedule::new(Duration::from_millis(1), interval).unwrap(),
            source,
        }
    }

    pub(crate) async fn attach(
        &self,
        attachment: Attachment,
    ) -> Result<ExternalTable, crate::Error> {
        external::attach(
            &self.services.store,
            &self.services.kafka,
            &self.catalog,
            self.services.warehouse.root(),
            attachment,
        )
        .await
    }

    pub(crate) async fn index_and_commit(&self, days_ago: &[i64]) {
        let partition = Partition::new(self.topic.name(), 0).unwrap();
        let lease = partition_lease(&self.services, &self.topic, 0).await;
        let mut indexer = Indexer::start(self.services.clone(), lease, partition)
            .await
            .unwrap();

        let today = mink_core::now_ms() / DAY_MS;
        for days in days_ago {
            let timestamp = (today - days) * DAY_MS;
            let values = ["a", "b", "c"];
            mink_conformance::produce_at(&self.bootstrap, self.topic.name(), 0, &values, timestamp);
        }
        for _ in 0..2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            indexer.tick().await.unwrap();
        }

        self.writer().await.tick().await.unwrap();
    }

    pub(crate) async fn writer(&self) -> Writer {
        let scope = Scope::Writer(self.topic.name().to_owned());
        let lease = self
            .services
            .store
            .leases()
            .acquire(&scope, Uuid::from_u128(2), TTL)
            .await
            .unwrap()
            .unwrap();

        Writer::start(self.services.clone(), lease, self.topic.name().to_owned())
            .await
            .unwrap()
    }

    pub(crate) async fn tier(&self) -> Tier {
        let partition = Partition::new(self.topic.name(), 0).unwrap();
        let lease = self
            .services
            .store
            .leases()
            .acquire(&Scope::Tier(partition.clone()), Uuid::from_u128(3), TTL)
            .await
            .unwrap()
            .unwrap();

        Tier::start(self.services.clone(), lease, partition)
            .await
            .unwrap()
    }
}
