use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use mink_core::{Granularity, Partition, Scope, Topic};
use mink_store::Lease;
use uuid::Uuid;

use crate::services::Services;
use crate::warehouse::Warehouse;
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
        kafka_bootstrap: vec![kafka_bootstrap],
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

    Services::new(config, warehouse).await.unwrap()
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
    let scope = Scope::Partition(Partition::new(topic.name(), index).unwrap());

    services
        .store
        .leases()
        .acquire(&scope, Uuid::from_u128(1), TTL)
        .await
        .unwrap()
        .unwrap()
}
