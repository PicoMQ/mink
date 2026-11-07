use iceberg::spec::Schema;
use mink_core::{ExternalTable, Granularity, TableState, Topic};
use mink_kafka::Pool;
use mink_metrics::Metrics;
use mink_store::Store;

use crate::buckets::Buckets;
use crate::config::Config;
use crate::external::{self, Attachment};
use crate::warehouse::Warehouse;
use crate::{Error, registration};

pub async fn add_table(
    config: &Config,
    topic: &str,
    granularity: Option<Granularity>,
) -> Result<Topic, Error> {
    let kafka_config = config.kafka.client_config()?;
    let kafka = Pool::new(kafka_config, Metrics::new());
    let granularity = granularity.unwrap_or(config.granularity);

    with_store(config, async |store| {
        registration::add(store, &kafka, topic, granularity).await
    })
    .await
}

pub async fn remove_table(config: &Config, topic: &str) -> Result<(), Error> {
    with_store(config, async |store| {
        Ok(store.topics().remove(topic).await?)
    })
    .await
}

pub async fn list_tables(config: &Config) -> Result<Vec<(Topic, TableState)>, Error> {
    with_store(config, async |store| Ok(store.topics().list().await?)).await
}

pub async fn set_granularity(
    config: &Config,
    topic: &str,
    granularity: Granularity,
) -> Result<i32, Error> {
    with_store(config, async |store| {
        Ok(store.time_specs().record(topic, granularity).await?)
    })
    .await
}

pub async fn attach_external_table(
    config: &Config,
    attachment: Attachment,
) -> Result<ExternalTable, Error> {
    let kafka = Pool::new(config.kafka.client_config()?, Metrics::new());
    let buckets = Buckets::new(config.storage.clone());
    let warehouse = Warehouse::connect(&config.warehouse, &buckets)?;
    let catalog = catalog(config, attachment.table.catalog(), &buckets).await?;

    with_store(config, async |store| {
        external::attach(store, &kafka, &catalog, &warehouse, attachment).await
    })
    .await
}

pub async fn detach_external_table(config: &Config, topic: &str) -> Result<(), Error> {
    with_store(config, async |store| {
        Ok(store.external_tables().detach(topic).await?)
    })
    .await
}

pub async fn table_schema(config: &Config, topic: &str) -> Result<Schema, Error> {
    with_store(config, async |store| {
        store
            .topics()
            .get(topic)
            .await?
            .ok_or_else(|| Error::NoSuchTable(topic.to_owned()))?;
        Ok(store.schemas().latest(topic).await?)
    })
    .await
}

async fn catalog(
    config: &Config,
    name: &str,
    buckets: &Buckets,
) -> Result<external::Catalog, Error> {
    let settings = config
        .external_catalogs
        .get(name)
        .ok_or_else(|| Error::NoSuchExternalCatalog(name.to_owned()))?;

    external::Catalog::new(name, settings, buckets).await
}

async fn with_store<T>(
    config: &Config,
    step: impl AsyncFnOnce(&Store) -> Result<T, Error>,
) -> Result<T, Error> {
    let store = Store::connect(&config.postgres).await?;

    let result = step(&store).await;

    store.close().await;
    result
}
