use mink_core::{Granularity, TableState, Topic};
use mink_kafka::Pool;
use mink_metrics::Metrics;
use mink_store::Store;

use crate::Error;
use crate::config::Config;
use crate::registration;

pub async fn add_table(
    config: &Config,
    topic: &str,
    granularity: Option<Granularity>,
) -> Result<Topic, Error> {
    let kafka_config = mink_kafka::Config::new(config.kafka_bootstrap.clone())?;
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

async fn with_store<T>(
    config: &Config,
    step: impl AsyncFnOnce(&Store) -> Result<T, Error>,
) -> Result<T, Error> {
    let store = Store::connect(&config.postgres).await?;

    let result = step(&store).await;

    store.close().await;
    result
}
