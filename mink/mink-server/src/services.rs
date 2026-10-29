use std::sync::Arc;

use mink_kafka::Pool;
use mink_metrics::Metrics;
use mink_store::Store;
use uuid::Uuid;

use crate::Error;
use crate::config::Config;
use crate::membership::Membership;
use crate::registry::Registry;
use crate::schemas::Schemas;
use crate::warehouse::Warehouse;

pub(crate) struct Services {
    pub(crate) config: Config,
    pub(crate) store: Store,
    pub(crate) warehouse: Warehouse,
    pub(crate) kafka: Pool,
    pub(crate) membership: Membership,
    pub(crate) schemas: Schemas,
    pub(crate) metrics: Metrics,
}

impl Services {
    pub(crate) async fn start(config: Config) -> Result<Arc<Services>, Error> {
        let warehouse = Warehouse::connect(&config.warehouse, &config.storage)?;

        Services::new(config, warehouse).await
    }

    pub(crate) async fn new(config: Config, warehouse: Warehouse) -> Result<Arc<Services>, Error> {
        let metrics = Metrics::new();
        let store = Store::connect(&config.postgres).await?;
        let kafka_config = config.kafka.client_config()?;
        let kafka = Pool::new(kafka_config, metrics.clone());
        let registry = config
            .schema_registry
            .clone()
            .map(|settings| Registry::new(settings, metrics.clone()))
            .transpose()?;
        let schemas = Schemas::new(store.clone(), registry);

        Ok(Arc::new(Services {
            config,
            store,
            warehouse,
            kafka,
            membership: Membership::new(Uuid::new_v4()),
            schemas,
            metrics,
        }))
    }
}
