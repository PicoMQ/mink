use std::collections::BTreeMap;
use std::sync::Arc;

use mink_kafka::Pool;
use mink_metrics::Metrics;
use mink_store::Store;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::buckets::Buckets;
use crate::config::Config;
use crate::membership::Membership;
use crate::schemas::Schemas;
use crate::tier::TierLags;
use crate::warehouse::Warehouse;
use crate::{Error, Role, external, registry};

pub(crate) struct Services {
    pub(crate) config: Config,
    pub(crate) store: Store,
    pub(crate) warehouse: Warehouse,
    pub(crate) buckets: Buckets,
    pub(crate) kafka: Pool,
    pub(crate) membership: Membership,
    pub(crate) schemas: Schemas,
    pub(crate) external_catalogs: BTreeMap<String, Arc<external::Catalog>>,
    pub(crate) tier_flushes: Semaphore,
    pub(crate) tier_lags: TierLags,
    pub(crate) metrics: Metrics,
}

impl Services {
    pub(crate) async fn start(config: Config) -> Result<Arc<Services>, Error> {
        let buckets = Buckets::new(config.storage.clone());
        let warehouse = Warehouse::connect(&config.warehouse, &buckets)?;

        Services::new(config, warehouse, buckets).await
    }

    pub(crate) async fn new(
        config: Config,
        warehouse: Warehouse,
        buckets: Buckets,
    ) -> Result<Arc<Services>, Error> {
        let metrics = Metrics::new();
        let store = Store::connect(&config.postgres).await?;
        let kafka_config = config.kafka.client_config()?;
        let kafka = Pool::new(kafka_config, metrics.clone());
        let registry = match &config.schema_registry {
            Some(settings) => Some(registry::connect(settings, metrics.clone()).await?),
            None => None,
        };
        let schemas = Schemas::new(store.clone(), registry);

        let tier_flushes = Semaphore::new(config.max_tier_flushes);
        let reaches_catalogs = [Role::Indexer, Role::Tier, Role::Admin]
            .into_iter()
            .any(|role| config.has(role));
        let mut external_catalogs = BTreeMap::new();
        for (name, catalog) in config.external_catalogs.iter().filter(|_| reaches_catalogs) {
            external_catalogs.insert(
                name.clone(),
                Arc::new(external::Catalog::new(name, catalog, &buckets).await?),
            );
        }

        Ok(Arc::new(Services {
            config,
            store,
            warehouse,
            buckets,
            kafka,
            membership: Membership::new(Uuid::new_v4()),
            schemas,
            external_catalogs,
            tier_flushes,
            tier_lags: TierLags::default(),
            metrics,
        }))
    }

    pub(crate) fn external_catalog(&self, name: &str) -> Result<Arc<external::Catalog>, Error> {
        self.external_catalogs
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NoSuchExternalCatalog(name.to_owned()))
    }
}
