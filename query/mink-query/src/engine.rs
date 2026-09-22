//! A connected engine: the cluster, the lake reader, and a DataFusion session with the catalog registered.

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use mink_client::Cluster;
use mink_lake::Reader;
use mink_lake::iceberg;

use crate::catalog::Catalog;
use crate::config::Config;
use crate::error::Result;

pub struct Engine {
    context: SessionContext,
    catalog: Arc<Catalog>,
}

impl Engine {
    pub async fn connect(config: Config) -> Result<Self> {
        let cluster = Cluster::connect_all(config.bootstrap.clone())?;
        let lake: Option<Arc<dyn Reader>> = match &config.lake {
            Some(mink_lake::Config::Iceberg(lake)) => {
                Some(Arc::new(iceberg::Catalog::connect(lake).await?))
            }
            None => None,
        };
        let catalog = Arc::new(Catalog::new(cluster, lake));

        let mut session = SessionConfig::new()
            .with_information_schema(true)
            .with_batch_size(config.batch_size)
            .with_default_catalog_and_schema(
                Config::CATALOG,
                config.database.as_deref().unwrap_or("default"),
            );
        if let Some(partitions) = config.target_partitions {
            session = session.with_target_partitions(partitions);
        }
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(limit) = config.memory_limit {
            runtime = runtime.with_memory_pool(Arc::new(FairSpillPool::new(limit)));
        }
        let context = SessionContext::new_with_config_rt(session, Arc::new(runtime.build()?));
        context.register_catalog(Config::CATALOG, catalog.clone());

        Ok(Engine { context, catalog })
    }

    pub fn context(&self) -> &SessionContext {
        &self.context
    }

    pub async fn refresh(&self) -> Result<()> {
        self.catalog.refresh().await
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        self.refresh().await?;

        Ok(self.context.sql(sql).await?)
    }
}
