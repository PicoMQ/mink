mod confluent;
mod glue;

use std::time::Duration;

use async_trait::async_trait;
use mink_core::Part;
use mink_metrics::Metrics;
use mink_schema::{RegistrySchema, Rejection, SchemaId, SchemaKey, SchemaSet};
use mink_store::Store;

use confluent::Confluent;
use glue::Glue;

use crate::Error;
use crate::config::SchemaRegistry;

const LAGGING_REGISTRY: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(2)];

#[derive(Debug)]
pub(crate) enum Failure {
    Rejected(Rejection),
    Failed(Error),
}

impl From<Error> for Failure {
    fn from(error: Error) -> Failure {
        Failure::Failed(error)
    }
}

impl From<mink_store::Error> for Failure {
    fn from(error: mink_store::Error) -> Failure {
        Failure::Failed(error.into())
    }
}

impl From<Rejection> for Failure {
    fn from(rejection: Rejection) -> Failure {
        Failure::Rejected(rejection)
    }
}

#[async_trait]
pub(crate) trait Registry: Send + Sync {
    async fn reachable(&self) -> bool;

    async fn resolve(&self, schema_id: SchemaId) -> Result<SchemaKey, Failure>;

    async fn schema(&self, key: SchemaKey) -> Result<RegistrySchema, Failure>;

    async fn accepted(&self, topic: &str, part: Part, key: SchemaKey) -> Result<bool, Failure>;
}

pub(crate) async fn connect(
    settings: &SchemaRegistry,
    metrics: Metrics,
) -> Result<Box<dyn Registry>, Error> {
    match settings {
        SchemaRegistry::Confluent {
            url,
            username,
            password,
        } => Ok(Box::new(Confluent::new(
            url.clone(),
            username.clone(),
            password.clone(),
            metrics,
        )?)),
        SchemaRegistry::Glue { name, region } => Ok(Box::new(
            Glue::new(name.clone(), region.clone(), metrics).await,
        )),
    }
}

pub(crate) async fn stored(store: &Store, key: SchemaKey) -> Result<SchemaSet, Error> {
    SchemaSet::load(key, |next| async move {
        let stored = store.registry().get(next).await?;

        stored.ok_or(Error::UnstoredRegistrySchema(next))
    })
    .await
}

pub(crate) async fn fetch(
    store: &Store,
    registry: &dyn Registry,
    key: SchemaKey,
) -> Result<SchemaSet, Failure> {
    SchemaSet::load(key, |next| async move {
        if let Some(stored) = store.registry().get(next).await? {
            return Ok(stored);
        }

        let fetched = registry.schema(next).await?;

        store.registry().put(&fetched).await?;
        Ok(fetched)
    })
    .await
}

pub(crate) async fn retrying<T, F>(attempt: impl Fn() -> F) -> Result<T, Failure>
where
    F: Future<Output = Result<T, Failure>>,
{
    let mut answer = attempt().await;

    for wait in LAGGING_REGISTRY {
        let lagging = matches!(
            answer,
            Err(Failure::Rejected(
                Rejection::Missing | Rejection::NotAccepted
            ))
        );
        if !lagging {
            break;
        }

        tokio::time::sleep(wait).await;
        answer = attempt().await;
    }

    answer
}
