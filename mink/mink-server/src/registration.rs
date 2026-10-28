use mink_core::{Granularity, Scope, TableLocation, Topic};
use mink_kafka::Pool;
use mink_store::Store;
use uuid::Uuid;

use crate::Error;
use crate::owners::{owner, scope_key};
use crate::services::Services;

pub(crate) async fn add(
    store: &Store,
    kafka: &Pool,
    name: &str,
    granularity: Granularity,
) -> Result<Topic, Error> {
    let listing = kafka.cluster().await?.topics().await?;
    let topic = listing
        .find(name)
        .cloned()
        .ok_or_else(|| Error::NoSuchTopic(name.to_owned()))?;

    store.topics().add(&topic, granularity).await?;

    Ok(topic)
}

pub(crate) async fn register(services: &Services) -> Result<Vec<Topic>, Error> {
    let listing = services.kafka.cluster().await?.topics().await?;
    let topics = services.store.topics();

    for (table, state) in topics.list().await? {
        if listing.unavailable(table.name()) {
            continue;
        }

        match topics
            .refresh(table.name(), listing.find(table.name()))
            .await
        {
            Ok(next) if next != state => {
                tracing::warn!(table = %table.name(), from = state.name(), to = next.name(), "the table changed state");
            }
            Ok(_) | Err(mink_store::Error::NoTable { .. }) => {}
            Err(error @ mink_store::Error::FewerPartitions { .. }) => {
                tracing::warn!(%error, "the table keeps its partitions");
            }
            Err(error) => return Err(error.into()),
        }
    }

    Ok(topics.indexing().await?)
}

pub(crate) async fn delete_removed(services: &Services, indexers: &[Uuid]) -> Result<(), Error> {
    let topics = services.store.topics();
    let warehouse = &services.warehouse;
    let node = services.membership.id();

    for name in topics.removable(services.config.lease_ttl).await? {
        let writer = owner(indexers, &scope_key(&Scope::Topic(name.clone())));
        if writer != Some(node) {
            continue;
        }

        let location = TableLocation::new(warehouse.root(), &name);
        warehouse
            .delete_under(&format!("{}/", location.path()))
            .await?;

        topics.delete(&name).await?;
        tracing::info!(table = %name, "the table was removed");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use mink_core::{Granularity, TableState};
    use uuid::Uuid;

    use super::*;
    use crate::testing::{self, kafka};

    #[tokio::test]
    async fn registration_follows_kafka() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let ttl = Duration::from_millis(50);
        let Some(services) = testing::services(|config| config.lease_ttl = ttl).await else {
            return;
        };
        let topics = services.store.topics();
        let name = |prefix: &str| format!("{prefix}{}", Uuid::new_v4().simple());
        let (grown, replaced, missing, removed) = (name("g"), name("r"), name("m"), name("d"));

        mink_conformance::create_topic(&bootstrap, &grown, 2).await;
        mink_conformance::create_topic(&bootstrap, &replaced, 1).await;
        let listing = services.kafka.cluster().await.unwrap().topics().await;
        let grown_uuid = listing.unwrap().find(&grown).unwrap().uuid();

        for topic in [
            Topic::new(&grown, grown_uuid, 1).unwrap(),
            Topic::new(&replaced, Uuid::new_v4(), 1).unwrap(),
            Topic::new(&missing, Uuid::new_v4(), 1).unwrap(),
            Topic::new(&removed, Uuid::new_v4(), 1).unwrap(),
        ] {
            topics.add(&topic, Granularity::Hour).await.unwrap();
        }

        let object = format!("s3://mink/warehouse/{removed}/metadata/v1-e1.metadata.json");
        let warehouse = &services.warehouse;
        warehouse
            .write(&object, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        topics.remove(&removed).await.unwrap();
        tokio::time::sleep(ttl * 2).await;

        let indexing = register(&services).await.unwrap();
        let elsewhere = delete_removed(&services, &[Uuid::new_v4()]).await;
        let kept = topics
            .table(&removed)
            .await
            .unwrap()
            .map(|(_, state)| state);
        delete_removed(&services, &[services.membership.id()])
            .await
            .unwrap();

        let state = async |name: &str| topics.table(name).await.unwrap().map(|(_, state)| state);
        assert!(indexing.iter().any(|topic| topic.name() == grown));
        assert_eq!(
            topics.get(&grown).await.unwrap().unwrap().partition_count(),
            2
        );
        assert_eq!(state(&replaced).await, Some(TableState::Replaced));
        assert_eq!(state(&missing).await, Some(TableState::Missing));
        assert!(elsewhere.is_ok());
        assert_eq!(kept, Some(TableState::Removing));
        assert_eq!(state(&removed).await, None);
        assert!(warehouse.read(&object).await.is_err());
    }
}
