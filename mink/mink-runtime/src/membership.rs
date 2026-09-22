//! Live-node membership read from the heartbeat table of the metadata store.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use mink_common::Clock;
use mink_coordinator::{Error, Membership};

pub(crate) struct Store {
    store: Arc<dyn mink_sql::Store>,
    clock: Arc<dyn Clock>,
}

impl Store {
    pub(crate) fn new(store: Arc<dyn mink_sql::Store>, clock: Arc<dyn Clock>) -> Self {
        Store { store, clock }
    }
}

#[async_trait]
impl Membership for Store {
    async fn live_nodes(&self) -> Result<BTreeSet<i32>, Error> {
        self.store
            .live_nodes(self.clock.millis())
            .await
            .map(|nodes| nodes.into_iter().collect())
            .map_err(|e| Error::Membership(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mink_common::SystemClock;
    use mink_sql::{Heartbeat, HeartbeatConfig, SqliteStore};

    use super::*;

    #[tokio::test]
    async fn reflects_heartbeats_and_departures() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.db");
        let store: Arc<dyn mink_sql::Store> = Arc::new(SqliteStore::open(&path).await.unwrap());
        let membership = Store::new(
            Arc::new(SqliteStore::open(&path).await.unwrap()),
            Arc::new(SystemClock),
        );
        assert!(membership.live_nodes().await.unwrap().is_empty());

        let config = HeartbeatConfig {
            ttl_ms: 200,
            interval: Duration::from_millis(20),
        };
        let one = Heartbeat::spawn(store.clone(), 1, config.clone());
        let two = Heartbeat::spawn(store, 2, config);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while membership.live_nodes().await.unwrap() != BTreeSet::from([1, 2]) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "nodes never came alive"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        two.shutdown().await;
        assert_eq!(membership.live_nodes().await.unwrap(), BTreeSet::from([1]));
        drop(one);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(membership.live_nodes().await.unwrap().is_empty());
    }
}
