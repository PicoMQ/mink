//! A node's periodic liveness heartbeat into the store, expired on shutdown.

use std::sync::Arc;
use std::time::Duration;

use mink_common::{Clock, SystemClock};
use tokio::sync::Notify;

use crate::store::{DEFAULT_LEASE_TTL_MS, Store};
use crate::worker::Worker;

#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub ttl_ms: i64,
    pub interval: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        HeartbeatConfig {
            ttl_ms: DEFAULT_LEASE_TTL_MS,
            interval: Duration::from_millis(DEFAULT_LEASE_TTL_MS as u64 / 4),
        }
    }
}

pub struct Heartbeat {
    worker: Worker,
}

impl Heartbeat {
    pub fn spawn(store: Arc<dyn Store>, node_id: i32, config: HeartbeatConfig) -> Self {
        Heartbeat {
            worker: Worker::spawn(|stop| beat(store, node_id, config, stop)),
        }
    }

    pub async fn shutdown(self) {
        self.worker.shutdown().await;
    }
}

async fn beat(store: Arc<dyn Store>, node_id: i32, config: HeartbeatConfig, stop: Arc<Notify>) {
    loop {
        if let Err(error) = store
            .heartbeat_node(node_id, SystemClock.millis(), config.ttl_ms)
            .await
        {
            tracing::warn!(%error, node_id, "node heartbeat failed");
        }

        tokio::select! {
            _ = stop.notified() => break,
            _ = tokio::time::sleep(config.interval) => {}
        }
    }

    let _ = store.expire_node(node_id).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SqliteStore;

    #[tokio::test]
    async fn heartbeats_keep_the_node_alive_and_shutdown_expires_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.db");
        let store: Arc<dyn Store> = Arc::new(SqliteStore::open(&path).await.unwrap());
        let observer: Arc<dyn Store> = Arc::new(SqliteStore::open(&path).await.unwrap());
        let config = HeartbeatConfig {
            ttl_ms: 200,
            interval: Duration::from_millis(20),
        };

        let heartbeat = Heartbeat::spawn(store, 7, config);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if observer.live_nodes(SystemClock.millis()).await.unwrap() == vec![7] {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node never came alive"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            observer.live_nodes(SystemClock.millis()).await.unwrap(),
            vec![7]
        );

        heartbeat.shutdown().await;
        assert_eq!(
            observer.live_nodes(SystemClock.millis()).await.unwrap(),
            Vec::<i32>::new()
        );
    }
}
