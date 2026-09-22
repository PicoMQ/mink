//! Coordinator election through a single-row lease with epochs, published as a leadership signal.

use std::mem;
use std::sync::Arc;
use std::time::Duration;

use mink_common::{Clock, SystemClock};
use tokio::sync::{Notify, watch};

use crate::store::{DEFAULT_LEASE_TTL_MS, Store, StoreError};
use crate::worker::Worker;

#[derive(Debug, Clone)]
pub struct LeaseConfig {
    pub ttl_ms: i64,
    pub check_interval: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            ttl_ms: DEFAULT_LEASE_TTL_MS,
            check_interval: Duration::from_millis(DEFAULT_LEASE_TTL_MS as u64 / 4),
        }
    }
}

pub struct Lease {
    leadership: watch::Receiver<bool>,
    worker: Worker,
}

impl Lease {
    pub fn spawn(store: Arc<dyn Store>, holder: String, config: LeaseConfig) -> Self {
        let (tx, leadership) = watch::channel(false);
        let worker = Worker::spawn(|stop| run(store, holder, config, tx, stop));

        Self { leadership, worker }
    }

    pub fn leadership(&self) -> watch::Receiver<bool> {
        self.leadership.clone()
    }

    pub async fn shutdown(self) {
        self.worker.shutdown().await;
    }
}

async fn run(
    store: Arc<dyn Store>,
    holder: String,
    config: LeaseConfig,
    tx: watch::Sender<bool>,
    stop: Arc<Notify>,
) {
    let mut held: Option<(u64, i64)> = None;
    loop {
        let now = SystemClock.millis();
        let attempt: Result<Option<u64>, StoreError> = match held {
            None => store.acquire_lease(&holder, None, now, config.ttl_ms).await,
            Some((epoch, _)) => {
                store
                    .acquire_lease(&holder, Some(epoch), now, config.ttl_ms)
                    .await
            }
        };
        match attempt {
            Ok(Some(epoch)) => {
                held = Some((epoch, now));
                tx.send_if_modified(|leader| !mem::replace(leader, true));
            }
            Ok(None) => {
                held = None;
                tx.send_if_modified(|leader| mem::replace(leader, false));
            }
            Err(error) => {
                tracing::warn!(%error, "lease store unreachable");
                if let Some((_, last_ok)) = held
                    && now - last_ok >= config.ttl_ms
                {
                    held = None;
                    tx.send_if_modified(|leader| mem::replace(leader, false));
                }
            }
        }

        tokio::select! {
            _ = stop.notified() => break,
            _ = tokio::time::sleep(config.check_interval) => {}
        }
    }

    if let Some((epoch, _)) = held {
        let _ = store.release_lease(&holder, epoch).await;
    }
    let _ = tx.send(false);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SqliteStore;

    fn fast() -> LeaseConfig {
        LeaseConfig {
            ttl_ms: 200,
            check_interval: Duration::from_millis(20),
        }
    }

    async fn wait_for(rx: &mut watch::Receiver<bool>, want: bool) {
        tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|v| *v == want))
            .await
            .expect("leadership never changed")
            .expect("keeper dropped");
    }

    #[tokio::test]
    async fn single_leader_and_handover_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease.db");
        let store_a: Arc<dyn Store> = Arc::new(SqliteStore::open(&path).await.unwrap());
        let store_b: Arc<dyn Store> = Arc::new(SqliteStore::open(&path).await.unwrap());

        let a = Lease::spawn(store_a, "node-a".into(), fast());
        let mut rx_a = a.leadership();
        wait_for(&mut rx_a, true).await;

        let b = Lease::spawn(store_b, "node-b".into(), fast());
        let mut rx_b = b.leadership();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!*rx_b.borrow(), "two leaders on one lease");
        assert!(*rx_a.borrow());

        a.shutdown().await;
        wait_for(&mut rx_b, true).await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn takeover_after_ttl_on_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lease.db");
        let store_a: Arc<dyn Store> = Arc::new(SqliteStore::open(&path).await.unwrap());
        let store_b: Arc<dyn Store> = Arc::new(SqliteStore::open(&path).await.unwrap());

        let a = Lease::spawn(store_a, "node-a".into(), fast());
        let mut rx_a = a.leadership();
        wait_for(&mut rx_a, true).await;
        drop(a);

        let b = Lease::spawn(store_b, "node-b".into(), fast());
        let mut rx_b = b.leadership();
        wait_for(&mut rx_b, true).await;
        b.shutdown().await;
    }
}
