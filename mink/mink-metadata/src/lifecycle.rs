//! Leader-only background loops that expire prepared objects and delete destroyed ones from storage.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mink_common::sync::lock;
use mink_common::{Clock, SystemClock};
use s3stream::{CompactOperations, ObjectPath, ObjectStorageTrait, gen_object_key};
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;

use crate::command::Command;
use crate::error::Error;
use crate::sink::CommandSink;
use crate::view::ViewPublisher;

pub(crate) const MAX_DELETE_BATCH_COUNT: usize = 2000;

const CLEAN_STEP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub(crate) enum CleanError {
    #[error("object storage delete failed: {0}")]
    Storage(String),
    #[error("metadata propose failed: {0}")]
    Metadata(#[from] Error),
    #[error("timed out after {CLEAN_STEP_TIMEOUT:?}")]
    Timeout,
}

pub struct ObjectCleaner {
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
    object_storage: Option<Arc<dyn ObjectStorageTrait>>,
}

impl ObjectCleaner {
    pub fn new(
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        object_storage: Option<Arc<dyn ObjectStorageTrait>>,
    ) -> Self {
        Self {
            sink,
            views,
            object_storage,
        }
    }

    pub(crate) async fn clean(&self, limit: usize) -> Result<Vec<u64>, CleanError> {
        let batch = limit.min(MAX_DELETE_BATCH_COUNT);
        let marked = self.views.load().state.peek_destroyed_objects(batch);
        if marked.is_empty() {
            return Ok(Vec::new());
        }

        let (keep_data, deletable): (Vec<_>, Vec<_>) = marked
            .into_iter()
            .partition(|(_, operation)| *operation == CompactOperations::KeepData);
        let mut deletable: Vec<u64> = deletable.into_iter().map(|(id, _)| id).collect();

        let mut cleaned: Vec<u64> = keep_data.into_iter().map(|(id, _)| id).collect();
        if !deletable.is_empty()
            && let Some(storage) = &self.object_storage
        {
            let bucket_id = storage.bucket_id();
            let paths: Vec<ObjectPath> = deletable
                .iter()
                .map(|id| ObjectPath {
                    bucket_id,
                    key: gen_object_key(0, *id),
                })
                .collect();
            tokio::time::timeout(CLEAN_STEP_TIMEOUT, storage.delete(&paths))
                .await
                .map_err(|_| CleanError::Timeout)?
                .map_err(|e| CleanError::Storage(e.to_string()))?;
            cleaned.append(&mut deletable);
        }

        if cleaned.is_empty() {
            return Ok(Vec::new());
        }

        tokio::time::timeout(
            CLEAN_STEP_TIMEOUT,
            self.sink.propose(Command::CleanDestroyedObjects {
                object_ids: cleaned.clone(),
            }),
        )
        .await
        .map_err(|_| CleanError::Timeout)??;

        Ok(cleaned)
    }
}

pub struct Lifecycle {
    sink: Arc<dyn CommandSink>,
    cleaner: Arc<ObjectCleaner>,
    tick: Duration,
    leader: Arc<AtomicBool>,
    loops: Mutex<Vec<JoinHandle<()>>>,
}

impl Lifecycle {
    pub fn new(sink: Arc<dyn CommandSink>, cleaner: Arc<ObjectCleaner>, tick: Duration) -> Self {
        Self {
            sink,
            cleaner,
            tick,
            leader: Arc::new(AtomicBool::new(false)),
            loops: Mutex::new(Vec::new()),
        }
    }

    pub fn on_leader_start(&self) {
        if self.leader.swap(true, Ordering::SeqCst) {
            return;
        }

        let mut loops = lock(&self.loops);
        loops.push(tokio::spawn(expire_loop(
            self.sink.clone(),
            self.leader.clone(),
            self.tick,
        )));
        loops.push(tokio::spawn(clean_loop(
            self.cleaner.clone(),
            self.leader.clone(),
            self.tick,
        )));
    }

    pub fn on_leader_stop(&self) {
        if self.leader.swap(false, Ordering::SeqCst) {
            self.abort_loops();
        }
    }

    fn abort_loops(&self) {
        for task in lock(&self.loops).drain(..) {
            task.abort();
        }
    }

    pub fn drive(self: Arc<Self>, mut leadership: Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if *leadership.borrow_and_update() {
                    self.on_leader_start();
                } else {
                    self.on_leader_stop();
                }
                if leadership.changed().await.is_err() {
                    self.on_leader_stop();
                    return;
                }
            }
        })
    }
}

impl Drop for Lifecycle {
    fn drop(&mut self) {
        self.leader.store(false, Ordering::SeqCst);
        self.abort_loops();
    }
}

async fn expire_loop(sink: Arc<dyn CommandSink>, leader: Arc<AtomicBool>, tick: Duration) {
    loop {
        tokio::time::sleep(tick).await;
        if !leader.load(Ordering::SeqCst) {
            return;
        }
        if let Err(error) = sink
            .propose(Command::ExpirePreparedObjects {
                now_ms: SystemClock.millis(),
            })
            .await
        {
            tracing::debug!(%error, "expire prepared objects failed");
        }
    }
}

async fn clean_loop(cleaner: Arc<ObjectCleaner>, leader: Arc<AtomicBool>, tick: Duration) {
    loop {
        tokio::time::sleep(tick).await;
        if !leader.load(Ordering::SeqCst) {
            return;
        }
        match cleaner.clean(MAX_DELETE_BATCH_COUNT).await {
            Err(error) => {
                tracing::warn!(%error, "object cleaner failed, destroyed marks retained")
            }
            Ok(_) => {
                let backlog = cleaner.views.load().state.mark_destroyed.len();
                if backlog > MAX_DELETE_BATCH_COUNT {
                    tracing::warn!(
                        backlog,
                        "gc backlog exceeds one clean batch; falling behind"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use s3stream::MemoryObjectStorage;

    use super::*;
    use crate::command::Outcome;
    use crate::sink::LocalSink;

    const NODE: i32 = 1;
    const EPOCH: i64 = 1;

    async fn registered_sink() -> (Arc<LocalSink>, Arc<ViewPublisher>) {
        let (sink, views) = LocalSink::new();
        let sink = Arc::new(sink);
        sink.propose(Command::RegisterNode {
            node_id: NODE,
            node_epoch: EPOCH,
            http_address: String::new(),
            slots: 1,
            protocol_addresses: Default::default(),
        })
        .await
        .unwrap();
        (sink, views)
    }

    async fn commit_object(sink: &Arc<LocalSink>, stream_id: u64, object_id: u64) {
        sink.propose(Command::PrepareObject {
            node_id: NODE,
            node_epoch: EPOCH,
            count: 1,
            ttl_ms: 60_000,
            now_ms: 0,
        })
        .await
        .unwrap();
        sink.propose(Command::CompactStreamObject {
            node_id: NODE,
            node_epoch: EPOCH,
            request: s3stream::CompactStreamObjectRequest {
                object_id,
                object_size: 10,
                stream_id,
                stream_epoch: 1,
                start_offset: 0,
                end_offset: 0,
                source_object_ids: Vec::new(),
                operations: Vec::new(),
                attributes: 0,
            },
            now_ms: 0,
        })
        .await
        .unwrap();
    }

    async fn destroyed_object_fixture(sink: &Arc<LocalSink>) -> u64 {
        let stream_id = match sink
            .propose(Command::CreateStream {
                node_id: NODE,
                node_epoch: EPOCH,
            })
            .await
            .unwrap()
            .result
        {
            Outcome::Id(id) => id,
            other => panic!("unexpected {other:?}"),
        };
        sink.propose(Command::OpenStream {
            node_id: NODE,
            node_epoch: EPOCH,
            stream_id,
            epoch: 1,
        })
        .await
        .unwrap();
        commit_object(sink, stream_id, 0).await;
        sink.propose(Command::CloseStream {
            node_id: NODE,
            node_epoch: EPOCH,
            stream_id,
            epoch: 1,
        })
        .await
        .unwrap();
        sink.propose(Command::DeleteStream {
            node_id: NODE,
            node_epoch: EPOCH,
            stream_id,
            epoch: 1,
        })
        .await
        .unwrap();
        0
    }

    #[tokio::test]
    async fn clean_deletes_and_unmarks() {
        let (sink, views) = registered_sink().await;
        let object_id = destroyed_object_fixture(&sink).await;
        assert_eq!(views.load().state.peek_destroyed_objects(10).len(), 1);

        let storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
        let cleaner = ObjectCleaner::new(sink.clone(), views.clone(), Some(storage));
        let cleaned = cleaner.clean(MAX_DELETE_BATCH_COUNT).await.unwrap();
        assert_eq!(cleaned, vec![object_id]);
        assert!(views.load().state.peek_destroyed_objects(10).is_empty());

        assert!(
            cleaner
                .clean(MAX_DELETE_BATCH_COUNT)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn clean_without_storage_retains_deletable() {
        let (sink, views) = registered_sink().await;
        destroyed_object_fixture(&sink).await;

        let cleaner = ObjectCleaner::new(sink.clone(), views.clone(), None);
        assert!(
            cleaner
                .clean(MAX_DELETE_BATCH_COUNT)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(views.load().state.peek_destroyed_objects(10).len(), 1);
    }

    #[tokio::test]
    async fn lifecycle_expires_prepared_objects_while_leader() {
        let (sink, views) = registered_sink().await;
        sink.propose(Command::PrepareObject {
            node_id: NODE,
            node_epoch: EPOCH,
            count: 1,
            ttl_ms: 1,
            now_ms: 0,
        })
        .await
        .unwrap();

        let cleaner = Arc::new(ObjectCleaner::new(sink.clone(), views.clone(), None));
        let lifecycle = Lifecycle::new(sink.clone(), cleaner, Duration::from_millis(5));
        lifecycle.on_leader_start();
        lifecycle.on_leader_start();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if views.load().state.prepared_objects_count() == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "expire loop never ran"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        lifecycle.on_leader_stop();
        lifecycle.on_leader_stop();
    }

    #[tokio::test]
    async fn drive_follows_leadership_watch() {
        let (sink, views) = registered_sink().await;
        sink.propose(Command::PrepareObject {
            node_id: NODE,
            node_epoch: EPOCH,
            count: 1,
            ttl_ms: 1,
            now_ms: 0,
        })
        .await
        .unwrap();

        let cleaner = Arc::new(ObjectCleaner::new(sink.clone(), views.clone(), None));
        let lifecycle = Arc::new(Lifecycle::new(
            sink.clone(),
            cleaner,
            Duration::from_millis(5),
        ));
        let (tx, rx) = tokio::sync::watch::channel(false);
        let driver = lifecycle.clone().drive(rx);

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(views.load().state.prepared_objects_count(), 1);

        tx.send(true).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while views.load().state.prepared_objects_count() != 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "expire loop never ran"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        drop(tx);
        driver.await.unwrap();
    }
}
