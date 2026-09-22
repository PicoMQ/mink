//! The command sink interface, its statistics, and the single-process implementation that applies locally.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::apply::apply;
use crate::command::{Command, Outcome};
use crate::error::Error;
use crate::state::State;
use crate::view::{View, ViewPublisher};

#[derive(Debug, Clone, PartialEq)]
pub struct Proposed {
    pub applied_index: u64,
    pub result: Outcome,
}

#[derive(Debug, Default)]
pub struct SinkStats {
    pub snapshot: SnapshotStats,
}

#[derive(Debug, Default)]
pub struct SnapshotStats {
    pub last_applied_index: AtomicU64,
    pub last_bytes: AtomicU64,
    pub last_duration_ms: AtomicU64,
    pub last_at_ms: AtomicU64,
    pub taken: AtomicU64,
    pub failed: AtomicU64,
}

impl SnapshotStats {
    pub fn record_success(&self, applied_index: u64, bytes: u64, duration_ms: u64, at_ms: u64) {
        self.last_applied_index
            .store(applied_index, Ordering::Relaxed);
        self.last_bytes.store(bytes, Ordering::Relaxed);
        self.last_duration_ms.store(duration_ms, Ordering::Relaxed);
        self.last_at_ms.store(at_ms, Ordering::Relaxed);
        self.taken.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
pub trait CommandSink: Send + Sync {
    async fn propose(&self, command: Command) -> Result<Proposed, Error>;

    fn stats(&self) -> Arc<SinkStats> {
        static ZERO: OnceLock<Arc<SinkStats>> = OnceLock::new();
        ZERO.get_or_init(|| Arc::new(SinkStats::default())).clone()
    }
}

pub struct LocalSink {
    state: Mutex<(State, u64)>,
    views: Arc<ViewPublisher>,
}

impl LocalSink {
    pub fn new() -> (Self, Arc<ViewPublisher>) {
        Self::with_state(State::new(), 0)
    }

    pub fn with_state(state: State, applied_index: u64) -> (Self, Arc<ViewPublisher>) {
        let views = Arc::new(ViewPublisher::with_view(View {
            applied_index,
            state: state.clone(),
        }));
        let sink = Self {
            state: Mutex::new((state, applied_index)),
            views: views.clone(),
        };

        (sink, views)
    }
}

#[async_trait]
impl CommandSink for LocalSink {
    async fn propose(&self, command: Command) -> Result<Proposed, Error> {
        let mut guard = self.state.lock().await;
        let (state, applied_index) = &mut *guard;
        let result = apply(state, &command)?;
        *applied_index += 1;

        // Published under the lock so views appear in apply order.
        self.views.publish(View {
            applied_index: *applied_index,
            state: state.clone(),
        });

        Ok(Proposed {
            applied_index: *applied_index,
            result,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(node_id: i32, node_epoch: i64) -> Command {
        Command::RegisterNode {
            node_id,
            node_epoch,
            http_address: String::new(),
            slots: 1,
            protocol_addresses: Default::default(),
        }
    }

    #[tokio::test]
    async fn propose_applies_and_publishes_in_order() {
        let (sink, views) = LocalSink::new();
        let first = sink.propose(register(1, 10)).await.unwrap();
        assert_eq!(first.applied_index, 1);
        let second = sink
            .propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(second.applied_index, 2);
        assert_eq!(second.result, Outcome::Id(0));

        let view = views.load();
        assert_eq!(view.applied_index, 2);
        assert!(view.state.get_stream(0).is_some());
    }

    #[tokio::test]
    async fn failed_propose_consumes_no_index_and_publishes_nothing() {
        let (sink, views) = LocalSink::new();
        sink.propose(register(1, 10)).await.unwrap();
        let err = sink
            .propose(Command::CreateStream {
                node_id: 9,
                node_epoch: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), 5);
        assert_eq!(views.load().applied_index, 1);
        let next = sink
            .propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(next.applied_index, 2);
    }

    #[tokio::test]
    async fn restores_from_state() {
        let (sink, _) = LocalSink::new();
        sink.propose(register(1, 10)).await.unwrap();
        sink.propose(Command::CreateStream {
            node_id: 1,
            node_epoch: 10,
        })
        .await
        .unwrap();
        let snapshot = {
            let guard = sink.state.lock().await;
            (guard.0.clone(), guard.1)
        };

        let (restored, views) = LocalSink::with_state(snapshot.0, snapshot.1);
        assert_eq!(views.load().applied_index, 2);
        let next = restored
            .propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(
            next.result,
            Outcome::Id(1),
            "id counter survived the restore"
        );
    }
}
