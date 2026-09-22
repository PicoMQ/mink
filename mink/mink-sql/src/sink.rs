//! The command sink: batches proposals into log rows, tails the log to apply them, and snapshots the state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mink_common::sync::lock;
use mink_common::{Clock, SystemClock};
use mink_metadata::{
    CodecError, Command, CommandSink, Error as MetadataError, Outcome, Proposed, SinkStats, State,
    View, ViewPublisher, apply, codec, snapshot,
};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::store::{Store, StoreError};

type BatchResults = Vec<Result<Outcome, MetadataError>>;
type Waiter = oneshot::Sender<Result<Proposed, MetadataError>>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("snapshot: {0}")]
    Snapshot(#[from] CodecError),
    #[error("corrupt log at idx {idx}: {error}")]
    CorruptLog { idx: u64, error: CodecError },
    #[error("log truncated below idx {idx} but no snapshot covers the gap")]
    TruncatedWithoutSnapshot { idx: u64 },
}

#[derive(Debug, Clone)]
pub struct Config {
    pub poll_interval: Duration,
    pub max_batch: usize,
    pub fetch_limit: u32,
    pub queue_depth: usize,
    pub snapshot_every: u64,
    pub snapshot_min_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(10),
            max_batch: 256,
            fetch_limit: 1024,
            queue_depth: 4096,
            snapshot_every: 1024,
            snapshot_min_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Default)]
struct Pending {
    poisoned: bool,
    map: HashMap<u64, oneshot::Sender<BatchResults>>,
}

struct Shared {
    store: Arc<dyn Store>,
    views: Arc<ViewPublisher>,
    pending: Mutex<Pending>,
    last_seen: AtomicU64,
    nudge: Notify,
    stats: Arc<SinkStats>,
}

impl Shared {
    fn poison(&self, applied: u64, message: &str) {
        tracing::error!(applied, message, "halting sink");
        let mut pending = lock(&self.pending);
        pending.poisoned = true;
        pending.map.clear();
    }

    fn forget(&self, idx: u64) {
        lock(&self.pending).map.remove(&idx);
    }

    fn publish(&self, applied_index: u64, state: &State) {
        self.last_seen.fetch_max(applied_index, Ordering::SeqCst);
        self.views.publish(View {
            applied_index,
            state: state.clone(),
        });
    }
}

type ProposeRequest = (Command, Waiter);

pub struct Sink {
    shared: Arc<Shared>,
    queue: mpsc::Sender<ProposeRequest>,
    tasks: Vec<JoinHandle<()>>,
}

impl Sink {
    pub async fn open(
        store: Arc<dyn Store>,
        config: Config,
    ) -> Result<(Self, Arc<ViewPublisher>), Error> {
        let (mut state, mut applied) = match store.load_snapshot().await? {
            Some((idx, payload)) => (snapshot::decode(&payload)?, idx),
            None => (State::new(), 0),
        };
        let mut snapshot_base = applied;
        'replay: loop {
            let rows = store.fetch_after(applied, config.fetch_limit).await?;
            if rows.is_empty() {
                break;
            }
            for (idx, payload) in rows {
                if idx > applied + 1 {
                    let (snap_idx, snap_payload) = store
                        .load_snapshot()
                        .await?
                        .ok_or(Error::TruncatedWithoutSnapshot { idx })?;
                    state = snapshot::decode(&snap_payload)?;
                    applied = snap_idx;
                    snapshot_base = snap_idx;
                    continue 'replay;
                }
                let commands = codec::decode_batch(&payload)
                    .map_err(|error| Error::CorruptLog { idx, error })?;
                for command in &commands {
                    let _ = apply(&mut state, command);
                }
                applied = idx;
            }
        }

        let views = Arc::new(ViewPublisher::with_view(View {
            applied_index: applied,
            state: state.clone(),
        }));
        let shared = Arc::new(Shared {
            store,
            views: views.clone(),
            pending: Mutex::new(Pending::default()),
            last_seen: AtomicU64::new(applied),
            nudge: Notify::new(),
            stats: Arc::new(SinkStats::default()),
        });

        let (queue, queue_rx) = mpsc::channel(config.queue_depth);
        let tasks = vec![
            tokio::spawn(tail(shared.clone(), state, applied, config.clone())),
            tokio::spawn(snapshot_loop(shared.clone(), snapshot_base, config.clone())),
            tokio::spawn(flush(shared.clone(), queue_rx, config)),
        ];

        Ok((
            Self {
                shared,
                queue,
                tasks,
            },
            views,
        ))
    }

    pub fn views(&self) -> Arc<ViewPublisher> {
        self.shared.views.clone()
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[async_trait]
impl CommandSink for Sink {
    async fn propose(&self, command: Command) -> Result<Proposed, MetadataError> {
        let (tx, rx) = oneshot::channel();
        self.queue
            .send((command, tx))
            .await
            .map_err(|_| MetadataError::Unexpected {
                message: "sql sink is shut down".into(),
            })?;

        rx.await.map_err(|_| MetadataError::Unexpected {
            message: "sql sink dropped the proposal (shutdown or log corruption)".into(),
        })?
    }

    fn stats(&self) -> Arc<SinkStats> {
        self.shared.stats.clone()
    }
}

async fn tail(shared: Arc<Shared>, mut state: State, mut applied: u64, config: Config) {
    'tail: loop {
        let rows = match shared.store.fetch_after(applied, config.fetch_limit).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, "metadata log fetch failed; retrying");
                tokio::time::sleep(config.poll_interval).await;
                continue;
            }
        };
        if rows.is_empty() {
            if let Ok(Some(snap_idx)) = shared.store.snapshot_idx().await
                && snap_idx > applied
            {
                if !recover(&shared, &mut state, &mut applied, &config).await {
                    return;
                }
                continue;
            }

            tokio::select! {
                _ = shared.nudge.notified() => {}
                _ = tokio::time::sleep(config.poll_interval) => {}
            }
            continue;
        }

        for (idx, payload) in rows {
            if idx > applied + 1 {
                if !recover(&shared, &mut state, &mut applied, &config).await {
                    return;
                }
                continue 'tail;
            }

            let commands = match codec::decode_batch(&payload) {
                Ok(commands) => commands,
                Err(error) => {
                    tracing::error!(idx, %error, "corrupt metadata log row");
                    shared.poison(applied, "corrupt metadata log row");
                    return;
                }
            };
            let results: BatchResults = commands
                .iter()
                .map(|command| apply(&mut state, command))
                .collect();
            applied = idx;

            // Published before results so a returning proposer sees its own write.
            shared.publish(idx, &state);
            if let Some(waiter) = lock(&shared.pending).map.remove(&idx) {
                let _ = waiter.send(results);
            }
        }
    }
}

async fn recover(shared: &Shared, state: &mut State, applied: &mut u64, config: &Config) -> bool {
    match restore(shared, *applied).await {
        Restore::Installed(snap_idx, snap_state) => {
            *state = *snap_state;
            *applied = snap_idx;
        }
        Restore::Retry => tokio::time::sleep(config.poll_interval).await,
        Restore::Poisoned => return false,
    }

    true
}

async fn snapshot_loop(shared: Arc<Shared>, mut last: u64, config: Config) {
    if config.snapshot_every == 0 {
        return;
    }
    loop {
        shared
            .views
            .wait_applied(last + config.snapshot_every)
            .await;
        let view = shared.views.load();
        let applied = view.applied_index;
        let started = Instant::now();
        let state = view.state.clone();
        let payload = tokio::task::spawn_blocking(move || snapshot::encode(&state))
            .await
            .expect("snapshot encode panicked");
        match shared.store.store_snapshot(applied, &payload).await {
            Ok(()) => {
                last = applied;
                shared.stats.snapshot.record_success(
                    applied,
                    payload.len() as u64,
                    started.elapsed().as_millis() as u64,
                    SystemClock.millis().max(0) as u64,
                );
                if let Err(error) = shared.store.truncate_log(applied).await {
                    tracing::warn!(%error, "log truncation failed; retrying next cycle");
                }
                tokio::time::sleep(config.snapshot_min_interval).await;
            }
            Err(error) => {
                shared.stats.snapshot.record_failure();
                tracing::warn!(%error, "snapshot store failed; retrying next cycle");
                tokio::time::sleep(config.poll_interval).await;
            }
        }
    }
}

enum Restore {
    Installed(u64, Box<State>),
    Retry,
    Poisoned,
}

async fn restore(shared: &Shared, applied: u64) -> Restore {
    let poison = |message: &str| {
        shared.poison(applied, message);
        Restore::Poisoned
    };

    let snapshot = match shared.store.load_snapshot().await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(%error, "snapshot load failed during gap recovery; retrying");
            return Restore::Retry;
        }
    };
    let Some((snap_idx, payload)) = snapshot else {
        return poison("log truncated but no snapshot exists");
    };
    if snap_idx <= applied {
        return poison("log truncated beyond the stored snapshot");
    }
    let state = match snapshot::decode(&payload) {
        Ok(state) => state,
        Err(error) => {
            tracing::error!(%error, "snapshot decode failed");
            return poison("corrupt snapshot during gap recovery");
        }
    };

    shared.publish(snap_idx, &state);
    lock(&shared.pending).map.retain(|&idx, _| idx > snap_idx);

    Restore::Installed(snap_idx, Box::new(state))
}

async fn flush(shared: Arc<Shared>, mut queue: mpsc::Receiver<ProposeRequest>, config: Config) {
    while let Some(first) = queue.recv().await {
        let mut commands = Vec::with_capacity(8);
        let mut waiters = Vec::with_capacity(8);
        commands.push(first.0);
        waiters.push(first.1);
        while commands.len() < config.max_batch {
            match queue.try_recv() {
                Ok((command, waiter)) => {
                    commands.push(command);
                    waiters.push(waiter);
                }
                Err(_) => break,
            }
        }

        let payload = codec::encode_batch(&commands);
        commit(&shared, &payload, commands.len(), waiters).await;
    }
}

async fn commit(shared: &Shared, payload: &[u8], command_count: usize, waiters: Vec<Waiter>) {
    loop {
        let next = shared.last_seen.load(Ordering::SeqCst) + 1;

        // Registered before append: the tailer may apply the row before append returns.
        let rx = {
            let mut pending = lock(&shared.pending);
            if pending.poisoned {
                fail_all(waiters, "metadata log is poisoned (corrupt row)");
                return;
            }
            let (tx, rx) = oneshot::channel();
            pending.map.insert(next, tx);
            rx
        };
        match shared.store.append(next, payload).await {
            Ok(true) => {
                shared.nudge.notify_waiters();
                match rx.await {
                    Ok(results) => {
                        debug_assert_eq!(results.len(), command_count);
                        for (waiter, result) in waiters.into_iter().zip(results) {
                            let _ = waiter.send(result.map(|result| Proposed {
                                applied_index: next,
                                result,
                            }));
                        }
                    }
                    Err(_) => fail_all(waiters, "metadata log is poisoned (corrupt row)"),
                }
                return;
            }
            Ok(false) => {
                shared.forget(next);
                match shared.store.last_idx().await {
                    Ok(last) => {
                        shared.last_seen.fetch_max(last, Ordering::SeqCst);
                    }
                    Err(error) => {
                        fail_all(waiters, &format!("refreshing log tail failed: {error}"));
                        return;
                    }
                }
            }
            Err(error) => {
                shared.forget(next);
                fail_all(waiters, &format!("append failed: {error}"));
                return;
            }
        }
    }
}

fn fail_all(waiters: Vec<Waiter>, message: &str) {
    for waiter in waiters {
        let _ = waiter.send(Err(MetadataError::Unexpected {
            message: message.to_owned(),
        }));
    }
}

#[cfg(test)]
mod tests {
    use mink_metadata::LocalSink;

    use super::*;
    use crate::store::SqliteStore;

    fn fast() -> Config {
        Config {
            poll_interval: Duration::from_millis(1),
            snapshot_min_interval: Duration::ZERO,
            ..Config::default()
        }
    }

    async fn memory() -> (Sink, Arc<ViewPublisher>) {
        let store = Arc::new(SqliteStore::memory().await.unwrap());
        Sink::open(store, fast()).await.unwrap()
    }

    struct Gated {
        inner: SqliteStore,
        release: tokio::sync::watch::Receiver<bool>,
    }

    #[async_trait]
    impl Store for Gated {
        async fn append(&self, idx: u64, payload: &[u8]) -> Result<bool, StoreError> {
            self.inner.append(idx, payload).await
        }
        async fn last_idx(&self) -> Result<u64, StoreError> {
            self.inner.last_idx().await
        }
        async fn fetch_after(
            &self,
            after: u64,
            limit: u32,
        ) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
            self.inner.fetch_after(after, limit).await
        }
        async fn load_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
            self.inner.load_snapshot().await
        }
        async fn snapshot_idx(&self) -> Result<Option<u64>, StoreError> {
            self.inner.snapshot_idx().await
        }
        async fn store_snapshot(&self, applied_idx: u64, payload: &[u8]) -> Result<(), StoreError> {
            let mut release = self.release.clone();
            release
                .wait_for(|open| *open)
                .await
                .expect("release sender dropped");
            self.inner.store_snapshot(applied_idx, payload).await
        }
        async fn truncate_log(&self, up_to: u64) -> Result<(), StoreError> {
            self.inner.truncate_log(up_to).await
        }
        async fn acquire_lease(
            &self,
            holder: &str,
            prev_epoch: Option<u64>,
            now_ms: i64,
            ttl_ms: i64,
        ) -> Result<Option<u64>, StoreError> {
            self.inner
                .acquire_lease(holder, prev_epoch, now_ms, ttl_ms)
                .await
        }
        async fn release_lease(&self, holder: &str, epoch: u64) -> Result<(), StoreError> {
            self.inner.release_lease(holder, epoch).await
        }

        async fn heartbeat_node(
            &self,
            node_id: i32,
            now_ms: i64,
            ttl_ms: i64,
        ) -> Result<(), StoreError> {
            self.inner.heartbeat_node(node_id, now_ms, ttl_ms).await
        }

        async fn expire_node(&self, node_id: i32) -> Result<(), StoreError> {
            self.inner.expire_node(node_id).await
        }

        async fn live_nodes(&self, now_ms: i64) -> Result<Vec<i32>, StoreError> {
            self.inner.live_nodes(now_ms).await
        }
    }

    #[tokio::test]
    async fn apply_never_waits_on_snapshot_store() {
        let (release, gate) = tokio::sync::watch::channel(false);
        let store = Arc::new(Gated {
            inner: SqliteStore::memory().await.unwrap(),
            release: gate,
        });
        let config = Config {
            snapshot_every: 5,
            ..fast()
        };
        let (sink, views) = Sink::open(store.clone(), config).await.unwrap();

        sink.propose(register(1, 10)).await.unwrap();
        for _ in 0..30 {
            sink.propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        }
        let applied = views.load().applied_index;
        assert!(applied >= 31, "apply stalled at {applied}");
        assert_eq!(store.inner.snapshot_idx().await.unwrap(), None);

        release.send(true).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshotted = store.inner.snapshot_idx().await.unwrap().is_some();
            let rows = store.inner.fetch_after(0, 1024).await.unwrap();
            if snapshotted && (rows.len() as u64) < applied {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "released snapshot cycle never completed"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn register(node_id: i32, node_epoch: i64) -> Command {
        Command::RegisterNode {
            node_id,
            node_epoch,
            http_address: String::new(),
            slots: 1,
            protocol_addresses: Default::default(),
        }
    }

    fn workload() -> Vec<Command> {
        vec![
            register(1, 10),
            register(2, 20),
            Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            },
            Command::CreateStream {
                node_id: 2,
                node_epoch: 20,
            },
            Command::OpenStream {
                node_id: 1,
                node_epoch: 10,
                stream_id: 0,
                epoch: 1,
            },
            Command::OpenStream {
                node_id: 1,
                node_epoch: 9,
                stream_id: 1,
                epoch: 1,
            },
            Command::OpenStream {
                node_id: 2,
                node_epoch: 20,
                stream_id: 1,
                epoch: 1,
            },
            Command::PrepareObject {
                node_id: 1,
                node_epoch: 10,
                count: 3,
                ttl_ms: 60_000,
                now_ms: 5,
            },
            Command::PutKv {
                key: "a".into(),
                value: bytes::Bytes::from_static(b"1"),
            },
            Command::PutKvIfAbsent {
                key: "a".into(),
                value: bytes::Bytes::from_static(b"2"),
            },
            Command::DeleteKv { key: "a".into() },
            Command::CloseStream {
                node_id: 1,
                node_epoch: 10,
                stream_id: 0,
                epoch: 1,
            },
            Command::DeleteStream {
                node_id: 1,
                node_epoch: 10,
                stream_id: 0,
                epoch: 1,
            },
        ]
    }

    #[tokio::test]
    async fn equivalent_to_local_sink() {
        let (local, local_views) = LocalSink::new();
        let (sql, sql_views) = memory().await;
        for command in workload() {
            let local_result = local.propose(command.clone()).await.map(|p| p.result);
            let sql_result = sql.propose(command).await.map(|p| p.result);
            assert_eq!(local_result, sql_result);
        }
        assert_eq!(local_views.load().state, sql_views.load().state);
    }

    #[tokio::test]
    async fn propose_publishes_before_returning() {
        let (sink, views) = memory().await;
        sink.propose(register(1, 10)).await.unwrap();
        let proposed = sink
            .propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(proposed.result, Outcome::Id(0));
        let view = views.load();
        assert!(view.applied_index >= proposed.applied_index);
        assert!(view.state.get_stream(0).is_some());
    }

    #[tokio::test]
    async fn restart_replays_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        {
            let store = Arc::new(SqliteStore::open(&path).await.unwrap());
            let (sink, _) = Sink::open(store, fast()).await.unwrap();
            sink.propose(register(1, 10)).await.unwrap();
            sink.propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
            sink.propose(Command::PutKv {
                key: "k".into(),
                value: bytes::Bytes::from_static(b"v"),
            })
            .await
            .unwrap();
        }
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let (sink, views) = Sink::open(store, fast()).await.unwrap();
        let view = views.load();
        assert!(view.state.get_stream(0).is_some());
        assert_eq!(
            view.state.get_kv("k"),
            Some(bytes::Bytes::from_static(b"v"))
        );
        let proposed = sink
            .propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(proposed.result, Outcome::Id(1));
    }

    #[tokio::test]
    async fn multi_writer_convergence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store_a = Arc::new(SqliteStore::open(&path).await.unwrap());
        let store_b = Arc::new(SqliteStore::open(&path).await.unwrap());
        let (sink_a, views_a) = Sink::open(store_a, fast()).await.unwrap();
        let (sink_b, views_b) = Sink::open(store_b, fast()).await.unwrap();

        sink_a.propose(register(1, 10)).await.unwrap();
        sink_b.propose(register(2, 20)).await.unwrap();

        let sink_a = Arc::new(sink_a);
        let sink_b = Arc::new(sink_b);
        let mut handles = Vec::new();
        for i in 0..20u32 {
            let (sink, node_id, node_epoch) = if i % 2 == 0 {
                (sink_a.clone(), 1, 10)
            } else {
                (sink_b.clone(), 2, 20)
            };
            handles.push(tokio::spawn(async move {
                sink.propose(Command::CreateStream {
                    node_id,
                    node_epoch,
                })
                .await
                .unwrap()
                .result
            }));
        }
        let mut ids = Vec::new();
        for handle in handles {
            match handle.await.unwrap() {
                Outcome::Id(id) => ids.push(id),
                other => panic!("unexpected result {other:?}"),
            }
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..20).collect::<Vec<u64>>());

        let target = views_a
            .load()
            .applied_index
            .max(views_b.load().applied_index);
        let view_a = views_a.wait_applied(target).await;
        let view_b = views_b.wait_applied(target).await;
        assert_eq!(view_a.state, view_b.state);
        assert_eq!(view_a.state.streams.len(), 20);
    }

    #[tokio::test]
    async fn concurrent_proposes_coalesce() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let (sink, _) = Sink::open(store.clone(), fast()).await.unwrap();
        let sink = Arc::new(sink);
        sink.propose(register(1, 10)).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..100 {
            let sink = sink.clone();
            handles.push(tokio::spawn(async move {
                sink.propose(Command::CreateStream {
                    node_id: 1,
                    node_epoch: 10,
                })
                .await
                .unwrap()
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        let rows = store.fetch_after(0, 10_000).await.unwrap();
        assert!(
            rows.len() < 101,
            "expected batching to produce fewer rows than commands, got {}",
            rows.len()
        );
        let commands: usize = rows
            .iter()
            .map(|(_, payload)| codec::decode_batch(payload).unwrap().len())
            .sum();
        assert_eq!(commands, 101);
    }

    #[tokio::test]
    async fn snapshot_cycle_truncates_log_and_cold_start_restores() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let config = Config {
            snapshot_every: 5,
            ..fast()
        };
        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let final_state = {
            let (sink, views) = Sink::open(store.clone(), config.clone()).await.unwrap();
            sink.propose(register(1, 10)).await.unwrap();
            for _ in 0..30 {
                sink.propose(Command::CreateStream {
                    node_id: 1,
                    node_epoch: 10,
                })
                .await
                .unwrap();
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let snapshot = store.load_snapshot().await.unwrap();
                let rows = store.fetch_after(0, 10_000).await.unwrap();
                if let Some((snap_idx, _)) = snapshot
                    && snap_idx > 0
                    && rows.iter().all(|(idx, _)| *idx > snap_idx)
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "snapshot cycle never ran"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let rows = store.fetch_after(0, 10_000).await.unwrap();
            assert!(rows.len() < 31, "log not truncated: {} rows", rows.len());
            views.load().state.clone()
        };

        let store = Arc::new(SqliteStore::open(&path).await.unwrap());
        let (sink, views) = Sink::open(store, config).await.unwrap();
        assert_eq!(views.load().state, final_state);
        let next = sink
            .propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(next.result, Outcome::Id(30));
    }

    #[tokio::test]
    async fn lagging_reader_recovers_through_truncation_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");

        let store_b = Arc::new(SqliteStore::open(&path).await.unwrap());
        let config_b = Config {
            poll_interval: Duration::from_millis(500),
            snapshot_every: 0,
            ..Config::default()
        };
        let (_sink_b, views_b) = Sink::open(store_b, config_b).await.unwrap();

        let store_a = Arc::new(SqliteStore::open(&path).await.unwrap());
        let config_a = Config {
            snapshot_every: 5,
            ..fast()
        };
        let (sink_a, views_a) = Sink::open(store_a.clone(), config_a).await.unwrap();
        sink_a.propose(register(1, 10)).await.unwrap();
        for _ in 0..20 {
            sink_a
                .propose(Command::CreateStream {
                    node_id: 1,
                    node_epoch: 10,
                })
                .await
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while store_a.load_snapshot().await.unwrap().is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "snapshot never written"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        sink_a
            .propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();

        let target = views_a.load().applied_index;
        let view_b = tokio::time::timeout(Duration::from_secs(10), views_b.wait_applied(target))
            .await
            .expect("lagging reader never recovered");
        assert_eq!(view_b.state, views_a.load().state);
        assert_eq!(view_b.state.streams.len(), 21);
    }

    #[tokio::test]
    async fn failures_are_typed_and_isolated() {
        let (sink, views) = memory().await;
        sink.propose(register(1, 10)).await.unwrap();
        let err = sink
            .propose(Command::CreateStream {
                node_id: 9,
                node_epoch: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            5,
            "NodeEpochMismatch must survive the sink boundary"
        );
        let ok = sink
            .propose(Command::CreateStream {
                node_id: 1,
                node_epoch: 10,
            })
            .await
            .unwrap();
        assert_eq!(ok.result, Outcome::Id(0));
        assert_eq!(views.load().state.streams.len(), 1);
    }
}
