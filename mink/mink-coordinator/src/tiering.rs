//! The tiering schedule: per-table state machine with epochs, due times, heartbeats and timeouts.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use mink_table::{Id, Path};

use crate::Error;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    New,
    Initialized,
    Scheduled,
    Pending,
    Tiering,
    Tiered,
    Failed,
}

impl State {
    fn accepts(self, from: Option<State>) -> bool {
        match (self, from) {
            (State::New | State::Initialized, from) => from.is_none(),
            (_, None) => false,
            (State::Scheduled, Some(s)) => {
                matches!(s, State::New | State::Initialized | State::Tiered)
            }
            (State::Pending, Some(s)) => {
                matches!(s, State::Scheduled | State::Failed | State::Tiered)
            }
            (State::Tiering, Some(State::Pending)) => true,
            (State::Tiered | State::Failed, Some(State::Tiering)) => true,
            _ => false,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            State::New => "new",
            State::Initialized => "initialized",
            State::Scheduled => "scheduled",
            State::Pending => "pending",
            State::Tiering => "tiering",
            State::Tiered => "tiered",
            State::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub table_id: Id,
    pub path: Path,
    pub state: Option<State>,
    pub epoch: u64,
    pub last_tiered_ms: i64,
    pub due_ms: Option<i64>,
    pub heartbeat_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub table_id: Id,
    pub path: Path,
    pub epoch: u64,
}

#[derive(Debug)]
struct Entry {
    path: Path,
    freshness_ms: i64,
    last_tiered_ms: i64,
    epoch: u64,
    state: Option<State>,
    due_ms: Option<i64>,
    heartbeat_ms: Option<i64>,
}

impl Entry {
    fn reschedule(&mut self) {
        self.due_ms = Some(self.last_tiered_ms + self.freshness_ms);
    }
}

pub struct Manager {
    timeout_ms: i64,
    tables: BTreeMap<Id, Entry>,
    pending: VecDeque<Id>,
}

impl Manager {
    pub fn new(timeout: Duration) -> Self {
        Manager {
            timeout_ms: timeout.as_millis() as i64,
            tables: BTreeMap::new(),
            pending: VecDeque::new(),
        }
    }

    pub fn restore(
        &mut self,
        table_id: Id,
        path: Path,
        freshness: Duration,
        last_tiered_ms: i64,
        now_ms: i64,
    ) {
        self.register(table_id, path, freshness, last_tiered_ms);
        self.transition(table_id, State::Initialized, now_ms);
        self.transition(table_id, State::Scheduled, now_ms);
    }

    pub fn add(&mut self, table_id: Id, path: Path, freshness: Duration, now_ms: i64) {
        self.register(table_id, path, freshness, now_ms);
        self.transition(table_id, State::New, now_ms);
        self.transition(table_id, State::Scheduled, now_ms);
    }

    pub fn remove(&mut self, table_id: Id) {
        self.tables.remove(&table_id);
        self.pending.retain(|id| *id != table_id);
    }

    pub fn table_ids(&self) -> impl Iterator<Item = Id> + '_ {
        self.tables.keys().copied()
    }

    pub fn contains(&self, table_id: Id) -> bool {
        self.tables.contains_key(&table_id)
    }

    pub fn state(&self, table_id: Id) -> Option<State> {
        self.tables.get(&table_id).and_then(|entry| entry.state)
    }

    pub fn status(&self) -> Vec<Status> {
        self.tables
            .iter()
            .map(|(id, entry)| Status {
                table_id: *id,
                path: entry.path.clone(),
                state: entry.state,
                epoch: entry.epoch,
                last_tiered_ms: entry.last_tiered_ms,
                due_ms: entry.due_ms,
                heartbeat_ms: entry.heartbeat_ms,
            })
            .collect()
    }

    pub fn update_freshness(&mut self, table_id: Id, freshness: Duration) {
        let Some(entry) = self.tables.get_mut(&table_id) else {
            return;
        };
        let freshness_ms = freshness.as_millis() as i64;
        if entry.freshness_ms == freshness_ms {
            return;
        }
        entry.freshness_ms = freshness_ms;
        if entry.state == Some(State::Scheduled) {
            entry.reschedule();
        }
    }

    pub fn tick(&mut self, now_ms: i64) {
        let due: Vec<Id> = self
            .tables
            .iter()
            .filter(|(_, e)| {
                e.state == Some(State::Scheduled) && e.due_ms.is_some_and(|d| d <= now_ms)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            self.transition(id, State::Pending, now_ms);
        }

        let silent: Vec<Id> = self
            .tables
            .iter()
            .filter(|(_, e)| {
                e.state == Some(State::Tiering)
                    && e.heartbeat_ms
                        .is_some_and(|beat| now_ms - beat >= self.timeout_ms)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in silent {
            self.transition(id, State::Failed, now_ms);
            self.transition(id, State::Pending, now_ms);
        }
    }

    pub fn request_table(&mut self, now_ms: i64) -> Option<Table> {
        while let Some(id) = self.pending.pop_front() {
            let Some(entry) = self.tables.get(&id) else {
                continue;
            };
            let path = entry.path.clone();
            self.transition(id, State::Tiering, now_ms);
            let epoch = self.tables[&id].epoch;
            return Some(Table {
                table_id: id,
                path,
                epoch,
            });
        }
        None
    }

    pub fn finish(
        &mut self,
        table_id: Id,
        epoch: u64,
        forced: bool,
        now_ms: i64,
    ) -> Result<(), Error> {
        self.check(table_id, epoch)?;
        self.transition(table_id, State::Tiered, now_ms);
        let next = if forced {
            State::Pending
        } else {
            State::Scheduled
        };
        self.transition(table_id, next, now_ms);

        Ok(())
    }

    pub fn fail(&mut self, table_id: Id, epoch: u64, now_ms: i64) -> Result<(), Error> {
        self.check(table_id, epoch)?;
        self.transition(table_id, State::Failed, now_ms);
        self.transition(table_id, State::Pending, now_ms);

        Ok(())
    }

    pub fn heartbeat(&mut self, table_id: Id, epoch: u64, now_ms: i64) -> Result<(), Error> {
        self.check(table_id, epoch)?;
        let entry = self.tables.get_mut(&table_id).expect("checked");
        if entry.state != Some(State::Tiering) {
            return Err(Error::NotTiering(table_id));
        }
        entry.heartbeat_ms = Some(now_ms);

        Ok(())
    }

    fn register(&mut self, id: Id, path: Path, freshness: Duration, last_tiered_ms: i64) {
        self.tables.insert(
            id,
            Entry {
                path,
                freshness_ms: freshness.as_millis() as i64,
                last_tiered_ms,
                epoch: 0,
                state: None,
                due_ms: None,
                heartbeat_ms: None,
            },
        );
    }

    fn check(&self, id: Id, epoch: u64) -> Result<(), Error> {
        let entry = self.tables.get(&id).ok_or(Error::NotLakeTable(id))?;
        if entry.epoch != epoch {
            return Err(Error::TieringFenced {
                id,
                current: entry.epoch,
                given: epoch,
            });
        }

        Ok(())
    }

    fn transition(&mut self, id: Id, target: State, now_ms: i64) {
        let Some(entry) = self.tables.get_mut(&id) else {
            return;
        };
        if !target.accepts(entry.state) {
            tracing::warn!(?id, from = ?entry.state, to = ?target, "invalid tiering transition");
            return;
        }
        match target {
            State::New | State::Initialized => {}
            State::Scheduled => entry.reschedule(),
            State::Pending => {
                entry.epoch += 1;
                entry.due_ms = None;
                self.pending.push_back(id);
            }
            State::Tiering => entry.heartbeat_ms = Some(now_ms),
            State::Tiered => {
                entry.last_tiered_ms = now_ms;
                entry.heartbeat_ms = None;
            }
            State::Failed => entry.heartbeat_ms = None,
        }

        entry.state = Some(target);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> Path {
        "db.t".parse().unwrap()
    }

    const FRESH: Duration = Duration::from_secs(60);
    const T: Id = Id(1);

    #[test]
    fn a_new_table_is_due_after_one_freshness_interval() {
        let mut m = Manager::new(DEFAULT_TIMEOUT);
        m.add(T, path(), FRESH, 0);
        assert_eq!(m.state(T), Some(State::Scheduled));
        m.tick(59_999);
        assert!(m.request_table(59_999).is_none());
        m.tick(60_000);
        assert_eq!(m.state(T), Some(State::Pending));
        let handed = m.request_table(60_000).unwrap();
        assert_eq!(handed.epoch, 1);
        assert_eq!(m.state(T), Some(State::Tiering));
        assert!(m.request_table(60_000).is_none());
    }

    #[test]
    fn restored_tables_keep_their_last_tiered_time() {
        let mut m = Manager::new(DEFAULT_TIMEOUT);
        m.restore(T, path(), FRESH, 30_000, 100_000);
        m.tick(100_000);
        assert_eq!(m.state(T), Some(State::Pending));
    }

    #[test]
    fn finish_reschedules_and_forced_finish_requeues() {
        let mut m = Manager::new(DEFAULT_TIMEOUT);
        m.add(T, path(), FRESH, 0);
        m.tick(60_000);
        let handed = m.request_table(60_000).unwrap();
        m.finish(T, handed.epoch, false, 70_000).unwrap();
        assert_eq!(m.state(T), Some(State::Scheduled));
        m.tick(129_999);
        assert_eq!(m.state(T), Some(State::Scheduled));
        m.tick(130_000);
        let handed = m.request_table(130_000).unwrap();
        assert_eq!(handed.epoch, 2);
        m.finish(T, 2, true, 131_000).unwrap();
        assert_eq!(m.state(T), Some(State::Pending));
        assert_eq!(m.request_table(131_000).unwrap().epoch, 3);
    }

    #[test]
    fn stale_epochs_are_fenced() {
        let mut m = Manager::new(DEFAULT_TIMEOUT);
        m.add(T, path(), FRESH, 0);
        m.tick(60_000);
        let handed = m.request_table(60_000).unwrap();
        m.fail(T, handed.epoch, 61_000).unwrap();
        assert!(matches!(
            m.finish(T, handed.epoch, false, 62_000),
            Err(Error::TieringFenced {
                current: 2,
                given: 1,
                ..
            })
        ));
        assert!(matches!(
            m.heartbeat(Id(9), 1, 62_000),
            Err(Error::NotLakeTable(Id(9)))
        ));
    }

    #[test]
    fn silent_workers_time_out_and_the_table_is_requeued() {
        let mut m = Manager::new(Duration::from_secs(10));
        m.add(T, path(), FRESH, 0);
        m.tick(60_000);
        let handed = m.request_table(60_000).unwrap();
        m.heartbeat(T, handed.epoch, 65_000).unwrap();
        m.tick(74_999);
        assert_eq!(m.state(T), Some(State::Tiering));
        m.tick(75_000);
        assert_eq!(m.state(T), Some(State::Pending));
        assert!(matches!(
            m.heartbeat(T, handed.epoch, 75_000),
            Err(Error::TieringFenced { .. })
        ));
        assert_eq!(m.request_table(75_000).unwrap().epoch, 2);
    }

    #[test]
    fn heartbeat_outside_tiering_is_rejected_and_removed_tables_vanish() {
        let mut m = Manager::new(DEFAULT_TIMEOUT);
        m.add(T, path(), FRESH, 0);
        assert!(matches!(m.heartbeat(T, 0, 1), Err(Error::NotTiering(T))));
        m.tick(60_000);
        m.remove(T);
        assert!(m.request_table(60_000).is_none());
        assert!(!m.contains(T));
    }

    #[test]
    fn freshness_updates_move_the_deadline() {
        let mut m = Manager::new(DEFAULT_TIMEOUT);
        m.add(T, path(), FRESH, 0);
        m.update_freshness(T, Duration::from_secs(10));
        m.tick(10_000);
        assert_eq!(m.state(T), Some(State::Pending));
    }
}
