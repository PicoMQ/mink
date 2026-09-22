//! One consumer group's classic-protocol state machine: membership, generations, rebalance rounds,
//! protocol selection, leader assignment distribution and session expiry.

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::error::Error;

pub(crate) const INITIAL_REBALANCE_DELAY: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl State {
    pub fn name(self) -> &'static str {
        match self {
            State::Empty => "Empty",
            State::PreparingRebalance => "PreparingRebalance",
            State::CompletingRebalance => "CompletingRebalance",
            State::Stable => "Stable",
        }
    }
}

pub(crate) struct Joining {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub protocol_type: String,
    pub protocols: Vec<(String, Bytes)>,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
}

pub(crate) struct Member {
    pub id: String,
    pub instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub protocols: Vec<(String, Bytes)>,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub assignment: Bytes,
    last_heartbeat: Instant,
    rejoined: bool,
    join: Option<oneshot::Sender<Joined>>,
    sync: Option<oneshot::Sender<Result<Bytes, Error>>>,
}

impl Member {
    pub fn metadata(&self, protocol: Option<&str>) -> Bytes {
        protocol
            .and_then(|p| self.protocols.iter().find(|(name, _)| name == p))
            .map(|(_, metadata)| metadata.clone())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Joined {
    pub generation: i32,
    pub protocol_type: Option<String>,
    pub protocol: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<(String, Option<String>, Bytes)>,
}

pub(crate) struct Group {
    pub id: String,
    pub state: State,
    pub generation: i32,
    pub protocol_type: Option<String>,
    pub protocol: Option<String>,
    pub leader: Option<String>,
    pub members: BTreeMap<String, Member>,
    pending: HashSet<String>,
    deadline: Option<Instant>,
    initial: bool,
}

impl Group {
    pub fn new(id: String) -> Self {
        Group {
            id,
            state: State::Empty,
            generation: 0,
            protocol_type: None,
            protocol: None,
            leader: None,
            members: BTreeMap::new(),
            pending: HashSet::new(),
            deadline: None,
            initial: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty() && self.pending.is_empty()
    }

    pub fn reserve_member_id(&mut self, client_id: &str) -> String {
        let id = format!("{client_id}-{}", uuid::Uuid::new_v4());
        self.pending.insert(id.clone());
        id
    }

    pub fn join(
        &mut self,
        joining: Joining,
        now: Instant,
    ) -> Result<oneshot::Receiver<Joined>, Error> {
        if let Some(existing) = &self.protocol_type
            && !self.members.is_empty()
            && *existing != joining.protocol_type
        {
            return Err(Error::InconsistentProtocol);
        }
        let known = self.members.contains_key(&joining.member_id);
        if !known && !self.pending.remove(&joining.member_id) {
            return Err(Error::UnknownMember(joining.member_id));
        }
        if !known && !self.supports_all(&joining.protocols) {
            return Err(Error::InconsistentProtocol);
        }

        let (tx, rx) = oneshot::channel();
        let member = self
            .members
            .entry(joining.member_id.clone())
            .or_insert_with(|| Member {
                id: joining.member_id.clone(),
                instance_id: None,
                client_id: String::new(),
                client_host: String::new(),
                protocols: Vec::new(),
                session_timeout: joining.session_timeout,
                rebalance_timeout: joining.rebalance_timeout,
                assignment: Bytes::new(),
                last_heartbeat: now,
                rejoined: false,
                join: None,
                sync: None,
            });
        member.instance_id = joining.instance_id;
        member.client_id = joining.client_id;
        member.client_host = joining.client_host;
        member.protocols = joining.protocols;
        member.session_timeout = joining.session_timeout;
        member.rebalance_timeout = joining.rebalance_timeout;
        member.last_heartbeat = now;
        member.rejoined = true;
        member.join = Some(tx);
        self.protocol_type = Some(joining.protocol_type);

        match self.state {
            State::PreparingRebalance => {}
            State::Empty => self.prepare(now, true),
            State::CompletingRebalance | State::Stable => self.prepare(now, false),
        }
        self.try_complete(now);

        Ok(rx)
    }

    fn supports_all(&self, protocols: &[(String, Bytes)]) -> bool {
        self.members.is_empty()
            || self.members.values().any(|m| {
                m.protocols
                    .iter()
                    .any(|(name, _)| protocols.iter().any(|(n, _)| n == name))
            })
    }

    fn prepare(&mut self, now: Instant, initial: bool) {
        let timeout = self
            .members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or(INITIAL_REBALANCE_DELAY);
        self.state = State::PreparingRebalance;
        self.initial = initial;
        self.deadline = Some(
            now + if initial {
                INITIAL_REBALANCE_DELAY
            } else {
                timeout
            },
        );
        for member in self.members.values_mut() {
            if member.join.is_none() {
                member.rejoined = false;
            }
            if let Some(sync) = member.sync.take() {
                drop(sync.send(Err(Error::RebalanceInProgress)));
            }
        }
    }

    fn try_complete(&mut self, now: Instant) {
        if self.state != State::PreparingRebalance {
            return;
        }
        let due = self.deadline.is_some_and(|d| now >= d);
        let all = self.members.values().all(|m| m.rejoined);
        if !(due || (all && !self.initial)) {
            return;
        }
        if due {
            self.members.retain(|_, m| m.rejoined);
        }
        if self.members.is_empty() {
            self.state = State::Empty;
            self.generation += 1;
            self.protocol = None;
            self.leader = None;
            self.deadline = None;
            return;
        }

        self.generation += 1;
        self.protocol = self.select_protocol();
        if !self
            .leader
            .as_ref()
            .is_some_and(|l| self.members.contains_key(l))
        {
            self.leader = self.members.keys().next().cloned();
        }
        self.state = State::CompletingRebalance;
        self.deadline = None;

        let leader = self.leader.clone().unwrap_or_default();
        let roster: Vec<(String, Option<String>, Bytes)> = self
            .members
            .values()
            .map(|m| {
                (
                    m.id.clone(),
                    m.instance_id.clone(),
                    m.metadata(self.protocol.as_deref()),
                )
            })
            .collect();
        for member in self.members.values_mut() {
            member.rejoined = false;
            member.last_heartbeat = now;
            if let Some(tx) = member.join.take() {
                let _ = tx.send(Joined {
                    generation: self.generation,
                    protocol_type: self.protocol_type.clone(),
                    protocol: self.protocol.clone(),
                    leader: leader.clone(),
                    member_id: member.id.clone(),
                    members: if member.id == leader {
                        roster.clone()
                    } else {
                        Vec::new()
                    },
                });
            }
        }
    }

    fn select_protocol(&self) -> Option<String> {
        let leader = self
            .leader
            .as_ref()
            .and_then(|l| self.members.get(l))
            .or_else(|| self.members.values().next())?;
        leader
            .protocols
            .iter()
            .map(|(name, _)| name)
            .find(|name| {
                self.members
                    .values()
                    .all(|m| m.protocols.iter().any(|(n, _)| n == *name))
            })
            .cloned()
    }

    pub fn sync(
        &mut self,
        member_id: &str,
        generation: i32,
        assignments: Vec<(String, Bytes)>,
        now: Instant,
    ) -> Result<oneshot::Receiver<Result<Bytes, Error>>, Error> {
        self.check(member_id, generation)?;
        let (tx, rx) = oneshot::channel();
        match self.state {
            State::Empty | State::PreparingRebalance => return Err(Error::RebalanceInProgress),
            State::Stable => {
                let member = self.members.get_mut(member_id).expect("checked");
                member.last_heartbeat = now;
                drop(tx.send(Ok(member.assignment.clone())));
                return Ok(rx);
            }
            State::CompletingRebalance => {}
        }

        let member = self.members.get_mut(member_id).expect("checked");
        member.last_heartbeat = now;
        member.sync = Some(tx);
        if self.leader.as_deref() == Some(member_id) {
            for (id, assignment) in assignments {
                if let Some(m) = self.members.get_mut(&id) {
                    m.assignment = assignment;
                }
            }
            self.state = State::Stable;
            for m in self.members.values_mut() {
                if let Some(tx) = m.sync.take() {
                    drop(tx.send(Ok(m.assignment.clone())));
                }
            }
        }

        Ok(rx)
    }

    pub fn heartbeat(
        &mut self,
        member_id: &str,
        generation: i32,
        now: Instant,
    ) -> Result<(), Error> {
        self.check(member_id, generation)?;
        let member = self.members.get_mut(member_id).expect("checked");
        member.last_heartbeat = now;
        match self.state {
            State::PreparingRebalance => Err(Error::RebalanceInProgress),
            State::Empty => Err(Error::UnknownMember(member_id.to_owned())),
            State::CompletingRebalance | State::Stable => Ok(()),
        }
    }

    pub fn check(&self, member_id: &str, generation: i32) -> Result<(), Error> {
        if !self.members.contains_key(member_id) {
            return Err(Error::UnknownMember(member_id.to_owned()));
        }
        if generation != self.generation {
            return Err(Error::IllegalGeneration {
                given: generation,
                current: self.generation,
            });
        }

        Ok(())
    }

    pub fn check_commit(&self, member_id: &str, generation: i32) -> Result<(), Error> {
        if generation < 0 && member_id.is_empty() {
            return match self.state {
                State::Empty => Ok(()),
                _ => Err(Error::UnknownMember(member_id.to_owned())),
            };
        }
        self.check(member_id, generation)?;
        match self.state {
            State::CompletingRebalance => Err(Error::RebalanceInProgress),
            _ => Ok(()),
        }
    }

    pub fn leave(&mut self, member_id: &str, now: Instant) -> Result<(), Error> {
        if self.pending.remove(member_id) {
            return Ok(());
        }
        let Some(member) = self.members.remove(member_id) else {
            return Err(Error::UnknownMember(member_id.to_owned()));
        };
        drop(member);
        self.after_departure(now);

        Ok(())
    }

    fn after_departure(&mut self, now: Instant) {
        if self.members.is_empty() {
            self.state = State::Empty;
            self.generation += 1;
            self.protocol = None;
            self.leader = None;
            self.deadline = None;
            return;
        }
        match self.state {
            State::PreparingRebalance => self.try_complete(now),
            State::CompletingRebalance | State::Stable => {
                self.prepare(now, false);
                self.try_complete(now);
            }
            State::Empty => {}
        }
    }

    pub fn tick(&mut self, now: Instant) {
        let expired: Vec<String> = self
            .members
            .values()
            .filter(|m| {
                let waiting = self.state == State::PreparingRebalance && m.rejoined;
                !waiting && now.duration_since(m.last_heartbeat) > m.session_timeout
            })
            .map(|m| m.id.clone())
            .collect();
        for id in expired {
            tracing::info!(group = %self.id, member = %id, "session expired");
            self.members.remove(&id);
            self.after_departure(now);
        }
        self.try_complete(now);
    }

    pub fn fail_all(&mut self) {
        for member in self.members.values_mut() {
            member.join.take();
            if let Some(tx) = member.sync.take() {
                drop(tx.send(Err(Error::NotCoordinator)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn joining(id: &str, protocols: &[&str]) -> Joining {
        Joining {
            member_id: id.to_owned(),
            instance_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            protocol_type: "consumer".into(),
            protocols: protocols
                .iter()
                .map(|p| (p.to_string(), Bytes::from(format!("meta-{p}"))))
                .collect(),
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(5),
        }
    }

    fn joined(mut rx: oneshot::Receiver<Joined>) -> Joined {
        rx.try_recv().expect("join completed")
    }

    #[test]
    fn first_member_becomes_leader_after_the_initial_delay() {
        let mut g = Group::new("g".into());
        let now = Instant::now();
        let id = g.reserve_member_id("c");
        let mut rx = g.join(joining(&id, &["range"]), now).unwrap();
        assert_eq!(g.state, State::PreparingRebalance);
        assert!(rx.try_recv().is_err());

        g.tick(now + INITIAL_REBALANCE_DELAY);
        let j = rx.try_recv().unwrap();
        assert_eq!(g.state, State::CompletingRebalance);
        assert_eq!((j.generation, j.leader.as_str()), (1, id.as_str()));
        assert_eq!(j.protocol.as_deref(), Some("range"));
        assert_eq!(j.members.len(), 1);

        let mut rx = g
            .sync(&id, 1, vec![(id.clone(), Bytes::from("a1"))], now)
            .unwrap();
        assert_eq!(rx.try_recv().unwrap().unwrap(), Bytes::from("a1"));
        assert_eq!(g.state, State::Stable);
        assert!(g.heartbeat(&id, 1, now).is_ok());
        assert!(matches!(
            g.heartbeat(&id, 0, now),
            Err(Error::IllegalGeneration { .. })
        ));
    }

    #[test]
    fn a_second_member_forces_a_rebalance_and_followers_wait_for_the_leader() {
        let mut g = Group::new("g".into());
        let now = Instant::now();
        let a = g.reserve_member_id("a");
        drop(g.join(joining(&a, &["range", "sticky"]), now).unwrap());
        g.tick(now + INITIAL_REBALANCE_DELAY);
        drop(
            g.sync(&a, 1, vec![(a.clone(), Bytes::from("a"))], now)
                .unwrap(),
        );
        assert_eq!(g.state, State::Stable);

        let b = g.reserve_member_id("b");
        let b_join = g.join(joining(&b, &["sticky"]), now).unwrap();
        assert_eq!(g.state, State::PreparingRebalance);
        assert!(matches!(
            g.heartbeat(&a, 1, now),
            Err(Error::RebalanceInProgress)
        ));

        let a_join = g.join(joining(&a, &["range", "sticky"]), now).unwrap();
        assert_eq!(g.state, State::CompletingRebalance);
        let ja = joined(a_join);
        let jb = joined(b_join);
        assert_eq!(ja.generation, 2);
        assert_eq!(ja.protocol.as_deref(), Some("sticky"));
        assert_eq!(ja.leader, a);
        assert_eq!(ja.members.len(), 2);
        assert!(jb.members.is_empty());

        let mut b_sync = g.sync(&b, 2, vec![], now).unwrap();
        assert!(b_sync.try_recv().is_err());
        let mut a_sync = g
            .sync(
                &a,
                2,
                vec![
                    (a.clone(), Bytes::from("pa")),
                    (b.clone(), Bytes::from("pb")),
                ],
                now,
            )
            .unwrap();
        assert_eq!(a_sync.try_recv().unwrap().unwrap(), Bytes::from("pa"));
        assert_eq!(b_sync.try_recv().unwrap().unwrap(), Bytes::from("pb"));
        assert_eq!(g.state, State::Stable);
    }

    #[test]
    fn expired_sessions_and_leaves_shrink_the_group() {
        let mut g = Group::new("g".into());
        let now = Instant::now();
        let a = g.reserve_member_id("a");
        let b = g.reserve_member_id("b");
        drop(g.join(joining(&a, &["p"]), now).unwrap());
        drop(g.join(joining(&b, &["p"]), now).unwrap());
        g.tick(now + INITIAL_REBALANCE_DELAY);
        assert_eq!(g.generation, 1);
        drop(
            g.sync(
                &a,
                1,
                vec![(a.clone(), Bytes::new()), (b.clone(), Bytes::new())],
                now,
            )
            .unwrap(),
        );
        assert_eq!(g.state, State::Stable);

        g.tick(now + Duration::from_secs(11));
        assert_eq!(g.state, State::Empty);
        assert_eq!(g.generation, 2);

        let c = g.reserve_member_id("c");
        drop(g.join(joining(&c, &["p"]), now).unwrap());
        g.tick(now + INITIAL_REBALANCE_DELAY);
        assert!(g.leave(&c, now).is_ok());
        assert!(matches!(
            g.leave("nobody", now),
            Err(Error::UnknownMember(_))
        ));
        assert!(g.is_empty());
    }

    #[test]
    fn unknown_and_mismatched_protocols_are_rejected() {
        let mut g = Group::new("g".into());
        let now = Instant::now();
        assert!(matches!(
            g.join(joining("stranger", &["p"]), now),
            Err(Error::UnknownMember(_))
        ));
        let a = g.reserve_member_id("a");
        drop(g.join(joining(&a, &["p"]), now).unwrap());
        let b = g.reserve_member_id("b");
        assert!(matches!(
            g.join(joining(&b, &["other"]), now),
            Err(Error::InconsistentProtocol)
        ));
    }

    #[test]
    fn simple_consumers_commit_only_to_empty_groups() {
        let mut g = Group::new("g".into());
        assert!(g.check_commit("", -1).is_ok());
        let now = Instant::now();
        let a = g.reserve_member_id("a");
        drop(g.join(joining(&a, &["p"]), now).unwrap());
        g.tick(now + INITIAL_REBALANCE_DELAY);
        assert!(matches!(
            g.check_commit("", -1),
            Err(Error::UnknownMember(_))
        ));
        assert!(matches!(
            g.check_commit(&a, 1),
            Err(Error::RebalanceInProgress)
        ));
    }
}
