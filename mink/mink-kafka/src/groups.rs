//! The group coordinator hosted on the coordinator node: the registry of groups, the group request
//! handlers, and the timer that expires sessions and finishes overdue rebalances.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse, SyncGroupRequest,
    SyncGroupResponse,
};
use kafka_protocol::protocol::StrBytes;
use mink_common::sync::lock;
use tokio::sync::watch;

use crate::Kafka;
use crate::dispatch::Request;
use crate::error::Error;
use crate::group::{Group, Joining, State};

#[derive(Default)]
pub(crate) struct Groups {
    pub(crate) groups: Mutex<BTreeMap<String, Group>>,
}

impl Groups {
    fn with<T>(&self, id: &str, f: impl FnOnce(&mut Group) -> T) -> T {
        let mut groups = lock(&self.groups);
        let group = groups
            .entry(id.to_owned())
            .or_insert_with(|| Group::new(id.to_owned()));
        let out = f(group);
        if group.is_empty() && group.state == State::Empty && group.generation == 0 {
            groups.remove(id);
        }
        out
    }

    fn existing<T>(&self, id: &str, f: impl FnOnce(&mut Group) -> T) -> Result<T, Error> {
        let mut groups = lock(&self.groups);
        groups
            .get_mut(id)
            .map(f)
            .ok_or_else(|| Error::GroupNotFound(id.to_owned()))
    }

    fn tick(&self, now: Instant) {
        for group in lock(&self.groups).values_mut() {
            group.tick(now);
        }
    }

    fn clear(&self) {
        let mut groups = lock(&self.groups);
        for group in groups.values_mut() {
            group.fail_all();
        }
        groups.clear();
    }
}

pub(crate) async fn sweep(kafka: Kafka, tick: Duration, mut stop: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(tick);
    let mut leading = false;
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = stop.changed() => {
                kafka.inner.groups.clear();
                return;
            }
        }
        let now_leading = kafka.inner.coordinator.is_leader();
        if !leading && now_leading {
            let database = kafka.config().database.clone();
            if let Err(e) = kafka.ensure_database(&database).await {
                tracing::warn!(%e, database, "could not create the kafka database");
            }
        }
        if leading && !now_leading {
            tracing::info!("coordinator leadership lost; dropping consumer groups");
            kafka.inner.groups.clear();
        }
        leading = now_leading;
        if leading {
            kafka.inner.groups.tick(Instant::now());
        }
    }
}

fn str(s: &str) -> StrBytes {
    StrBytes::from(s.to_owned())
}

fn code(result: Result<(), Error>) -> i16 {
    result.err().map_or(0, |e| e.code())
}

impl Kafka {
    fn coordinating(&self) -> Result<(), Error> {
        if self.inner.coordinator.is_leader() {
            Ok(())
        } else {
            Err(Error::NotCoordinator)
        }
    }

    fn session_timeout(&self, ms: i32) -> Result<Duration, Error> {
        let timeout = Duration::from_millis(ms.max(0) as u64);
        let config = self.config();
        if timeout < config.min_session_timeout || timeout > config.max_session_timeout {
            return Err(Error::InvalidSessionTimeout(ms));
        }

        Ok(timeout)
    }

    pub(crate) async fn join_group(
        &self,
        request: &Request,
        join: JoinGroupRequest,
    ) -> Result<JoinGroupResponse, Error> {
        let group_id = join.group_id.as_str().to_owned();
        let member_id = join.member_id.to_string();
        let response = JoinGroupResponse::default().with_member_id(join.member_id.clone());
        let failed = |e: Error| {
            JoinGroupResponse::default()
                .with_error_code(e.code())
                .with_member_id(match &e {
                    Error::MemberIdRequired(id) => str(id),
                    _ => join.member_id.clone(),
                })
                .with_generation_id(-1)
        };

        if let Err(e) = self.coordinating() {
            return Ok(failed(e));
        }
        if group_id.is_empty() {
            return Ok(failed(Error::InvalidRequest("empty group id".into())));
        }
        let session_timeout = match self.session_timeout(join.session_timeout_ms) {
            Ok(t) => t,
            Err(e) => return Ok(failed(e)),
        };
        let rebalance_timeout = if request.version >= 1 && join.rebalance_timeout_ms > 0 {
            Duration::from_millis(join.rebalance_timeout_ms as u64)
        } else {
            session_timeout
        };
        let client_id = request.client_id.clone().unwrap_or_default();

        let joining = |member_id: String| Joining {
            member_id,
            instance_id: join.group_instance_id.as_ref().map(|s| s.to_string()),
            client_id: client_id.clone(),
            client_host: String::new(),
            protocol_type: join.protocol_type.to_string(),
            protocols: join
                .protocols
                .iter()
                .map(|p| (p.name.to_string(), p.metadata.clone()))
                .collect(),
            session_timeout,
            rebalance_timeout,
        };

        let now = Instant::now();
        let waiter = self.inner.groups.with(&group_id, |group| {
            if member_id.is_empty() {
                let id = group.reserve_member_id(&client_id);
                if request.version >= 4 {
                    return Err(Error::MemberIdRequired(id));
                }
                return group.join(joining(id), now);
            }
            group.join(joining(member_id.clone()), now)
        });
        let waiter = match waiter {
            Ok(waiter) => waiter,
            Err(e) => return Ok(failed(e)),
        };
        let Ok(joined) = waiter.await else {
            return Ok(failed(Error::NotCoordinator));
        };

        let members = joined
            .members
            .into_iter()
            .map(|(id, instance_id, metadata)| {
                JoinGroupResponseMember::default()
                    .with_member_id(str(&id))
                    .with_group_instance_id(instance_id.map(|s| str(&s)))
                    .with_metadata(metadata)
            })
            .collect();

        Ok(response
            .with_generation_id(joined.generation)
            .with_protocol_type(joined.protocol_type.map(|s| str(&s)))
            .with_protocol_name(joined.protocol.map(|s| str(&s)))
            .with_leader(str(&joined.leader))
            .with_member_id(str(&joined.member_id))
            .with_members(members))
    }

    pub(crate) async fn sync_group(
        &self,
        _request: &Request,
        sync: SyncGroupRequest,
    ) -> Result<SyncGroupResponse, Error> {
        let failed = |e: Error| SyncGroupResponse::default().with_error_code(e.code());
        if let Err(e) = self.coordinating() {
            return Ok(failed(e));
        }
        let assignments: Vec<(String, Bytes)> = sync
            .assignments
            .iter()
            .map(|a| (a.member_id.to_string(), a.assignment.clone()))
            .collect();
        let waiter = self.inner.groups.existing(sync.group_id.as_str(), |group| {
            group.sync(
                sync.member_id.as_str(),
                sync.generation_id,
                assignments,
                Instant::now(),
            )
        });
        let waiter = match waiter {
            Ok(Ok(waiter)) => waiter,
            Ok(Err(e)) | Err(e) => return Ok(failed(e)),
        };
        let assignment = match waiter.await {
            Ok(Ok(assignment)) => assignment,
            Ok(Err(e)) => return Ok(failed(e)),
            Err(_) => return Ok(failed(Error::NotCoordinator)),
        };
        let (protocol_type, protocol) = self
            .inner
            .groups
            .existing(sync.group_id.as_str(), |g| {
                (g.protocol_type.clone(), g.protocol.clone())
            })
            .unwrap_or_default();

        Ok(SyncGroupResponse::default()
            .with_protocol_type(protocol_type.map(|s| str(&s)))
            .with_protocol_name(protocol.map(|s| str(&s)))
            .with_assignment(assignment))
    }

    pub(crate) async fn heartbeat(
        &self,
        _request: &Request,
        beat: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, Error> {
        let result = self.coordinating().and_then(|()| {
            self.inner
                .groups
                .existing(beat.group_id.as_str(), |group| {
                    group.heartbeat(beat.member_id.as_str(), beat.generation_id, Instant::now())
                })
                .and_then(|r| r)
        });

        Ok(HeartbeatResponse::default().with_error_code(code(result)))
    }

    pub(crate) async fn leave_group(
        &self,
        request: &Request,
        leave: LeaveGroupRequest,
    ) -> Result<LeaveGroupResponse, Error> {
        if let Err(e) = self.coordinating() {
            return Ok(LeaveGroupResponse::default().with_error_code(e.code()));
        }
        let members: Vec<(String, Option<StrBytes>)> = if request.version >= 3 {
            leave
                .members
                .iter()
                .map(|m| (m.member_id.to_string(), m.group_instance_id.clone()))
                .collect()
        } else {
            vec![(leave.member_id.to_string(), None)]
        };
        let now = Instant::now();
        let results: Vec<Result<(), Error>> = members
            .iter()
            .map(|(id, _)| {
                self.inner
                    .groups
                    .existing(leave.group_id.as_str(), |group| group.leave(id, now))
                    .and_then(|r| r)
            })
            .collect();

        if request.version < 3 {
            let first = results.into_iter().next().unwrap_or(Ok(()));
            return Ok(LeaveGroupResponse::default().with_error_code(code(first)));
        }

        Ok(LeaveGroupResponse::default().with_members(
            members
                .into_iter()
                .zip(results)
                .map(|((id, instance), result)| {
                    MemberResponse::default()
                        .with_member_id(str(&id))
                        .with_group_instance_id(instance)
                        .with_error_code(code(result))
                })
                .collect(),
        ))
    }

    pub(crate) async fn describe_groups(
        &self,
        _request: &Request,
        describe: DescribeGroupsRequest,
    ) -> Result<DescribeGroupsResponse, Error> {
        let groups = lock(&self.inner.groups.groups);
        let described = describe
            .groups
            .into_iter()
            .map(|id| {
                let base = DescribedGroup::default().with_group_id(id.clone());
                if let Err(e) = self.coordinating() {
                    return base.with_error_code(e.code()).with_group_state(str("Dead"));
                }
                let Some(group) = groups.get(id.as_str()) else {
                    return base.with_group_state(str("Dead"));
                };
                let protocol = group.protocol.as_deref();
                base.with_group_state(str(group.state.name()))
                    .with_protocol_type(str(group.protocol_type.as_deref().unwrap_or("")))
                    .with_protocol_data(str(protocol.unwrap_or("")))
                    .with_members(
                        group
                            .members
                            .values()
                            .map(|m| {
                                DescribedGroupMember::default()
                                    .with_member_id(str(&m.id))
                                    .with_group_instance_id(m.instance_id.as_deref().map(str))
                                    .with_client_id(str(&m.client_id))
                                    .with_client_host(str(&m.client_host))
                                    .with_member_metadata(m.metadata(protocol))
                                    .with_member_assignment(m.assignment.clone())
                            })
                            .collect(),
                    )
            })
            .collect();

        Ok(DescribeGroupsResponse::default().with_groups(described))
    }

    pub(crate) async fn list_groups(
        &self,
        _request: &Request,
        list: ListGroupsRequest,
    ) -> Result<ListGroupsResponse, Error> {
        if let Err(e) = self.coordinating() {
            return Ok(ListGroupsResponse::default().with_error_code(e.code()));
        }
        let states: Vec<String> = list.states_filter.iter().map(|s| s.to_string()).collect();
        let groups = lock(&self.inner.groups.groups)
            .values()
            .filter(|g| states.is_empty() || states.iter().any(|s| s == g.state.name()))
            .filter(|_| {
                list.types_filter.is_empty()
                    || list.types_filter.iter().any(|t| t.as_str() == "classic")
            })
            .map(|g| {
                ListedGroup::default()
                    .with_group_id(GroupId(str(&g.id)))
                    .with_protocol_type(str(g.protocol_type.as_deref().unwrap_or("")))
                    .with_group_state(str(g.state.name()))
                    .with_group_type(str("classic"))
            })
            .collect();

        Ok(ListGroupsResponse::default().with_groups(groups))
    }

    pub(crate) async fn delete_groups(
        &self,
        _request: &Request,
        delete: DeleteGroupsRequest,
    ) -> Result<DeleteGroupsResponse, Error> {
        let mut results = Vec::with_capacity(delete.groups_names.len());
        for id in delete.groups_names {
            let result = match self.coordinating() {
                Err(e) => Err(e),
                Ok(()) => {
                    let removable = {
                        let mut groups = lock(&self.inner.groups.groups);
                        match groups.get(id.as_str()) {
                            None => Ok(false),
                            Some(g) if g.members.is_empty() => {
                                groups.remove(id.as_str());
                                Ok(true)
                            }
                            Some(_) => Err(Error::NonEmptyGroup(id.to_string())),
                        }
                    };
                    match removable {
                        Ok(_) => self
                            .inner
                            .coordinator
                            .delete_group_offsets(id.as_str())
                            .await
                            .map_err(Error::from),
                        Err(e) => Err(e),
                    }
                }
            };
            results.push(
                DeletableGroupResult::default()
                    .with_group_id(id)
                    .with_error_code(code(result)),
            );
        }

        Ok(DeleteGroupsResponse::default().with_results(results))
    }
}
