//! Committed consumer offsets: OffsetCommit writes them to metadata through the coordinator,
//! OffsetFetch reads them from the view.

use std::collections::BTreeMap;
use std::time::Duration;

use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::{
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use mink_metadata::View;
use mink_table::Bucket;

use crate::Kafka;
use crate::dispatch::Request;
use crate::error::Error;

struct Fetched {
    partition: i32,
    offset: i64,
    error: i16,
}

impl Kafka {
    pub(crate) async fn offset_commit(
        &self,
        request: &Request,
        commit: OffsetCommitRequest,
    ) -> Result<OffsetCommitResponse, Error> {
        let view = self.inner.service.view();
        let group = commit.group_id.as_str();
        let fenced = self.coordinating_member(
            group,
            commit.member_id.as_str(),
            commit.generation_id_or_member_epoch,
        );

        let mut offsets = BTreeMap::new();
        let mut rejected: BTreeMap<(usize, usize), i16> = BTreeMap::new();
        for (t, topic) in commit.topics.iter().enumerate() {
            let resolved = self.topic(&view, topic.name.as_str());
            for (p, partition) in topic.partitions.iter().enumerate() {
                let bucket = match &resolved {
                    Ok(topic) => topic.bucket(partition.partition_index),
                    Err(Error::InvalidTopic(name)) => Err(Error::InvalidTopic(name.clone())),
                    Err(_) => Err(Error::UnknownTopic(topic.name.to_string())),
                };
                match bucket {
                    Ok(bucket) if partition.committed_offset >= 0 => {
                        offsets.insert(bucket, partition.committed_offset);
                    }
                    Ok(_) => {
                        rejected.insert((t, p), Error::InvalidRequest(String::new()).code());
                    }
                    Err(e) => {
                        rejected.insert((t, p), e.code());
                    }
                }
            }
        }

        let ttl = match (request.version, commit.retention_time_ms) {
            (2..=4, ms) if ms > 0 => Duration::from_millis(ms as u64),
            _ => self.config().group_offsets_ttl,
        };
        let outcome = match fenced {
            Ok(()) if offsets.is_empty() => Ok(()),
            Ok(()) => self
                .inner
                .coordinator
                .commit_group_offsets(group, offsets, ttl)
                .await
                .map_err(Error::from),
            Err(e) => Err(e),
        };
        let code = outcome.err().map_or(0, |e| e.code());

        let topics = commit
            .topics
            .iter()
            .enumerate()
            .map(|(t, topic)| {
                OffsetCommitResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_topic_id(topic.topic_id)
                    .with_partitions(
                        topic
                            .partitions
                            .iter()
                            .enumerate()
                            .map(|(p, partition)| {
                                OffsetCommitResponsePartition::default()
                                    .with_partition_index(partition.partition_index)
                                    .with_error_code(rejected.get(&(t, p)).copied().unwrap_or(code))
                            })
                            .collect(),
                    )
            })
            .collect();

        Ok(OffsetCommitResponse::default().with_topics(topics))
    }

    fn coordinating_member(&self, group: &str, member: &str, generation: i32) -> Result<(), Error> {
        if !self.inner.coordinator.is_leader() {
            return Err(Error::NotCoordinator);
        }
        if group.is_empty() {
            return Err(Error::InvalidRequest("empty group id".into()));
        }
        let groups = mink_common::sync::lock(&self.inner.groups.groups);
        match groups.get(group) {
            Some(g) => g.check_commit(member, generation),
            None if generation < 0 => Ok(()),
            None => Err(Error::UnknownMember(member.to_owned())),
        }
    }

    pub(crate) async fn offset_fetch(
        &self,
        request: &Request,
        fetch: OffsetFetchRequest,
    ) -> Result<OffsetFetchResponse, Error> {
        let view = self.inner.service.view();
        if request.version >= 8 {
            let groups = fetch
                .groups
                .iter()
                .map(|g| {
                    let wanted = g.topics.as_ref().map(|topics| {
                        topics
                            .iter()
                            .map(|t| (t.name.clone(), t.partition_indexes.clone()))
                            .collect::<Vec<_>>()
                    });
                    let (topics, error) =
                        self.fetched(&view, g.group_id.as_str(), wanted.as_deref());
                    OffsetFetchResponseGroup::default()
                        .with_group_id(g.group_id.clone())
                        .with_error_code(error)
                        .with_topics(
                            topics
                                .into_iter()
                                .map(|(name, parts)| {
                                    OffsetFetchResponseTopics::default()
                                        .with_name(name)
                                        .with_partitions(
                                            parts
                                                .into_iter()
                                                .map(|f| {
                                                    OffsetFetchResponsePartitions::default()
                                                        .with_partition_index(f.partition)
                                                        .with_committed_offset(f.offset)
                                                        .with_committed_leader_epoch(-1)
                                                        .with_error_code(f.error)
                                                })
                                                .collect(),
                                        )
                                })
                                .collect(),
                        )
                })
                .collect();

            return Ok(OffsetFetchResponse::default().with_groups(groups));
        }

        let wanted = fetch.topics.as_ref().map(|topics| {
            topics
                .iter()
                .map(|t| (t.name.clone(), t.partition_indexes.clone()))
                .collect::<Vec<_>>()
        });
        let (topics, error) = self.fetched(&view, fetch.group_id.as_str(), wanted.as_deref());

        Ok(OffsetFetchResponse::default()
            .with_error_code(error)
            .with_topics(
                topics
                    .into_iter()
                    .map(|(name, parts)| {
                        OffsetFetchResponseTopic::default()
                            .with_name(name)
                            .with_partitions(
                                parts
                                    .into_iter()
                                    .map(|f| {
                                        OffsetFetchResponsePartition::default()
                                            .with_partition_index(f.partition)
                                            .with_committed_offset(f.offset)
                                            .with_committed_leader_epoch(-1)
                                            .with_error_code(f.error)
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            ))
    }

    fn fetched(
        &self,
        view: &View,
        group: &str,
        wanted: Option<&[(TopicName, Vec<i32>)]>,
    ) -> (Vec<(TopicName, Vec<Fetched>)>, i16) {
        if group.is_empty() {
            return (Vec::new(), Error::InvalidRequest(String::new()).code());
        }
        let committed = self
            .inner
            .coordinator
            .group_offsets(group)
            .map(|row| row.offsets)
            .unwrap_or_default();
        let lookup = |bucket: Bucket| committed.get(&bucket).copied().unwrap_or(-1);

        let topics = match wanted {
            Some(wanted) => wanted
                .iter()
                .map(|(name, partitions)| {
                    let topic = self.topic(view, name.as_str());
                    let parts = partitions
                        .iter()
                        .map(|&partition| match &topic {
                            Ok(t) => match t.bucket(partition) {
                                Ok(bucket) => Fetched {
                                    partition,
                                    offset: lookup(bucket),
                                    error: 0,
                                },
                                Err(e) => Fetched {
                                    partition,
                                    offset: -1,
                                    error: e.code(),
                                },
                            },
                            Err(_) => Fetched {
                                partition,
                                offset: -1,
                                error: 0,
                            },
                        })
                        .collect();
                    (name.clone(), parts)
                })
                .collect(),
            None => {
                let mut by_topic: BTreeMap<String, Vec<Fetched>> = BTreeMap::new();
                for (bucket, offset) in &committed {
                    let Some(path) = view.state.catalog.table_paths.get(&bucket.table()) else {
                        continue;
                    };
                    by_topic
                        .entry(path.table().as_str().to_owned())
                        .or_default()
                        .push(Fetched {
                            partition: bucket.bucket().0 as i32,
                            offset: *offset,
                            error: 0,
                        });
                }
                by_topic
                    .into_iter()
                    .map(|(name, parts)| (TopicName(StrBytes::from(name)), parts))
                    .collect()
            }
        };

        (topics, 0)
    }
}
