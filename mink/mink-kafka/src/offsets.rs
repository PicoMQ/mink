//! ListOffsets: earliest, latest and by-timestamp lookups against the bucket's log.

use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{ListOffsetsRequest, ListOffsetsResponse};
use mink_server::OffsetSpec;

use crate::Kafka;
use crate::dispatch::Request;
use crate::error::Error;

const LATEST: i64 = -1;
const EARLIEST: i64 = -2;
const MAX_TIMESTAMP: i64 = -3;
const EARLIEST_LOCAL: i64 = -4;
const LATEST_TIERED: i64 = -5;

fn spec(timestamp: i64) -> Result<OffsetSpec, Error> {
    match timestamp {
        LATEST => Ok(OffsetSpec::Latest),
        EARLIEST | EARLIEST_LOCAL => Ok(OffsetSpec::Earliest),
        LATEST_TIERED | MAX_TIMESTAMP => Err(Error::InvalidRequest(format!(
            "list offsets by {timestamp} is not supported"
        ))),
        ts if ts >= 0 => Ok(OffsetSpec::Timestamp(ts)),
        other => Err(Error::InvalidRequest(format!("bad timestamp {other}"))),
    }
}

impl Kafka {
    pub(crate) async fn list_offsets(
        &self,
        request: &Request,
        list: ListOffsetsRequest,
    ) -> Result<ListOffsetsResponse, Error> {
        let view = self.inner.service.view();
        let mut topics = Vec::with_capacity(list.topics.len());
        for topic in list.topics {
            let resolved = self.topic(&view, topic.name.as_str());
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let response = ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition.partition_index)
                    .with_timestamp(-1)
                    .with_offset(-1)
                    .with_leader_epoch(-1);
                let outcome = match &resolved {
                    Ok(t) => match (
                        t.bucket(partition.partition_index),
                        spec(partition.timestamp),
                    ) {
                        (Ok(bucket), Ok(spec)) => self
                            .inner
                            .service
                            .list_offset(bucket, spec)
                            .await
                            .map_err(Error::from)
                            .map(|offset| (offset, self.leader(&view, bucket))),
                        (Err(e), _) | (_, Err(e)) => Err(e),
                    },
                    Err(Error::InvalidTopic(name)) => Err(Error::InvalidTopic(name.clone())),
                    Err(_) => Err(Error::UnknownTopic(topic.name.to_string())),
                };
                partitions.push(match outcome {
                    Ok((offset, leader)) => {
                        let mut response = response
                            .with_offset(offset)
                            .with_timestamp(partition.timestamp.max(-1));
                        if request.version >= 4 {
                            response =
                                response.with_leader_epoch(leader.map_or(-1, |(_, epoch)| epoch));
                        }
                        response
                    }
                    Err(e) => response.with_error_code(e.code()),
                });
            }
            topics.push(
                ListOffsetsTopicResponse::default()
                    .with_name(topic.name)
                    .with_partitions(partitions),
            );
        }

        Ok(ListOffsetsResponse::default().with_topics(topics))
    }
}
