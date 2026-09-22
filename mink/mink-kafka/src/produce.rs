//! The produce path: each Kafka batch becomes one log batch appended to the partition's bucket, with
//! idempotent producers checked against the sequence window first.

use bytes::Bytes;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{NO_PRODUCER_ID, RecordSet};
use mink_common::{Clock, SystemClock};
use mink_server::OffsetSpec;
use mink_table::Bucket;

use crate::Kafka;
use crate::dispatch::Request;
use crate::error::Error;
use crate::producer::Check;
use crate::record;
use crate::topic::Topic;

struct Appended {
    base_offset: i64,
    log_append_time: i64,
}

impl Kafka {
    pub(crate) async fn produce(
        &self,
        _request: &Request,
        produce: ProduceRequest,
    ) -> Result<ProduceResponse, Error> {
        if produce
            .transactional_id
            .as_ref()
            .is_some_and(|id| !id.as_str().is_empty())
        {
            return Err(Error::InvalidRequest(
                "transactional produce is not supported".into(),
            ));
        }
        let view = self.inner.service.view();
        let mut responses = Vec::with_capacity(produce.topic_data.len());
        for topic_data in produce.topic_data {
            let topic = self.topic(&view, topic_data.name.as_str());
            let mut partitions = Vec::with_capacity(topic_data.partition_data.len());
            for partition in topic_data.partition_data {
                let outcome = self
                    .append(&topic, partition.index, partition.records)
                    .await;
                let response = PartitionProduceResponse::default()
                    .with_index(partition.index)
                    .with_log_start_offset(-1);
                partitions.push(match outcome {
                    Ok(appended) => response
                        .with_base_offset(appended.base_offset)
                        .with_log_append_time_ms(appended.log_append_time),
                    Err(e) => {
                        tracing::debug!(%e, topic = topic_data.name.as_str(), partition = partition.index, "produce failed");
                        response
                            .with_error_code(e.code())
                            .with_error_message(Some(StrBytes::from(e.to_string())))
                            .with_base_offset(-1)
                            .with_log_append_time_ms(-1)
                    }
                });
            }
            responses.push(
                TopicProduceResponse::default()
                    .with_name(topic_data.name)
                    .with_partition_responses(partitions),
            );
        }

        Ok(ProduceResponse::default().with_responses(responses))
    }

    async fn append(
        &self,
        topic: &Result<Topic, Error>,
        partition: i32,
        records: Option<Bytes>,
    ) -> Result<Appended, Error> {
        let bucket = match topic {
            Ok(topic) => topic.bucket(partition)?,
            Err(Error::InvalidTopic(name)) => return Err(Error::InvalidTopic(name.clone())),
            Err(e) => return Err(Error::UnknownTopic(e.to_string())),
        };
        let mut bytes = records.unwrap_or_default();
        let sets = record::decode_kafka(&mut bytes)?;
        if sets.iter().all(|set| set.records.is_empty()) {
            return Err(Error::InvalidRequest("produce carries no records".into()));
        }
        self.inner.service.leader_check(bucket)?;
        let now = SystemClock.millis();

        let mut first: Option<i64> = None;
        for set in &sets {
            let base_offset = self.append_set(bucket, set, now).await?;
            first.get_or_insert(base_offset);
        }

        Ok(Appended {
            base_offset: first.unwrap_or(-1),
            log_append_time: now,
        })
    }

    async fn append_set(&self, bucket: Bucket, set: &RecordSet, now: i64) -> Result<i64, Error> {
        let Some(head) = set.records.first() else {
            return Ok(self
                .inner
                .service
                .list_offset(bucket, OffsetSpec::Latest)
                .await?);
        };
        let idempotent = head.producer_id != NO_PRODUCER_ID;
        let count = set.records.len() as i32;
        if idempotent {
            match self.inner.producers.check(
                bucket,
                head.producer_id,
                head.producer_epoch,
                head.sequence,
                count,
            )? {
                Check::Append => {}
                Check::Duplicate { base_offset } => return Ok(base_offset),
            }
        }

        let log = record::to_log(&set.records, now)?;
        let info = self.inner.service.append(bucket, log).await?;
        if idempotent {
            self.inner.producers.accept(
                bucket,
                head.producer_id,
                head.producer_epoch,
                head.sequence,
                count,
                info.first_offset,
            );
        }

        Ok(info.first_offset)
    }
}
