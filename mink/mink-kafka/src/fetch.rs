//! The fetch path: long-poll reads across the requested partitions, each log batch re-encoded as one
//! Kafka batch at its original offsets; fetch sessions are answered but never kept.

use std::time::Duration;

use bytes::BytesMut;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use kafka_protocol::messages::fetch_response::{
    FetchableTopicResponse, LeaderIdAndEpoch, PartitionData,
};
use kafka_protocol::messages::{BrokerId, FetchRequest, FetchResponse};
use mink_log::FetchInfo;
use mink_metadata::View;
use mink_server::OffsetSpec;
use mink_table::Bucket;

use crate::Kafka;
use crate::dispatch::Request;
use crate::error::Error;
use crate::record;

struct Part {
    index: i32,
    bucket: Result<Bucket, Error>,
    offset: i64,
    max_bytes: usize,
}

impl Kafka {
    pub(crate) async fn fetch(
        &self,
        _request: &Request,
        fetch: FetchRequest,
    ) -> Result<FetchResponse, Error> {
        let view = self.inner.service.view();
        let config = self.config();
        let wait = Duration::from_millis(fetch.max_wait_ms.max(0) as u64).min(config.max_wait);
        let min_bytes = fetch.min_bytes.max(0) as usize;
        let total_cap = match fetch.max_bytes {
            n if n > 0 => (n as usize).min(config.max_fetch_bytes),
            _ => config.max_fetch_bytes,
        };

        let mut parts = Vec::new();
        let mut layout = Vec::with_capacity(fetch.topics.len());
        for topic in &fetch.topics {
            let resolved = self.topic(&view, topic.topic.as_str());
            let start = parts.len();
            for partition in &topic.partitions {
                let bucket = match &resolved {
                    Ok(t) => t.bucket(partition.partition),
                    Err(Error::InvalidTopic(name)) => Err(Error::InvalidTopic(name.clone())),
                    Err(_) => Err(Error::UnknownTopic(topic.topic.to_string())),
                };
                parts.push(Part {
                    index: partition.partition,
                    bucket,
                    offset: partition.fetch_offset,
                    max_bytes: match partition.partition_max_bytes {
                        n if n > 0 => (n as usize).min(total_cap),
                        _ => total_cap,
                    },
                });
            }
            layout.push((topic.topic.clone(), topic.topic_id, start..parts.len()));
        }

        let results = self.read_all(&parts, min_bytes, wait).await;

        let mut responses = Vec::with_capacity(layout.len());
        for (name, id, range) in layout {
            let mut partitions = Vec::with_capacity(range.len());
            for (part, result) in parts[range.clone()].iter().zip(&results[range]) {
                partitions.push(self.partition_data(&view, part, result).await);
            }
            responses.push(
                FetchableTopicResponse::default()
                    .with_topic(name)
                    .with_topic_id(id)
                    .with_partitions(partitions),
            );
        }

        Ok(FetchResponse::default()
            .with_session_id(0)
            .with_responses(responses))
    }

    async fn read_all(
        &self,
        parts: &[Part],
        min_bytes: usize,
        wait: Duration,
    ) -> Vec<Result<FetchInfo, Error>> {
        let service = &self.inner.service;
        let mut results: Vec<Option<Result<FetchInfo, Error>>> =
            parts.iter().map(|_| None).collect();
        let mut pending: FuturesUnordered<_> = parts
            .iter()
            .enumerate()
            .filter_map(|(i, part)| {
                let bucket = *part.bucket.as_ref().ok()?;
                Some(async move {
                    let read = service
                        .fetch_wait(bucket, part.offset, part.max_bytes, 1, wait, None)
                        .await;
                    (i, read)
                })
            })
            .collect();

        let mut total = 0;
        while let Some((i, read)) = pending.next().await {
            if let Ok(info) = &read {
                total += info.size();
            }
            results[i] = Some(read.map_err(Error::from));
            if total >= min_bytes.max(1) {
                break;
            }
        }
        drop(pending);

        for (i, part) in parts.iter().enumerate() {
            if results[i].is_some() {
                continue;
            }
            results[i] = Some(match &part.bucket {
                Ok(bucket) => service
                    .fetch(*bucket, part.offset, part.max_bytes, None)
                    .await
                    .map_err(Error::from),
                Err(Error::InvalidTopic(name)) => Err(Error::InvalidTopic(name.clone())),
                Err(Error::UnknownPartition(topic, p)) => {
                    Err(Error::UnknownPartition(topic.clone(), *p))
                }
                Err(e) => Err(Error::UnknownTopic(e.to_string())),
            });
        }

        results
            .into_iter()
            .map(|r| r.expect("every partition read"))
            .collect()
    }

    async fn partition_data(
        &self,
        view: &View,
        part: &Part,
        result: &Result<FetchInfo, Error>,
    ) -> PartitionData {
        let data = PartitionData::default().with_partition_index(part.index);
        let (bucket, info) = match (&part.bucket, result) {
            (Ok(bucket), Ok(info)) => (*bucket, info),
            (_, Err(e)) | (Err(e), _) => {
                let mut failed = data
                    .with_error_code(e.code())
                    .with_high_watermark(-1)
                    .with_last_stable_offset(-1)
                    .with_log_start_offset(-1);
                let leader = part
                    .bucket
                    .as_ref()
                    .ok()
                    .and_then(|b| self.leader(view, *b));
                if let Some((leader, epoch)) = leader {
                    failed = failed.with_current_leader(
                        LeaderIdAndEpoch::default()
                            .with_leader_id(BrokerId(leader))
                            .with_leader_epoch(epoch),
                    );
                }
                return failed;
            }
        };

        let log_start = self
            .inner
            .service
            .list_offset(bucket, OffsetSpec::Earliest)
            .await
            .unwrap_or(0);
        let mut records = BytesMut::new();
        for bytes in &info.batches {
            match record::from_log(bytes.clone()).and_then(|(_, rows)| record::to_kafka(&rows)) {
                Ok(encoded) => records.extend_from_slice(&encoded),
                Err(e) => {
                    return data
                        .with_error_code(e.code())
                        .with_high_watermark(info.high_watermark)
                        .with_last_stable_offset(info.high_watermark)
                        .with_log_start_offset(log_start);
                }
            }
        }

        data.with_high_watermark(info.high_watermark)
            .with_last_stable_offset(info.high_watermark)
            .with_log_start_offset(log_start)
            .with_records(Some(records.freeze()))
    }
}
