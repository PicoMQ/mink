//! Trims a log by time-to-live, held back by what the lake, the latest snapshot and the stream still need.

use std::sync::Arc;

use mink_common::sync::lock;
use mink_metadata::View;
use mink_table::Bucket;

use crate::error::Error;
use crate::node::Node;
use crate::registry::{Hosted, RetentionFrontier};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    Kept,
    Trimmed { new_start: i64, expired_to: i64 },
    Held { expired_to: i64, held_at: i64 },
}

impl Node {
    pub async fn retain_all(&self) -> Vec<(Bucket, Result<Retention, Error>)> {
        let view = self.metadata().views().load();
        let mut results = Vec::new();
        for hosted in self.registry().all() {
            let bucket = hosted.bucket;
            results.push((bucket, self.retain(&view, hosted).await));
        }

        results
    }

    pub async fn retain(&self, view: &View, hosted: Arc<Hosted>) -> Result<Retention, Error> {
        let Some(ttl) = hosted.descriptor.options().log_ttl else {
            return Ok(Retention::Kept);
        };
        let cutoff = self.clock().millis() - i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        let offsets = hosted.log.offsets();
        if offsets.is_empty() {
            return Ok(Retention::Kept);
        }

        let frontier = (*lock(&hosted.retention))
            .filter(|f| f.offset >= offsets.log_start && f.offset <= offsets.high_watermark);
        let expired_to = match frontier {
            Some(RetentionFrontier {
                offset,
                timestamp: Some(timestamp),
            }) if timestamp >= cutoff => offset,
            _ => {
                let from = frontier.map_or(offsets.log_start, |f| f.offset);
                let expired_to = hosted.log.offset_for_timestamp_from(cutoff, from).await?;
                let timestamp = hosted.log.commit_timestamp_at(expired_to).await?;
                *lock(&hosted.retention) = Some(RetentionFrontier {
                    offset: expired_to,
                    timestamp,
                });

                expired_to
            }
        };
        if expired_to <= offsets.log_start {
            return Ok(Retention::Kept);
        }

        let bucket = hosted.bucket;
        let catalog = &view.state.catalog;
        let mut floor = view
            .state
            .streams
            .get(&hosted.stream_id)
            .map(|stream| i64::try_from(stream.end_offset).unwrap_or(i64::MAX))
            .unwrap_or(0);
        if hosted.descriptor.options().lake.is_some() {
            floor = floor.min(
                catalog
                    .lake
                    .get(&bucket.table())
                    .and_then(|row| row.bucket_log_end_offset.get(&bucket).copied())
                    .unwrap_or(0),
            );
        }
        if hosted.kv.is_some() {
            floor = floor.min(
                catalog
                    .latest_kv_snapshot(bucket)
                    .map(|snapshot| snapshot.log_offset)
                    .unwrap_or(0),
            );
        }

        let new_start = expired_to.min(floor);
        if new_start <= offsets.log_start {
            return Ok(Retention::Held {
                expired_to,
                held_at: floor,
            });
        }
        hosted.log.trim(new_start).await?;
        tracing::info!(
            ?bucket,
            from = offsets.log_start,
            to = new_start,
            expired_to,
            "log trimmed by ttl"
        );

        Ok(Retention::Trimmed {
            new_start,
            expired_to,
        })
    }
}
