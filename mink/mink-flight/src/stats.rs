//! Cluster description, effective configuration, per-node statistics and the health reply.

use std::collections::BTreeMap;
use std::time::Duration;

use mink_common::sync::lock;

use crate::error::Error;
use crate::proto;
use crate::service::Flight;

fn duration(value: Duration) -> String {
    humantime::format_duration(value).to_string()
}

impl Flight {
    pub(crate) async fn cluster(&self) -> Result<proto::ClusterInfo, Error> {
        let view = self.inner.service.view();
        let live = self.inner.coordinator.live_nodes().await?;
        let catalog = &view.state.catalog;
        let mut leading: BTreeMap<i32, usize> = BTreeMap::new();
        let mut unled = 0;
        for row in catalog.buckets.values() {
            if row.leader < 0 {
                unled += 1;
            } else {
                *leading.entry(row.leader).or_default() += 1;
            }
        }

        Ok(proto::ClusterInfo {
            nodes: view
                .state
                .nodes
                .values()
                .map(|n| proto::ClusterNode {
                    node_id: n.node_id,
                    address: n.http_address.clone(),
                    epoch: n.epoch,
                    live: live.contains(&n.node_id),
                    leading: leading.get(&n.node_id).copied().unwrap_or(0),
                    protocols: n.protocol_addresses.clone(),
                })
                .collect(),
            coordinator: catalog
                .coordinator
                .as_ref()
                .map(|c| proto::CoordinatorInfo {
                    node_id: c.node_id,
                    address: c.address.clone(),
                    epoch: c.epoch,
                }),
            databases: catalog.databases.len(),
            tables: catalog.tables.len(),
            partitions: catalog.partitions.len(),
            buckets: catalog.buckets.len(),
            unled_buckets: unled,
        })
    }

    pub(crate) fn entries(&self) -> proto::ConfigEntries {
        let node = self.inner.service.node();
        let cfg = node.config();
        let coord = self.inner.coordinator.config();
        let mut entries = BTreeMap::new();
        let mut put = |key: &str, value: String| {
            entries.insert(key.to_owned(), value);
        };
        put("node_id", node.node_id().to_string());
        put("cluster_id", cfg.cluster_id.clone());
        put("data_dir", cfg.data_dir.display().to_string());
        put("kv_snapshot_interval", duration(cfg.kv_snapshot_interval));
        put(
            "log_retention_interval",
            duration(cfg.log_retention_interval),
        );
        put(
            "default_bucket_count",
            coord.default_bucket_count.to_string(),
        );
        put("snapshots_retained", coord.snapshots_retained.to_string());
        put("producer_offsets_ttl", duration(coord.producer_offsets_ttl));
        put(
            "producer_offsets_cleanup_interval",
            duration(coord.producer_offsets_cleanup_interval),
        );
        put(
            "flight.compression",
            format!("{:?}", self.inner.config.compression),
        );
        put(
            "flight.default_scan_bytes",
            self.inner.config.default_scan_bytes.to_string(),
        );
        put(
            "flight.default_snapshot_rows",
            self.inner.config.default_snapshot_rows.to_string(),
        );
        put(
            "flight.leader_wait",
            duration(self.inner.config.leader_wait),
        );
        for (key, value) in &self.inner.config.describe {
            entries.insert(key.clone(), value.clone());
        }
        proto::ConfigEntries { entries }
    }

    pub async fn node_stats(&self) -> proto::NodeStats {
        let node = self.inner.service.node();
        let view = self.inner.service.view();
        let mut buckets = Vec::new();
        for hosted in node.registry().all() {
            let offsets = hosted.log.offsets();
            let kv = match hosted.kv() {
                Some(tablet) => Some(proto::KvStats {
                    row_count: tablet.row_count().await,
                    flushed_log_offset: tablet.flushed_log_offset().await,
                    snapshot_log_offset: view
                        .state
                        .catalog
                        .latest_kv_snapshot(hosted.bucket)
                        .map(|row| row.log_offset)
                        .unwrap_or(-1),
                }),
                None => None,
            };
            let retention = lock(&hosted.retention).map(|f| proto::RetentionInfo {
                offset: f.offset,
                timestamp: f.timestamp,
            });
            buckets.push(proto::BucketStats {
                bucket: hosted.bucket,
                path: hosted.path.clone(),
                leader_epoch: hosted.leader_epoch,
                log_start_offset: offsets.log_start,
                high_watermark: offsets.high_watermark,
                log_end_offset: offsets.log_end,
                writers: hosted.log.writer_count(),
                kv,
                retention,
            });
        }
        buckets.sort_by_key(|b| b.bucket);
        let coordinator = self.inner.coordinator.is_leader();
        let tiering = if coordinator {
            self.inner
                .coordinator
                .tiering_status()
                .into_iter()
                .map(|s| proto::TieringStatus {
                    table_id: s.table_id,
                    path: s.path,
                    state: s.state.map(|s| s.name()).unwrap_or("none").to_owned(),
                    epoch: s.epoch,
                    last_tiered_ms: s.last_tiered_ms,
                    due_ms: s.due_ms,
                    heartbeat_ms: s.heartbeat_ms,
                })
                .collect()
        } else {
            Vec::new()
        };
        proto::NodeStats {
            node_id: node.node_id(),
            node_epoch: node.metadata().node_epoch(),
            coordinator,
            buckets,
            tiering,
        }
    }

    pub fn health(&self) -> proto::Health {
        let node = self.inner.service.node();
        let view = self.inner.service.view();
        let epoch = node.metadata().node_epoch();
        proto::Health {
            node_id: node.node_id(),
            node_epoch: epoch,
            registered: view
                .state
                .nodes
                .get(&node.node_id())
                .is_some_and(|n| n.epoch == epoch),
            coordinator: self.inner.coordinator.is_leader(),
            hosted_buckets: node.registry().len(),
            uptime_ms: node.clock().millis() - self.inner.started_ms,
        }
    }
}
