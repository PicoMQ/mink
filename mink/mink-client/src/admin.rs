//! Catalog and cluster administration through the coordinator or any node.

use std::collections::BTreeMap;
use std::time::Duration;

use mink_table::{Bucket, Change, Descriptor, Id, PartitionId, PartitionSpec, Path, SchemaId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::proto::{self, action};
use crate::{Cluster, Error};

#[derive(Clone)]
pub struct Admin {
    cluster: Cluster,
}

impl Admin {
    pub(crate) fn new(cluster: Cluster) -> Self {
        Admin { cluster }
    }

    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    async fn coordinator_one<R: DeserializeOwned>(
        &self,
        name: &str,
        body: &impl Serialize,
    ) -> Result<R, Error> {
        self.cluster
            .coordinator_action(name, body)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Protocol(format!("{name} returned no result")))
    }

    async fn coordinator_unit(&self, name: &str, body: &impl Serialize) -> Result<(), Error> {
        self.cluster.coordinator_action::<Value>(name, body).await?;

        Ok(())
    }

    async fn any_one<R: DeserializeOwned>(
        &self,
        name: &str,
        body: &impl Serialize,
    ) -> Result<R, Error> {
        self.cluster.any_action_one(name, body).await
    }

    pub async fn metadata(&self) -> Result<proto::Metadata, Error> {
        self.cluster.refresh_metadata().await
    }

    pub async fn describe_cluster(&self) -> Result<proto::ClusterInfo, Error> {
        self.any_one(action::DESCRIBE_CLUSTER, &()).await
    }

    pub async fn get_config(&self, address: Option<&str>) -> Result<proto::ConfigEntries, Error> {
        self.node_one(address, action::GET_CONFIG, &()).await
    }

    pub async fn node_stats(&self, address: Option<&str>) -> Result<proto::NodeStats, Error> {
        self.node_one(address, action::NODE_STATS, &()).await
    }

    pub async fn cluster_stats(
        &self,
    ) -> Result<Vec<(proto::ClusterNode, Result<proto::NodeStats, Error>)>, Error> {
        let cluster = self.describe_cluster().await?;
        let mut out = Vec::with_capacity(cluster.nodes.len());
        for node in cluster.nodes {
            if !node.live {
                continue;
            }
            let stats = self.node_stats(Some(&node.address)).await;
            out.push((node, stats));
        }

        Ok(out)
    }

    pub async fn health(&self, address: Option<&str>) -> Result<proto::Health, Error> {
        self.node_one(address, action::HEALTH, &()).await
    }

    pub async fn rebalance(&self) -> Result<Vec<proto::BucketMove>, Error> {
        let rebalanced: proto::Rebalanced = self.coordinator_one(action::REBALANCE, &()).await?;

        Ok(rebalanced.moves)
    }

    async fn node_one<R: DeserializeOwned>(
        &self,
        address: Option<&str>,
        name: &str,
        body: &impl Serialize,
    ) -> Result<R, Error> {
        match address {
            Some(address) => {
                self.cluster
                    .connection(address)?
                    .action_one(name, body)
                    .await
            }

            None => self.cluster.any_action_one(name, body).await,
        }
    }

    pub async fn create_database(
        &self,
        name: &str,
        comment: Option<&str>,
        custom: BTreeMap<String, String>,
        ignore_if_exists: bool,
    ) -> Result<(), Error> {
        self.coordinator_unit(
            action::CREATE_DATABASE,
            &proto::CreateDatabase {
                name: name.to_owned(),
                comment: comment.map(str::to_owned),
                custom,
                ignore_if_exists,
            },
        )
        .await
    }

    pub async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<(), Error> {
        self.coordinator_unit(
            action::DROP_DATABASE,
            &proto::DropDatabase {
                name: name.to_owned(),
                ignore_if_not_exists,
                cascade,
            },
        )
        .await
    }

    pub async fn list_databases(&self) -> Result<Vec<String>, Error> {
        let names: proto::Names = self.any_one(action::LIST_DATABASES, &()).await?;

        Ok(names.names)
    }

    pub async fn database_exists(&self, name: &str) -> Result<bool, Error> {
        let exists: proto::Exists = self
            .any_one(
                action::DATABASE_EXISTS,
                &proto::DatabaseName {
                    name: name.to_owned(),
                },
            )
            .await?;
        Ok(exists.exists)
    }

    pub async fn create_table(
        &self,
        path: &Path,
        descriptor: &Descriptor,
        ignore_if_exists: bool,
    ) -> Result<Option<Id>, Error> {
        let created: proto::Created = self
            .coordinator_one(
                action::CREATE_TABLE,
                &proto::CreateTable {
                    path: path.clone(),
                    descriptor: descriptor.clone(),
                    ignore_if_exists,
                },
            )
            .await?;
        Ok(created.table_id)
    }

    pub async fn drop_table(&self, path: &Path, ignore_if_not_exists: bool) -> Result<(), Error> {
        self.coordinator_unit(
            action::DROP_TABLE,
            &proto::DropTable {
                path: path.clone(),
                ignore_if_not_exists,
            },
        )
        .await?;
        self.cluster.forget_table(path);

        Ok(())
    }

    pub async fn list_tables(&self, database: &str) -> Result<Vec<String>, Error> {
        let names: proto::Names = self
            .any_one(
                action::LIST_TABLES,
                &proto::DatabaseName {
                    name: database.to_owned(),
                },
            )
            .await?;
        Ok(names.names)
    }

    pub async fn table_exists(&self, path: &Path) -> Result<bool, Error> {
        let exists: proto::Exists = self
            .any_one(
                action::TABLE_EXISTS,
                &proto::TableRef { path: path.clone() },
            )
            .await?;
        Ok(exists.exists)
    }

    pub async fn get_table(&self, path: &Path) -> Result<proto::TableInfo, Error> {
        Ok((*self.cluster.refresh_table(path).await?).clone())
    }

    pub async fn alter_table(
        &self,
        path: &Path,
        changes: Vec<Change>,
        ignore_if_not_exists: bool,
    ) -> Result<Option<SchemaId>, Error> {
        let altered: proto::Altered = self
            .coordinator_one(
                action::ALTER_TABLE,
                &proto::AlterTable {
                    path: path.clone(),
                    changes,
                    ignore_if_not_exists,
                },
            )
            .await?;
        self.cluster.forget_table(path);

        Ok(altered.schema_id)
    }

    pub async fn create_partition(
        &self,
        path: &Path,
        spec: &PartitionSpec,
        ignore_if_exists: bool,
    ) -> Result<Option<PartitionId>, Error> {
        let created: proto::PartitionCreated = self
            .coordinator_one(
                action::CREATE_PARTITION,
                &proto::PartitionRequest {
                    path: path.clone(),
                    spec: spec.clone(),
                    ignore_if_exists,
                },
            )
            .await?;
        self.cluster.forget_table(path);

        Ok(created.partition_id)
    }

    pub async fn drop_partition(
        &self,
        path: &Path,
        spec: &PartitionSpec,
        ignore_if_not_exists: bool,
    ) -> Result<(), Error> {
        self.coordinator_unit(
            action::DROP_PARTITION,
            &proto::PartitionRequest {
                path: path.clone(),
                spec: spec.clone(),
                ignore_if_exists: ignore_if_not_exists,
            },
        )
        .await?;
        self.cluster.forget_table(path);

        Ok(())
    }

    pub async fn list_partitions(&self, path: &Path) -> Result<Vec<proto::PartitionInfo>, Error> {
        let partitions: proto::Partitions = self
            .any_one(
                action::LIST_PARTITIONS,
                &proto::TableRef { path: path.clone() },
            )
            .await?;
        Ok(partitions.partitions)
    }

    pub async fn init_writer(&self) -> Result<i64, Error> {
        let id: proto::WriterId = self.any_one(action::INIT_WRITER, &()).await?;

        Ok(id.writer_id)
    }

    pub async fn list_offset(
        &self,
        path: &Path,
        bucket: Bucket,
        spec: proto::OffsetSpec,
    ) -> Result<i64, Error> {
        self.cluster
            .with_leader(path, bucket, |connection| async move {
                connection.list_offset(bucket, spec).await
            })
            .await
    }

    pub async fn latest_kv_snapshot(
        &self,
        bucket: Bucket,
    ) -> Result<Option<proto::KvSnapshot>, Error> {
        let latest: proto::LatestKvSnapshot = self
            .any_one(action::LATEST_KV_SNAPSHOT, &proto::BucketRef { bucket })
            .await?;
        Ok(latest.snapshot)
    }

    pub async fn lake_snapshot(&self, path: &Path) -> Result<Option<proto::LakeSnapshot>, Error> {
        let result: proto::LakeSnapshotResult = self
            .any_one(
                action::LAKE_SNAPSHOT,
                &proto::TableRef { path: path.clone() },
            )
            .await?;
        Ok(result.snapshot)
    }

    pub async fn register_producer_offsets(
        &self,
        producer_id: &str,
        offsets: &BTreeMap<Bucket, i64>,
        ttl: Option<Duration>,
    ) -> Result<bool, Error> {
        let registered: proto::ProducerOffsetsRegistered = self
            .coordinator_one(
                action::REGISTER_PRODUCER_OFFSETS,
                &proto::RegisterProducerOffsets {
                    producer_id: producer_id.to_owned(),
                    offsets: offsets
                        .iter()
                        .map(|(bucket, offset)| proto::BucketOffset {
                            bucket: *bucket,
                            offset: *offset,
                        })
                        .collect(),
                    ttl_ms: ttl.map(|ttl| i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX)),
                },
            )
            .await?;
        Ok(registered.created)
    }

    pub async fn producer_offsets(
        &self,
        producer_id: &str,
    ) -> Result<Option<proto::ProducerOffsets>, Error> {
        let result: proto::ProducerOffsetsResult = self
            .any_one(
                action::GET_PRODUCER_OFFSETS,
                &proto::ProducerRef {
                    producer_id: producer_id.to_owned(),
                },
            )
            .await?;
        Ok(result.snapshot)
    }

    pub async fn delete_producer_offsets(&self, producer_id: &str) -> Result<(), Error> {
        self.coordinator_unit(
            action::DELETE_PRODUCER_OFFSETS,
            &proto::ProducerRef {
                producer_id: producer_id.to_owned(),
            },
        )
        .await
    }
}
