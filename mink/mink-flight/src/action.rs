//! Dispatches DoAction requests by name to the service, the coordinator or the catalog views.

use std::time::Duration;

use arrow_flight::Action;
use bytes::Bytes;

use crate::codec::{self, Schemas};
use crate::error::Error;
use crate::proto::{self, action};
use crate::service::{Flight, coordinator_info, decode, one, table_at};

impl Flight {
    pub(crate) async fn action(&self, action: Action) -> Result<Vec<Bytes>, Error> {
        let body = &action.body;
        let service = &self.inner.service;

        Ok(match action.r#type.as_str() {
            action::INIT_WRITER => one(&proto::WriterId {
                writer_id: service.init_writer().await?,
            })?,
            action::LIST_OFFSETS => {
                let req: proto::ListOffsets = decode(body)?;
                let spec = match req.spec {
                    proto::OffsetSpec::Earliest => mink_server::OffsetSpec::Earliest,
                    proto::OffsetSpec::Latest => mink_server::OffsetSpec::Latest,
                    proto::OffsetSpec::Timestamp { timestamp } => {
                        mink_server::OffsetSpec::Timestamp(timestamp)
                    }
                };
                one(&proto::Offset {
                    offset: service.list_offset(req.bucket, spec).await?,
                })?
            }
            action::LOOKUP => {
                let req: proto::Lookup = decode(body)?;
                let view = service.view();
                let table = self.table(&view, req.bucket)?;
                let keys: Vec<&[u8]> = req.keys.iter().map(|k| k.as_slice()).collect();
                let values = service.multi_lookup(req.bucket, &keys)?;
                let schemas = Schemas::new(&table.schemas);
                let format = table.descriptor.options().kv_format;
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    let rows: Vec<Bytes> = value.into_iter().collect();
                    let batch = codec::rows(&rows, &schemas, format, None)?;
                    out.push(codec::ipc_stream(&batch)?.into());
                }
                out
            }
            action::PREFIX_LOOKUP => {
                let req: proto::PrefixLookup = decode(body)?;
                let view = service.view();
                let table = self.table(&view, req.bucket)?;
                let values = service.prefix_lookup(req.bucket, &req.prefix)?;
                let batch = codec::rows(
                    &values,
                    &Schemas::new(&table.schemas),
                    table.descriptor.options().kv_format,
                    None,
                )?;
                vec![codec::ipc_stream(&batch)?.into()]
            }
            action::METADATA => {
                let view = service.view();
                one(&proto::Metadata {
                    nodes: view
                        .state
                        .nodes
                        .values()
                        .map(|n| proto::NodeInfo {
                            node_id: n.node_id,
                            address: n.http_address.clone(),
                        })
                        .collect(),
                    coordinator: coordinator_info(&view),
                })?
            }
            action::CREATE_DATABASE => {
                let req: proto::CreateDatabase = decode(body)?;
                self.coordinator()?
                    .create_database(&req.name, req.comment, req.custom, req.ignore_if_exists)
                    .await?;
                vec![]
            }
            action::DROP_DATABASE => {
                let req: proto::DropDatabase = decode(body)?;
                self.coordinator()?
                    .drop_database(&req.name, req.ignore_if_not_exists, req.cascade)
                    .await?;
                vec![]
            }
            action::LIST_DATABASES => one(&proto::Names {
                names: service
                    .view()
                    .state
                    .catalog
                    .databases
                    .keys()
                    .cloned()
                    .collect(),
            })?,
            action::DATABASE_EXISTS => {
                let req: proto::DatabaseName = decode(body)?;
                one(&proto::Exists {
                    exists: service
                        .view()
                        .state
                        .catalog
                        .databases
                        .contains_key(&req.name),
                })?
            }
            action::CREATE_TABLE => {
                let req: proto::CreateTable = decode(body)?;
                let table_id = self
                    .coordinator()?
                    .create_table(&req.path, &req.descriptor, req.ignore_if_exists)
                    .await?;
                one(&proto::Created { table_id })?
            }
            action::DROP_TABLE => {
                let req: proto::DropTable = decode(body)?;
                self.coordinator()?
                    .drop_table(&req.path, req.ignore_if_not_exists)
                    .await?;
                vec![]
            }
            action::LIST_TABLES => {
                let req: proto::DatabaseName = decode(body)?;
                let view = service.view();
                if !view.state.catalog.databases.contains_key(&req.name) {
                    return Err(Error::Coordinator(
                        mink_metadata::Error::DatabaseNotExist { name: req.name }.into(),
                    ));
                }
                one(&proto::Names {
                    names: view
                        .state
                        .catalog
                        .tables
                        .keys()
                        .filter(|p| p.database().as_str() == req.name)
                        .map(|p| p.table().as_str().to_owned())
                        .collect(),
                })?
            }
            action::TABLE_EXISTS => {
                let req: proto::TableRef = decode(body)?;
                one(&proto::Exists {
                    exists: service.view().state.catalog.tables.contains_key(&req.path),
                })?
            }
            action::GET_TABLE => {
                let req: proto::TableRef = decode(body)?;
                one(&self.table_info(&service.view(), &req.path)?)?
            }
            action::ALTER_TABLE => {
                let req: proto::AlterTable = decode(body)?;
                let schema_id = self
                    .coordinator()?
                    .alter_table(&req.path, &req.changes, req.ignore_if_not_exists)
                    .await?;
                one(&proto::Altered { schema_id })?
            }
            action::CREATE_PARTITION => {
                let req: proto::PartitionRequest = decode(body)?;
                let partition_id = self
                    .coordinator()?
                    .create_partition(&req.path, &req.spec, req.ignore_if_exists)
                    .await?;
                one(&proto::PartitionCreated { partition_id })?
            }
            action::DROP_PARTITION => {
                let req: proto::PartitionRequest = decode(body)?;
                self.coordinator()?
                    .drop_partition(&req.path, &req.spec, req.ignore_if_exists)
                    .await?;
                vec![]
            }
            action::LIST_PARTITIONS => {
                let req: proto::TableRef = decode(body)?;
                let view = service.view();
                let table = table_at(&view, &req.path)?;
                one(&proto::Partitions {
                    partitions: view
                        .state
                        .catalog
                        .partitions_of(table.table_id)
                        .map(|p| proto::PartitionInfo {
                            partition_id: p.partition_id,
                            name: p.name.clone(),
                        })
                        .collect(),
                })?
            }
            action::REGISTER_PRODUCER_OFFSETS => {
                let req: proto::RegisterProducerOffsets = decode(body)?;
                let ttl = match req.ttl_ms {
                    Some(ms) if ms < 0 => {
                        return Err(Error::Request(format!("ttl_ms {ms} is negative")));
                    }

                    Some(ms) => Some(Duration::from_millis(ms as u64)),
                    None => None,
                };
                let created = self
                    .coordinator()?
                    .register_producer_offsets(
                        &req.producer_id,
                        req.offsets.iter().map(|o| (o.bucket, o.offset)).collect(),
                        ttl,
                    )
                    .await?;
                one(&proto::ProducerOffsetsRegistered { created })?
            }
            action::GET_PRODUCER_OFFSETS => {
                let req: proto::ProducerRef = decode(body)?;
                let snapshot = self
                    .inner
                    .coordinator
                    .producer_offsets(&req.producer_id)
                    .map(|row| proto::ProducerOffsets {
                        producer_id: req.producer_id.clone(),
                        expires_ms: row.expires_ms,
                        offsets: row
                            .offsets
                            .iter()
                            .map(|(bucket, offset)| proto::BucketOffset {
                                bucket: *bucket,
                                offset: *offset,
                            })
                            .collect(),
                    });
                one(&proto::ProducerOffsetsResult { snapshot })?
            }
            action::DELETE_PRODUCER_OFFSETS => {
                let req: proto::ProducerRef = decode(body)?;
                self.coordinator()?
                    .delete_producer_offsets(&req.producer_id)
                    .await?;
                vec![]
            }
            action::LATEST_KV_SNAPSHOT => {
                let req: proto::BucketRef = decode(body)?;
                one(&proto::LatestKvSnapshot {
                    snapshot: service
                        .view()
                        .state
                        .catalog
                        .latest_kv_snapshot(req.bucket)
                        .map(|row| proto::KvSnapshot {
                            snapshot_id: row.snapshot_id,
                            log_offset: row.log_offset,
                            row_count: row.row_count,
                            path: row.path.clone(),
                        }),
                })?
            }
            action::LAKE_SNAPSHOT => {
                let req: proto::TableRef = decode(body)?;
                let view = service.view();
                let table = table_at(&view, &req.path)?;
                one(&proto::LakeSnapshotResult {
                    snapshot: view.state.catalog.lake.get(&table.table_id).map(|lake| {
                        proto::LakeSnapshot {
                            snapshot_id: lake.snapshot_id,
                            bucket_log_end_offset: lake
                                .bucket_log_end_offset
                                .iter()
                                .map(|(b, o)| (*b, *o))
                                .collect(),
                        }
                    }),
                })?
            }
            action::DESCRIBE_CLUSTER => one(&self.cluster().await?)?,
            action::GET_CONFIG => one(&self.entries())?,
            action::NODE_STATS => one(&self.node_stats().await)?,
            action::REBALANCE => {
                let moves = self.coordinator()?.rebalance().await?;
                one(&proto::Rebalanced {
                    moves: moves
                        .iter()
                        .map(|m| proto::BucketMove {
                            bucket: m.bucket,
                            from: m.from,
                            to: m.to,
                        })
                        .collect(),
                })?
            }
            action::HEALTH => one(&self.health())?,
            other => return Err(Error::UnknownAction(other.to_owned())),
        })
    }
}
