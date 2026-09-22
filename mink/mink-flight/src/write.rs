//! The write path served over DoPut: decodes batches, appends or puts them to a bucket, or routes a
//! table-addressed batch to its buckets locally or through the leader, creating partitions on demand.

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::{FlightData, PutResult};
use futures::{StreamExt, TryStreamExt};
use mink_metadata::{TableRow, View};
use mink_record::{Router, Spec, codec as log_codec, take};
use mink_server::Owner;
use mink_table::{Bucket, PartitionId, PartitionName, PartitionSpec, Path};
use mink_tablet::Put;
use tokio::time::Instant;
use tonic::Streaming;

use crate::codec;
use crate::error::Error;
use crate::proto::{self, Write, action};
use crate::service::{BoxStream, Flight, Locator, decode};

impl Flight {
    pub(crate) fn write(&self, mut stream: Streaming<FlightData>) -> BoxStream<PutResult> {
        self.spawn_stream(move |this, tx| async move {
            let first = match stream.message().await {
                Ok(Some(first)) => first,
                Ok(None) => return Ok(()),
                Err(status) => return Err(Error::Request(status.to_string())),
            };
            let descriptor = first
                .flight_descriptor
                .as_ref()
                .ok_or_else(|| Error::Request("DoPut needs a descriptor".into()))?;
            let write: Write = decode(&descriptor.cmd)?;
            let (table, router) = match &write {
                Write::Append {
                    bucket, schema_id, ..
                }
                | Write::Put {
                    bucket, schema_id, ..
                } => (
                    this.with_schema(Locator::Bucket(*bucket), *schema_id)
                        .await?,
                    None,
                ),
                Write::AppendTable { path, schema_id }
                | Write::PutTable {
                    path, schema_id, ..
                } => {
                    let table = this.with_schema(Locator::Path(path), *schema_id).await?;
                    let schema = &table.schemas[schema_id.0 as usize];
                    let router = Router::new(schema.fields(), &table.descriptor)?;
                    (table, Some(router))
                }
            };

            let frames = futures::stream::once(async { Ok(first) })
                .chain(stream.map_err(|s| FlightError::Tonic(Box::new(s))));
            let mut decoder = FlightDataDecoder::new(frames);
            while let Some(frame) = decoder.try_next().await.map_err(Error::request)? {
                let DecodedPayload::RecordBatch(batch) = frame.payload else {
                    continue;
                };
                let meta: proto::WriteBatch = if frame.inner.app_metadata.is_empty() {
                    proto::WriteBatch::default()
                } else {
                    decode(&frame.inner.app_metadata)?
                };
                let app_metadata = match &router {
                    None => {
                        let written = this.write_batch(&table, &write, &meta, batch).await?;
                        serde_json::to_vec(&written)?
                    }

                    Some(router) => {
                        let routed = this.routed(&table, router, &write, &meta, batch).await?;
                        serde_json::to_vec(&routed)?
                    }
                };
                let result = PutResult {
                    app_metadata: app_metadata.into(),
                };
                if tx.send(Ok(result)).await.is_err() {
                    return Ok(());
                }
            }

            Ok(())
        })
    }

    async fn write_batch(
        &self,
        table: &TableRow,
        write: &Write,
        meta: &proto::WriteBatch,
        batch: RecordBatch,
    ) -> Result<proto::Written, Error> {
        let writer = |writer_id: Option<i64>| -> Result<Option<(i64, i32)>, Error> {
            match (writer_id, meta.batch_sequence) {
                (None, _) => Ok(None),
                (Some(id), Some(sequence)) => Ok(Some((id, sequence))),
                (Some(_), None) => Err(Error::Request(
                    "a writer id needs a batch_sequence per batch".into(),
                )),
            }
        };
        let changes = codec::changes(batch.num_rows(), meta.changes.as_deref())?;
        let info = match write {
            Write::Append {
                bucket,
                schema_id,
                writer_id,
            } => {
                let schema = table
                    .schemas
                    .get(schema_id.0 as usize)
                    .ok_or(Error::SchemaNotExist(schema_id.0))?;
                codec::check(&batch, schema)?;
                let append_only = meta.changes.is_none();
                let mut spec = Spec::new(*schema_id, append_only);
                if let Some((id, sequence)) = writer(*writer_id)? {
                    spec = spec.with_writer(id, sequence);
                }
                let codec = log_codec(
                    table.descriptor.options().log_format,
                    self.inner.config.compression,
                );
                let bytes = codec::log_batch(spec, &changes, &batch, codec.as_ref())?;
                self.inner.service.append(*bucket, bytes).await?
            }
            Write::Put {
                bucket,
                schema_id,
                writer_id,
                target_columns,
            } => {
                let schema = table
                    .schemas
                    .get(schema_id.0 as usize)
                    .ok_or(Error::SchemaNotExist(schema_id.0))?;
                codec::check(&batch, schema)?;
                let mut put = Put::upsert(*schema_id, batch).with_ops(codec::ops(&changes));
                if let Some((id, sequence)) = writer(*writer_id)? {
                    put = put.with_writer(id, sequence);
                }
                if let Some(columns) = target_columns {
                    put = put.with_target_columns(columns.clone());
                }
                self.inner.service.put(*bucket, put).await?
            }
            Write::AppendTable { .. } | Write::PutTable { .. } => {
                unreachable!("routed writes go through routed")
            }
        };

        Ok(proto::Written {
            first_offset: info.first_offset,
            last_offset: info.last_offset,
            duplicated: info.duplicated,
        })
    }

    async fn routed(
        &self,
        table: &TableRow,
        router: &Router,
        write: &Write,
        meta: &proto::WriteBatch,
        batch: RecordBatch,
    ) -> Result<proto::Routed, Error> {
        let (path, schema_id, target_columns) = match write {
            Write::AppendTable { path, schema_id } => (path, *schema_id, None),
            Write::PutTable {
                path,
                schema_id,
                target_columns,
            } => (path, *schema_id, target_columns.clone()),
            Write::Append { .. } | Write::Put { .. } => {
                unreachable!("bucket writes go through write_batch")
            }
        };
        codec::changes(batch.num_rows(), meta.changes.as_deref())?;
        let mut buckets = Vec::new();
        for group in router.split(&batch)? {
            let partition_id = match &group.partition {
                None => None,
                Some(name) => Some(self.partition(table, path, name).await?),
            };
            let bucket = Bucket::of(table.table_id, partition_id, group.bucket);
            let (rows, changes) = take(&batch, meta.changes.as_deref(), &group)?;
            let piece = proto::WriteBatch {
                batch_sequence: None,
                changes,
            };
            let addressed = if table.descriptor.has_primary_key() {
                Write::Put {
                    bucket,
                    schema_id,
                    writer_id: None,
                    target_columns: target_columns.clone(),
                }
            } else {
                Write::Append {
                    bucket,
                    schema_id,
                    writer_id: None,
                }
            };
            let written = match self.owner(bucket).await? {
                Owner::Local => self.write_batch(table, &addressed, &piece, rows).await?,
                Owner::Remote { address, .. } => {
                    self.inner
                        .forwarder
                        .put(&address, &addressed, rows, &piece)
                        .await?
                }
                Owner::Unknown => {
                    return Err(Error::Server(mink_server::Error::BucketNotExist(bucket)));
                }
            };
            buckets.push(proto::RoutedBucket {
                bucket,
                partition: group.partition,
                rows: group.rows.len(),
                first_offset: written.first_offset,
                last_offset: written.last_offset,
            });
        }

        Ok(proto::Routed { buckets })
    }

    async fn partition(
        &self,
        table: &TableRow,
        path: &Path,
        name: &PartitionName,
    ) -> Result<PartitionId, Error> {
        let existing = |view: &View| {
            view.state
                .catalog
                .partitions
                .get(&(table.table_id, name.clone()))
                .map(|row| row.partition_id)
        };
        if let Some(id) = existing(&self.inner.service.view()) {
            return Ok(id);
        }
        let spec = PartitionSpec::from_name(table.descriptor.partition_keys(), name)
            .map_err(Error::request)?;
        let request = proto::PartitionRequest {
            path: path.clone(),
            spec,
            ignore_if_exists: true,
        };
        let created: Option<PartitionId> = match self.coordinator() {
            Ok(coordinator) => {
                coordinator
                    .create_partition(&request.path, &request.spec, true)
                    .await?
            }

            Err(Error::NotCoordinator {
                coordinator: Some(node),
            }) => {
                let mut results = self
                    .inner
                    .forwarder
                    .action(&node.address, action::CREATE_PARTITION, &request)
                    .await?;
                let body = results
                    .pop()
                    .ok_or_else(|| Error::Internal("create_partition returned nothing".into()))?;
                decode::<proto::PartitionCreated>(&body)?.partition_id
            }

            Err(error) => return Err(error),
        };
        if let Some(id) = created {
            return Ok(id);
        }
        let mut applied = self.inner.service.node().metadata().views().subscribe();
        let deadline = Instant::now() + self.inner.config.leader_wait;
        loop {
            if let Some(id) = existing(&self.inner.service.view()) {
                return Ok(id);
            }
            if tokio::time::timeout_at(deadline, applied.changed())
                .await
                .is_err()
            {
                return Err(Error::TableNotExist(format!("{path} partition {name}")));
            }
        }
    }

    async fn owner(&self, bucket: Bucket) -> Result<Owner, Error> {
        let mut applied = self.inner.service.node().metadata().views().subscribe();
        let deadline = Instant::now() + self.inner.config.leader_wait;
        loop {
            let node = self.inner.service.node();
            if node.registry().contains(bucket) {
                return Ok(Owner::Local);
            }
            match self.inner.service.owner(bucket) {
                remote @ Owner::Remote { .. } => return Ok(remote),
                Owner::Local | Owner::Unknown => {}
            }
            if Instant::now() >= deadline {
                return Err(Error::NoLeader(bucket, self.inner.config.leader_wait));
            }
            tokio::select! {
                _ = applied.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }
}
