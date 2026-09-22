//! Append and upsert writers that route batches to buckets with idempotent per-bucket sequences.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use mink_record::{ChangeType, Router, take};
use mink_table::Bucket;

use crate::proto::{self, Write};
use crate::{Error, Table};

pub type Routed = Vec<proto::RoutedBucket>;

struct Writer {
    table: Table,
    router: Router,
    writer_id: Option<i64>,
    sequences: HashMap<Bucket, i32>,
}

impl Writer {
    fn new(table: Table, writer_id: Option<i64>) -> Result<Self, Error> {
        let router = Router::new(table.schema().fields(), table.descriptor())?;
        Ok(Writer {
            table,
            router,
            writer_id,
            sequences: HashMap::new(),
        })
    }

    async fn write(
        &mut self,
        batch: &RecordBatch,
        changes: Option<&[u8]>,
        make: impl Fn(Bucket) -> Write,
    ) -> Result<Routed, Error> {
        if let Some(changes) = changes
            && changes.len() != batch.num_rows()
        {
            return Err(Error::Protocol(format!(
                "{} change types for {} rows",
                changes.len(),
                batch.num_rows()
            )));
        }

        let mut routed = Vec::new();
        for group in self.router.split(batch)? {
            let bucket = self
                .table
                .resolve(group.partition.as_ref(), group.bucket)
                .await?;
            let (rows, changes) = take(batch, changes, &group)?;
            let sequence = self.writer_id.map(|_| {
                let next = self.sequences.entry(bucket).or_insert(0);
                let sequence = *next;
                *next += 1;
                sequence
            });
            let write = make(bucket);
            let meta = proto::WriteBatch {
                batch_sequence: sequence,
                changes,
            };
            // A retry resends the same sequence so the server can drop the duplicate.
            let acks = self
                .table
                .cluster()
                .with_leader(self.table.path(), bucket, |connection| {
                    let write = write.clone();
                    let rows = rows.clone();
                    let meta = meta.clone();
                    async move { connection.put(&write, vec![(rows, meta)]).await }
                })
                .await?;
            let ack = acks
                .first()
                .ok_or_else(|| Error::Protocol("write returned no ack".into()))?;
            let written: proto::Written = serde_json::from_slice(ack)?;
            routed.push(proto::RoutedBucket {
                bucket,
                partition: group.partition,
                rows: group.rows.len(),
                first_offset: written.first_offset,
                last_offset: written.last_offset,
            });
        }

        Ok(routed)
    }
}

pub struct Append {
    writer: Writer,
}

impl Append {
    pub(crate) async fn new(table: Table) -> Result<Self, Error> {
        if table.descriptor().has_primary_key() {
            return Err(Error::PrimaryKey(table.path().clone()));
        }

        let writer_id = table.cluster().admin().init_writer().await?;

        Ok(Append {
            writer: Writer::new(table, Some(writer_id))?,
        })
    }

    pub fn writer_id(&self) -> i64 {
        self.writer.writer_id.expect("writers are idempotent")
    }

    pub async fn append(&mut self, batch: &RecordBatch) -> Result<Routed, Error> {
        let schema_id = self.writer.table.schema_id();
        let writer_id = self.writer.writer_id;
        self.writer
            .write(batch, None, |bucket| Write::Append {
                bucket,
                schema_id,
                writer_id,
            })
            .await
    }
}

pub struct Upsert {
    writer: Writer,
    columns: Option<Vec<usize>>,
}

impl Upsert {
    pub(crate) async fn new(table: Table, columns: Option<Vec<usize>>) -> Result<Self, Error> {
        if !table.descriptor().has_primary_key() {
            return Err(Error::NotPrimaryKey(table.path().clone()));
        }
        if let Some(targets) = &columns {
            let key = table.schema().primary_key_indexes();
            if let Some(missing) = key.iter().find(|k| !targets.contains(k)) {
                return Err(Error::Protocol(format!(
                    "partial update must include primary key column {missing}"
                )));
            }
        }

        let writer_id = table.cluster().admin().init_writer().await?;

        Ok(Upsert {
            writer: Writer::new(table, Some(writer_id))?,
            columns,
        })
    }

    pub fn writer_id(&self) -> i64 {
        self.writer.writer_id.expect("writers are idempotent")
    }

    pub async fn upsert(&mut self, batch: &RecordBatch) -> Result<Routed, Error> {
        self.write(batch, None).await
    }

    pub async fn delete(&mut self, batch: &RecordBatch) -> Result<Routed, Error> {
        let changes = vec![ChangeType::Delete.byte(); batch.num_rows()];
        self.write(batch, Some(&changes)).await
    }

    pub async fn write(
        &mut self,
        batch: &RecordBatch,
        changes: Option<&[u8]>,
    ) -> Result<Routed, Error> {
        let schema_id = self.writer.table.schema_id();
        let writer_id = self.writer.writer_id;
        let columns = self.columns.clone();
        self.writer
            .write(batch, changes, |bucket| Write::Put {
                bucket,
                schema_id,
                writer_id,
                target_columns: columns.clone(),
            })
            .await
    }
}
