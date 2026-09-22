//! Replays the changelog from a recovery point into key-value operations, tracking auto-increment ids.

use std::sync::Arc;

use bytes::Bytes;
use mink_log::{FetchIsolation, Tablet};
use mink_record::{Batch, ChangeType, Codec, Row, Scalar};

use crate::Error;
use crate::autoinc::IdRange;
use crate::schema::{Version, Versions};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoverPoint {
    pub log_offset: i64,
    pub row_count: i64,
    pub auto_increment: Option<IdRange>,
}

impl RecoverPoint {
    pub fn fresh(log: &Tablet) -> Self {
        RecoverPoint {
            log_offset: log.log_start_offset(),
            row_count: 0,
            auto_increment: None,
        }
    }
}

pub struct Replayed {
    pub change: ChangeType,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub offset: i64,
    pub auto_increment: Option<i64>,
}

pub struct Replay<'a> {
    log: &'a Tablet,
    versions: &'a Versions,
    codec: &'a dyn Codec,
    latest: Arc<Version>,
    next: i64,
    isolation: FetchIsolation,
    max_bytes: usize,
    auto_increment: Option<usize>,
}

impl<'a> Replay<'a> {
    pub fn new(
        log: &'a Tablet,
        versions: &'a Versions,
        codec: &'a dyn Codec,
        from: i64,
        isolation: FetchIsolation,
        max_bytes: usize,
        auto_increment: Option<usize>,
    ) -> Result<Self, Error> {
        Ok(Replay {
            log,
            versions,
            codec,
            latest: versions.latest()?,
            next: from,
            isolation,
            max_bytes,
            auto_increment,
        })
    }

    pub fn offset(&self) -> i64 {
        self.next
    }

    pub async fn next_batch(&mut self) -> Result<Option<Vec<Replayed>>, Error> {
        let fetched = self
            .log
            .read(self.next, self.max_bytes, self.isolation, None)
            .await?;
        if fetched.batches.is_empty() {
            return Ok(None);
        }

        let mut out = Vec::new();
        for bytes in fetched.batches {
            let batch = Batch::parse(bytes)?;
            let header = *batch.header();
            let version = self.versions.get(header.schema_id)?;
            let records = batch.records(self.codec, version.arrow.clone(), None)?;
            let keys = version.keys.bind(&records.batch)?;
            let readers = version.readers(&records.batch)?;
            let remap = self.versions.remap(&version, &self.latest);
            for index in 0..records.batch.num_rows() {
                let offset = header.base_offset + index as i64;
                if offset < self.next {
                    continue;
                }
                let change = records.changes.get(index);
                if change == ChangeType::UpdateBefore {
                    continue;
                }
                let key = Bytes::from(keys.encode_vec(index)?);
                let mut id = None;
                let value = if change == ChangeType::Delete {
                    None
                } else {
                    let written: Row<'_> = readers.iter().map(|r| r.get(index)).collect();
                    let row = remap.row(&written);
                    if change == ChangeType::Insert
                        && let Some(position) = self.auto_increment
                    {
                        id = match row.get(position).copied().flatten() {
                            Some(Scalar::Int(v)) => Some(i64::from(v)),
                            Some(Scalar::BigInt(v)) => Some(v),
                            _ => None,
                        };
                    }
                    Some(self.latest.encode(&row)?.encode())
                };
                out.push(Replayed {
                    change,
                    key,
                    value,
                    offset,
                    auto_increment: id,
                });
            }
            self.next = header.next_offset();
        }

        Ok(Some(out))
    }
}
