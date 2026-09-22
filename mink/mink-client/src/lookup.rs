//! Point and prefix lookups on a primary-key table, routing encoded keys to their buckets.

use std::collections::BTreeMap;
use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use mink_record::{KeyEncoder, Router};
use mink_table::Bucket;
use mink_types::Fields;

use crate::proto::{self, action};
use crate::{Error, Table};

pub struct Lookup {
    table: Table,
    key: Keyed,
    prefix: Keyed,
}

struct Keyed {
    fields: Fields,
    router: Router,
    encoder: KeyEncoder,
}

impl Keyed {
    fn new(table: &Table, columns: &[String], key: &[String]) -> Result<Self, Error> {
        let schema = table.schema();
        let indices: Vec<usize> = columns
            .iter()
            .map(|name| {
                schema
                    .fields()
                    .index_of(name)
                    .ok_or_else(|| mink_record::Error::UnknownColumn(name.clone()))
            })
            .collect::<Result<_, _>>()?;
        let fields = schema
            .fields()
            .project(&indices)
            .map_err(|e| Error::Protocol(e.to_string()))?;
        let router = Router::new(&fields, table.descriptor())?;
        let encoder = KeyEncoder::new(&fields, key, table.descriptor().bucketing())?;

        Ok(Keyed {
            fields,
            router,
            encoder,
        })
    }

    fn check(&self, batch: &RecordBatch) -> Result<(), Error> {
        let expected: Vec<&str> = self.fields.names().collect();
        let schema = batch.schema();
        let found: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        if expected != found {
            return Err(Error::Protocol(format!(
                "key batch has columns {found:?}, expected {expected:?}"
            )));
        }

        Ok(())
    }
}

impl Lookup {
    pub(crate) fn new(table: Table) -> Result<Self, Error> {
        let descriptor = table.descriptor();
        let primary_key = table
            .schema()
            .primary_key()
            .ok_or_else(|| Error::NotPrimaryKey(table.path().clone()))?
            .columns()
            .to_vec();
        let in_schema_order = |names: &[String]| -> Vec<String> {
            table
                .schema()
                .fields()
                .names()
                .filter(|n| names.iter().any(|k| k == n))
                .map(str::to_owned)
                .collect()
        };
        let key = Keyed::new(&table, &in_schema_order(&primary_key), &primary_key)?;
        let prefix_columns: Vec<String> = descriptor
            .partition_keys()
            .iter()
            .chain(descriptor.bucket_keys())
            .cloned()
            .collect();
        let prefix = Keyed::new(
            &table,
            &in_schema_order(&prefix_columns),
            descriptor.bucket_keys(),
        )?;

        Ok(Lookup { table, key, prefix })
    }

    pub fn key_columns(&self) -> impl Iterator<Item = &str> {
        self.key.fields.names()
    }

    pub fn prefix_columns(&self) -> impl Iterator<Item = &str> {
        self.prefix.fields.names()
    }

    pub async fn lookup(&self, keys: &RecordBatch) -> Result<Vec<Option<RecordBatch>>, Error> {
        self.key.check(keys)?;
        let bound = self.key.encoder.bind(keys)?;
        let mut by_bucket: BTreeMap<Bucket, (Vec<usize>, Vec<Vec<u8>>)> = BTreeMap::new();
        for group in self.key.router.split(keys)? {
            let bucket = self
                .table
                .resolve(group.partition.as_ref(), group.bucket)
                .await?;
            let entry = by_bucket.entry(bucket).or_default();
            for row in group.rows {
                entry.0.push(row as usize);
                entry.1.push(bound.encode_vec(row as usize)?);
            }
        }

        let mut out: Vec<Option<RecordBatch>> = vec![None; keys.num_rows()];
        for (bucket, (positions, encoded)) in by_bucket {
            let request = proto::Lookup {
                bucket,
                keys: encoded,
            };
            let results: Vec<Bytes> = self
                .table
                .cluster()
                .with_leader(self.table.path(), bucket, |connection| {
                    let request = request.clone();
                    async move { connection.action_raw(action::LOOKUP, &request).await }
                })
                .await?;
            if results.len() != positions.len() {
                return Err(Error::Protocol(format!(
                    "lookup of {} keys returned {} results",
                    positions.len(),
                    results.len()
                )));
            }
            for (position, body) in positions.into_iter().zip(results) {
                out[position] = ipc_rows(&body)?;
            }
        }

        Ok(out)
    }

    pub async fn lookup_one(&self, key: &RecordBatch) -> Result<Option<RecordBatch>, Error> {
        if key.num_rows() != 1 {
            return Err(Error::Protocol(format!(
                "lookup_one takes one key row, got {}",
                key.num_rows()
            )));
        }

        Ok(self.lookup(key).await?.remove(0))
    }

    pub async fn prefix_lookup(
        &self,
        prefixes: &RecordBatch,
    ) -> Result<Vec<Option<RecordBatch>>, Error> {
        self.prefix.check(prefixes)?;
        let bound = self.prefix.encoder.bind(prefixes)?;
        let mut out: Vec<Option<RecordBatch>> = vec![None; prefixes.num_rows()];
        for group in self.prefix.router.split(prefixes)? {
            let bucket = self
                .table
                .resolve(group.partition.as_ref(), group.bucket)
                .await?;
            for row in group.rows {
                let request = proto::PrefixLookup {
                    bucket,
                    prefix: bound.encode_vec(row as usize)?,
                };
                let body: Bytes = self
                    .table
                    .cluster()
                    .with_leader(self.table.path(), bucket, |connection| {
                        let request = request.clone();
                        async move {
                            connection
                                .action_raw(action::PREFIX_LOOKUP, &request)
                                .await?
                                .into_iter()
                                .next()
                                .ok_or_else(|| {
                                    Error::Protocol("prefix lookup returned nothing".into())
                                })
                        }
                    })
                    .await?;
                out[row as usize] = ipc_rows(&body)?;
            }
        }

        Ok(out)
    }
}

fn ipc_rows(body: &[u8]) -> Result<Option<RecordBatch>, Error> {
    let reader = StreamReader::try_new(Cursor::new(body), None)?;
    let mut batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    let rows = match batches.len() {
        0 => None,
        1 => batches.pop(),
        _ => {
            let schema = batches[0].schema();
            Some(concat_batches(&schema, &batches)?)
        }
    };

    Ok(rows.filter(|b| b.num_rows() > 0))
}
