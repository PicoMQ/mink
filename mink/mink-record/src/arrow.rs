//! Log batch codec whose body is one Arrow IPC record batch preceded by the change type vector.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::MetadataVersion;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message};
use arrow_schema::SchemaRef;
use bytes::Bytes;

use crate::codec::{Changes, Codec, Records};
use crate::{ChangeType, Compression, Error, Header, message};

const ALIGNMENT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArrowCodec {
    compression: Compression,
}

impl ArrowCodec {
    pub fn new(compression: Compression) -> Self {
        ArrowCodec { compression }
    }

    fn options(&self) -> Result<IpcWriteOptions, Error> {
        IpcWriteOptions::try_new(ALIGNMENT, false, MetadataVersion::V5)
            .and_then(|options| options.try_with_compression(self.compression.ipc()))
            .map_err(|e| Error::Ipc(e.to_string()))
    }
}

impl Codec for ArrowCodec {
    fn encode(
        &self,
        append_only: bool,
        changes: &[ChangeType],
        batch: &RecordBatch,
        out: &mut Vec<u8>,
    ) -> Result<(), Error> {
        if changes.len() != batch.num_rows() {
            return Err(Error::ChangeCount {
                changes: changes.len(),
                rows: batch.num_rows(),
            });
        }
        if append_only {
            if let Some(change) = changes.iter().find(|c| **c != ChangeType::AppendOnly) {
                return Err(Error::NotAppendOnly(*change));
            }
        } else {
            out.extend(changes.iter().map(|c| c.byte()));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let options = self.options()?;
        let mut tracker = DictionaryTracker::new(false);
        let (dictionaries, encoded) = IpcDataGenerator::default()
            .encode(batch, &mut tracker, &options, &mut Default::default())
            .map_err(|e| Error::Ipc(e.to_string()))?;
        if !dictionaries.is_empty() {
            return Err(Error::Ipc("dictionary columns are not supported".into()));
        }

        write_message(&mut *out, encoded, &options).map_err(|e| Error::Ipc(e.to_string()))?;

        Ok(())
    }

    fn decode(
        &self,
        header: &Header,
        body: Bytes,
        schema: SchemaRef,
        projection: Option<&[usize]>,
    ) -> Result<Records, Error> {
        let count = header.record_count as usize;
        let (changes, arrow) = if header.append_only {
            (Changes::AppendOnly(count), body)
        } else {
            if body.len() < count {
                return Err(Error::Truncated {
                    needed: count,
                    found: body.len(),
                });
            }
            (Changes::vector(body.slice(..count))?, body.slice(count..))
        };

        if count == 0 {
            let schema = match projection {
                Some(columns) => Arc::new(
                    schema
                        .project(columns)
                        .map_err(|e| Error::Ipc(e.to_string()))?,
                ),
                None => schema,
            };
            return Ok(Records {
                changes,
                batch: RecordBatch::new_empty(schema),
            });
        }

        let framed = message::split(&arrow)?;
        let record_batch = framed
            .message
            .header_as_record_batch()
            .ok_or_else(|| Error::Ipc("message is not a record batch".into()))?;
        let buffer = Buffer::from(arrow.slice(framed.body_offset..));
        let batch = arrow_ipc::reader::read_record_batch(
            &buffer,
            record_batch,
            schema,
            &HashMap::new(),
            projection,
            &framed.message.version(),
        )
        .map_err(|e| Error::Ipc(e.to_string()))?;

        if batch.num_rows() != count {
            return Err(Error::RowCount {
                declared: count,
                found: batch.num_rows(),
            });
        }

        Ok(Records { changes, batch })
    }
}
