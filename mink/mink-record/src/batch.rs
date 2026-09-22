//! A parsed log batch: header plus bytes, with checksum verification, decoding, and building from Arrow rows.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use mink_table::SchemaId;

use crate::codec::{Codec, Records};
use crate::header::{self, HEADER_SIZE, Header, NO_BATCH_SEQUENCE, NO_LEADER_EPOCH, NO_WRITER_ID};
use crate::{ChangeType, Error};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    header: Header,
    bytes: Bytes,
}

impl Batch {
    pub fn parse(bytes: Bytes) -> Result<Self, Error> {
        let header = Header::read(&bytes)?;
        if bytes.len() < header.size {
            return Err(Error::Truncated {
                needed: header.size,
                found: bytes.len(),
            });
        }

        Ok(Batch {
            header,
            bytes: bytes.slice(..header.size),
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn size(&self) -> usize {
        self.header.size
    }

    pub fn body(&self) -> Bytes {
        self.bytes.slice(HEADER_SIZE..)
    }

    pub fn is_valid(&self) -> bool {
        header::checksum(&self.bytes) == self.header.crc
    }

    pub fn ensure_valid(&self) -> Result<(), Error> {
        let computed = header::checksum(&self.bytes);
        if computed != self.header.crc {
            return Err(Error::Crc {
                stored: self.header.crc,
                computed,
            });
        }

        Ok(())
    }

    pub fn records(
        &self,
        codec: &dyn Codec,
        schema: SchemaRef,
        projection: Option<&[usize]>,
    ) -> Result<Records, Error> {
        codec.decode(&self.header, self.body(), schema, projection)
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Spec {
    pub base_offset: i64,
    pub schema_id: SchemaId,
    pub append_only: bool,
    pub writer_id: i64,
    pub batch_sequence: i32,
}

impl Spec {
    pub fn new(schema_id: SchemaId, append_only: bool) -> Self {
        Spec {
            base_offset: 0,
            schema_id,
            append_only,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
        }
    }

    pub fn with_base_offset(mut self, base_offset: i64) -> Self {
        self.base_offset = base_offset;
        self
    }

    pub fn with_writer(mut self, writer_id: i64, batch_sequence: i32) -> Self {
        self.writer_id = writer_id;
        self.batch_sequence = batch_sequence;
        self
    }
}

pub fn build(
    spec: Spec,
    changes: &[ChangeType],
    batch: &RecordBatch,
    codec: &dyn Codec,
) -> Result<Vec<u8>, Error> {
    let record_count =
        i32::try_from(batch.num_rows()).map_err(|_| Error::BatchTooLarge(batch.num_rows()))?;
    let mut out = vec![0; HEADER_SIZE];
    codec.encode(spec.append_only, changes, batch, &mut out)?;

    let header = Header {
        base_offset: spec.base_offset,
        size: out.len(),
        commit_timestamp: 0,
        leader_epoch: NO_LEADER_EPOCH,
        crc: 0,
        schema_id: spec.schema_id,
        append_only: spec.append_only,
        last_offset_delta: (record_count - 1).max(0),
        writer_id: spec.writer_id,
        batch_sequence: spec.batch_sequence,
        record_count,
    };
    header.write(&mut out)?;
    header::seal(&mut out);

    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use mink_table::LogFormat;

    use super::*;
    use crate::codec::{Changes, codec};
    use crate::{Compression, header::MAGIC};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn rows() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn round_trips_every_compression() {
        for compression in [Compression::None, Compression::Lz4Frame, Compression::Zstd] {
            let codec = codec(LogFormat::Arrow, compression);
            let changes = [
                ChangeType::Insert,
                ChangeType::UpdateAfter,
                ChangeType::Delete,
            ];
            let spec = Spec::new(SchemaId(3), false)
                .with_base_offset(10)
                .with_writer(5, 2);
            let bytes = build(spec, &changes, &rows(), codec.as_ref()).unwrap();
            let batch = Batch::parse(Bytes::from(bytes)).unwrap();
            batch.ensure_valid().unwrap();
            let header = batch.header();
            assert_eq!(header.base_offset, 10);
            assert_eq!(header.last_offset(), 12);
            assert_eq!(header.record_count, 3);
            assert_eq!((header.writer_id, header.batch_sequence), (5, 2));
            assert_eq!(header.leader_epoch, NO_LEADER_EPOCH);
            assert!(!header.append_only);

            let records = batch.records(codec.as_ref(), schema(), None).unwrap();
            assert_eq!(records.batch, rows());
            assert_eq!(records.changes.iter().collect::<Vec<_>>(), changes);
        }
    }

    #[test]
    fn append_only_omits_the_vector_and_rejects_other_changes() {
        let codec = codec(LogFormat::Arrow, Compression::None);
        let appends = [ChangeType::AppendOnly; 3];
        let with_vector = build(
            Spec::new(SchemaId(1), false),
            &appends,
            &rows(),
            codec.as_ref(),
        )
        .unwrap();
        let without = build(
            Spec::new(SchemaId(1), true),
            &appends,
            &rows(),
            codec.as_ref(),
        )
        .unwrap();
        assert_eq!(with_vector.len(), without.len() + 3);

        let batch = Batch::parse(Bytes::from(without)).unwrap();
        assert!(batch.header().append_only);
        let records = batch.records(codec.as_ref(), schema(), None).unwrap();
        assert_eq!(records.changes, Changes::AppendOnly(3));

        let err = build(
            Spec::new(SchemaId(1), true),
            &[
                ChangeType::AppendOnly,
                ChangeType::Insert,
                ChangeType::AppendOnly,
            ],
            &rows(),
            codec.as_ref(),
        )
        .unwrap_err();
        assert_eq!(err, Error::NotAppendOnly(ChangeType::Insert));
    }

    #[test]
    fn empty_batch_is_header_only() {
        let codec = codec(LogFormat::Arrow, Compression::Zstd);
        let empty = RecordBatch::new_empty(schema());
        let bytes = build(Spec::new(SchemaId(1), true), &[], &empty, codec.as_ref()).unwrap();
        assert_eq!(bytes.len(), HEADER_SIZE);
        let batch = Batch::parse(Bytes::from(bytes)).unwrap();
        batch.ensure_valid().unwrap();
        assert_eq!(batch.header().last_offset_delta, 0);
        let records = batch.records(codec.as_ref(), schema(), Some(&[1])).unwrap();
        assert_eq!(records.batch.num_rows(), 0);
        assert_eq!(records.batch.num_columns(), 1);
    }

    #[test]
    fn projection_at_decode_time() {
        let codec = codec(LogFormat::Arrow, Compression::Zstd);
        let bytes = build(
            Spec::new(SchemaId(1), true),
            &[ChangeType::AppendOnly; 3],
            &rows(),
            codec.as_ref(),
        )
        .unwrap();
        let batch = Batch::parse(Bytes::from(bytes)).unwrap();
        let records = batch.records(codec.as_ref(), schema(), Some(&[1])).unwrap();
        assert_eq!(records.batch, rows().project(&[1]).unwrap());
    }

    #[test]
    fn corruption_is_detected() {
        let codec = codec(LogFormat::Arrow, Compression::None);
        let mut bytes = build(
            Spec::new(SchemaId(1), true),
            &[ChangeType::AppendOnly; 3],
            &rows(),
            codec.as_ref(),
        )
        .unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let batch = Batch::parse(Bytes::from(bytes.clone())).unwrap();
        assert!(!batch.is_valid());
        assert!(matches!(
            batch.ensure_valid().unwrap_err(),
            Error::Crc { .. }
        ));

        assert!(matches!(
            Batch::parse(Bytes::from(bytes[..bytes.len() - 5].to_vec())).unwrap_err(),
            Error::Truncated { .. }
        ));
        assert_eq!(bytes[12], MAGIC);
    }

    #[test]
    fn change_count_must_match_rows() {
        let codec = codec(LogFormat::Arrow, Compression::None);
        let err = build(
            Spec::new(SchemaId(1), false),
            &[ChangeType::Insert],
            &rows(),
            codec.as_ref(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            Error::ChangeCount {
                changes: 1,
                rows: 3
            }
        );
    }
}
