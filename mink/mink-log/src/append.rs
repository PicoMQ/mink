//! The outcome of an append and the splitting of an incoming byte run into validated batches.

use bytes::{Buf, Bytes};
use mink_record::Batch;

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendInfo {
    pub first_offset: i64,
    pub last_offset: i64,
    pub max_timestamp: i64,
    pub batch_count: usize,
    pub duplicated: bool,
}

impl AppendInfo {
    pub fn empty() -> Self {
        AppendInfo {
            first_offset: -1,
            last_offset: -1,
            max_timestamp: -1,
            batch_count: 0,
            duplicated: false,
        }
    }

    pub fn next_offset(&self) -> i64 {
        self.last_offset + 1
    }
}

pub(crate) fn analyze(mut bytes: Bytes) -> Result<Vec<Batch>, Error> {
    let mut batches = Vec::new();
    while !bytes.is_empty() {
        let batch = Batch::parse(bytes.clone())?;
        batch.ensure_valid()?;
        if batch.header().record_count == 0 {
            return Err(Error::EmptyBatch);
        }
        bytes.advance(batch.size());
        batches.push(batch);
    }

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use mink_record::{ChangeType, Compression, Spec, build, codec};
    use mink_table::{LogFormat, SchemaId};

    use super::*;

    fn batch(rows: i32) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let values = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from((0..rows).collect::<Vec<_>>()))],
        )
        .unwrap();
        let codec = codec(LogFormat::Arrow, Compression::None);
        build(
            Spec::new(SchemaId(1), true),
            &vec![ChangeType::AppendOnly; rows as usize],
            &values,
            codec.as_ref(),
        )
        .unwrap()
    }

    #[test]
    fn splits_concatenated_batches() {
        let mut bytes = batch(2);
        bytes.extend(batch(3));
        let batches = analyze(Bytes::from(bytes)).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].header().record_count, 2);
        assert_eq!(batches[1].header().record_count, 3);
    }

    #[test]
    fn rejects_corruption_trailing_bytes_and_empty_batches() {
        let mut corrupt = batch(2);
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(matches!(
            analyze(Bytes::from(corrupt)).unwrap_err(),
            Error::Corrupt(mink_record::Error::Crc { .. })
        ));

        let mut trailing = batch(2);
        trailing.extend_from_slice(&[0; 7]);
        assert!(matches!(
            analyze(Bytes::from(trailing)).unwrap_err(),
            Error::Corrupt(mink_record::Error::Truncated { .. })
        ));

        assert!(matches!(
            analyze(Bytes::from(batch(0))).unwrap_err(),
            Error::EmptyBatch
        ));
        assert!(analyze(Bytes::new()).unwrap().is_empty());
    }
}
