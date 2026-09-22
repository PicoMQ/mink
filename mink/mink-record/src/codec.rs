//! The log batch codec abstraction and the decoded records it yields.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use mink_table::LogFormat;

use crate::{ArrowCodec, ChangeType, Compression, Error, Header};

pub trait Codec: Send + Sync {
    fn encode(
        &self,
        append_only: bool,
        changes: &[ChangeType],
        batch: &RecordBatch,
        out: &mut Vec<u8>,
    ) -> Result<(), Error>;

    fn decode(
        &self,
        header: &Header,
        body: Bytes,
        schema: SchemaRef,
        projection: Option<&[usize]>,
    ) -> Result<Records, Error>;
}

pub fn codec(format: LogFormat, compression: Compression) -> Box<dyn Codec> {
    match format {
        LogFormat::Arrow => Box::new(ArrowCodec::new(compression)),
    }
}

#[derive(Debug, Clone)]
pub struct Records {
    pub changes: Changes,
    pub batch: RecordBatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Changes {
    AppendOnly(usize),
    Vector(Bytes),
}

impl Changes {
    pub fn vector(bytes: Bytes) -> Result<Self, Error> {
        for byte in &bytes {
            ChangeType::from_byte(*byte)?;
        }

        Ok(Changes::Vector(bytes))
    }

    pub fn len(&self) -> usize {
        match self {
            Changes::AppendOnly(count) => *count,
            Changes::Vector(bytes) => bytes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, row: usize) -> ChangeType {
        match self {
            Changes::AppendOnly(count) => {
                assert!(row < *count, "row {row} out of range for {count} rows");
                ChangeType::AppendOnly
            }
            Changes::Vector(bytes) => {
                ChangeType::from_byte(bytes[row]).expect("checked in Changes::vector")
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = ChangeType> + '_ {
        (0..self.len()).map(|row| self.get(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_is_validated_once() {
        let changes = Changes::vector(Bytes::from_static(&[1, 2, 3, 4])).unwrap();
        assert_eq!(changes.len(), 4);
        assert_eq!(
            changes.iter().collect::<Vec<_>>(),
            [
                ChangeType::Insert,
                ChangeType::UpdateBefore,
                ChangeType::UpdateAfter,
                ChangeType::Delete
            ]
        );
        assert_eq!(
            Changes::vector(Bytes::from_static(&[1, 9])).unwrap_err(),
            Error::ChangeType(9)
        );
    }

    #[test]
    fn append_only_is_implicit() {
        let changes = Changes::AppendOnly(2);
        assert_eq!(changes.get(1), ChangeType::AppendOnly);
        assert!(changes.iter().all(|c| c == ChangeType::AppendOnly));
        assert!(Changes::AppendOnly(0).is_empty());
    }
}
