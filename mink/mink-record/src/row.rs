//! Row codec abstraction and the compacted row implementation with a leading null bitmap.

use mink_table::KvFormat;
use mink_types::{DataType, Fields};

use crate::{Error, Scalar, compacted};

pub type Row<'a> = Vec<Option<Scalar<'a>>>;

pub trait RowCodec: Send + Sync {
    fn field_count(&self) -> usize;

    fn encode(&self, row: &[Option<Scalar<'_>>], out: &mut Vec<u8>) -> Result<(), Error>;

    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<Row<'a>, Error>;
}

pub fn row_codec(format: KvFormat, fields: &Fields) -> Result<Box<dyn RowCodec>, Error> {
    match format {
        KvFormat::Compacted => Ok(Box::new(CompactedRow::new(fields)?)),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedRow {
    types: Vec<DataType>,
}

impl CompactedRow {
    pub fn new(fields: &Fields) -> Result<Self, Error> {
        let types: Vec<DataType> = fields.iter().map(|f| f.data_type().clone()).collect();
        if let Some(unsupported) = types.iter().find(|t| !compacted::supports(t)) {
            return Err(Error::RowType(unsupported.clone()));
        }

        Ok(CompactedRow { types })
    }

    fn header_len(&self) -> usize {
        self.types.len().div_ceil(8)
    }
}

impl RowCodec for CompactedRow {
    fn field_count(&self) -> usize {
        self.types.len()
    }

    fn encode(&self, row: &[Option<Scalar<'_>>], out: &mut Vec<u8>) -> Result<(), Error> {
        if row.len() != self.types.len() {
            return Err(Error::FieldCount {
                expected: self.types.len(),
                found: row.len(),
            });
        }

        let header = out.len();
        out.resize(header + self.header_len(), 0);

        for (index, (value, data_type)) in row.iter().zip(&self.types).enumerate() {
            match value {
                Some(scalar) => compacted::write(out, *scalar),
                None if data_type.is_nullable() => out[header + index / 8] |= 1 << (index % 8),
                None => return Err(Error::NullField { index }),
            }
        }

        Ok(())
    }

    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<Row<'a>, Error> {
        let (header, mut input) =
            bytes
                .split_at_checked(self.header_len())
                .ok_or(Error::Truncated {
                    needed: self.header_len(),
                    found: bytes.len(),
                })?;

        let mut row = Vec::with_capacity(self.types.len());
        for (index, data_type) in self.types.iter().enumerate() {
            if header[index / 8] & (1 << (index % 8)) != 0 {
                row.push(None);
            } else {
                row.push(Some(compacted::read(&mut input, data_type)?));
            }
        }

        if !input.is_empty() {
            return Err(Error::TrailingBytes(input.len()));
        }

        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use mink_types::{Decimal, Field, Precision};

    use super::*;
    use crate::Timestamp;

    fn fields() -> Fields {
        Fields::new(vec![
            Field::new("id", DataType::big_int().with_nullable(false)).unwrap(),
            Field::new("name", DataType::string()).unwrap(),
            Field::new("score", DataType::double()).unwrap(),
            Field::new("flag", DataType::boolean()).unwrap(),
            Field::new("small", DataType::small_int()).unwrap(),
            Field::new("tiny", DataType::tiny_int()).unwrap(),
            Field::new("day", DataType::date()).unwrap(),
            Field::new("amount", DataType::decimal(Decimal::new(10, 2).unwrap())).unwrap(),
            Field::new("big", DataType::decimal(Decimal::new(30, 5).unwrap())).unwrap(),
            Field::new("ts", DataType::timestamp(Precision::new(3).unwrap())).unwrap(),
            Field::new("ts6", DataType::timestamp_ltz(Precision::new(6).unwrap())).unwrap(),
            Field::new("blob", DataType::bytes()).unwrap(),
        ])
        .unwrap()
    }

    #[test]
    fn round_trips_every_scalar() {
        let codec = CompactedRow::new(&fields()).unwrap();
        let row: Row<'_> = vec![
            Some(Scalar::BigInt(-42)),
            Some(Scalar::String("héllo")),
            Some(Scalar::Double(2.5)),
            Some(Scalar::Boolean(true)),
            Some(Scalar::SmallInt(-3)),
            Some(Scalar::TinyInt(7)),
            Some(Scalar::Date(19_000)),
            Some(Scalar::Decimal {
                unscaled: 12_345,
                precision: 10,
            }),
            Some(Scalar::Decimal {
                unscaled: -1_234_567_890_123_456_789_012_345,
                precision: 30,
            }),
            Some(Scalar::Timestamp {
                at: Timestamp {
                    millis: 1_700_000_000_000,
                    nanos: 0,
                },
                precision: 3,
            }),
            Some(Scalar::Timestamp {
                at: Timestamp {
                    millis: 1_700_000_000_000,
                    nanos: 123_000,
                },
                precision: 6,
            }),
            Some(Scalar::Bytes(b"\x00\x01\x02")),
        ];
        let mut bytes = Vec::new();
        codec.encode(&row, &mut bytes).unwrap();
        assert_eq!(bytes[0..2], [0, 0]);
        assert_eq!(codec.decode(&bytes).unwrap(), row);
    }

    #[test]
    fn nulls_take_a_bit_and_no_bytes() {
        let codec = CompactedRow::new(&fields()).unwrap();
        let mut row: Row<'_> = vec![None; 12];
        row[0] = Some(Scalar::BigInt(1));
        let mut bytes = Vec::new();
        codec.encode(&row, &mut bytes).unwrap();
        assert_eq!(bytes, [0b1111_1110, 0b0000_1111, 0x01]);
        assert_eq!(codec.decode(&bytes).unwrap(), row);
    }

    #[test]
    fn rejects_null_in_non_nullable_field() {
        let codec = CompactedRow::new(&fields()).unwrap();
        let row: Row<'_> = vec![None; 12];
        assert_eq!(
            codec.encode(&row, &mut Vec::new()),
            Err(Error::NullField { index: 0 })
        );
    }

    #[test]
    fn rejects_arity_mismatch_and_slack() {
        let codec = CompactedRow::new(&fields()).unwrap();
        assert_eq!(
            codec.encode(&[Some(Scalar::BigInt(1))], &mut Vec::new()),
            Err(Error::FieldCount {
                expected: 12,
                found: 1
            })
        );
        let mut row: Row<'_> = vec![None; 12];
        row[0] = Some(Scalar::BigInt(1));
        let mut bytes = Vec::new();
        codec.encode(&row, &mut bytes).unwrap();
        bytes.push(0);
        assert_eq!(codec.decode(&bytes), Err(Error::TrailingBytes(1)));
        assert!(matches!(
            codec.decode(&bytes[..1]),
            Err(Error::Truncated { .. })
        ));
    }

    #[test]
    fn rejects_nested_types() {
        let nested = Fields::new(vec![
            Field::new("xs", DataType::array(DataType::int())).unwrap(),
        ])
        .unwrap();
        assert!(matches!(CompactedRow::new(&nested), Err(Error::RowType(_))));
    }
}
