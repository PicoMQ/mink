//! Key byte encoding matching the Paimon binary row layout, including its null bitmap and variable-length slots.

use mink_common::bigint;
use mink_types::{DataType, Family};

use crate::scalar::{COMPACT_DECIMAL_PRECISION, COMPACT_TIMESTAMP_PRECISION};
use crate::{KeyColumn, Scalar};

const HEADER_BITS: usize = 8;
const INLINE_LIMIT: usize = 7;
const ROW_KIND_INSERT: u8 = 0;

pub(crate) fn supports(data_type: &DataType) -> bool {
    data_type.is(Family::Predefined)
}

pub(crate) fn encode<'a>(
    out: &mut Vec<u8>,
    columns: &[KeyColumn],
    value: impl Fn(usize) -> Option<Scalar<'a>>,
) {
    let arity = columns.len();
    let null_bytes = (arity + 63 + HEADER_BITS) / 64 * 8;
    out.resize(null_bytes + 8 * arity, 0);
    out[0] = ROW_KIND_INSERT;

    for position in 0..arity {
        let slot = null_bytes + 8 * position;
        match value(position) {
            Some(scalar) => write(out, slot, scalar),
            None => set_null(out, position),
        }
    }
}

fn set_null(out: &mut [u8], position: usize) {
    let bit = position + HEADER_BITS;
    out[bit / 8] |= 1 << (bit % 8);
}

fn write(out: &mut Vec<u8>, slot: usize, scalar: Scalar<'_>) {
    match scalar {
        Scalar::Boolean(v) => out[slot] = u8::from(v),
        Scalar::TinyInt(v) => out[slot] = v as u8,
        Scalar::SmallInt(v) => put(out, slot, &v.to_le_bytes()),
        Scalar::Int(v) | Scalar::Date(v) => put(out, slot, &v.to_le_bytes()),
        Scalar::BigInt(v) => put(out, slot, &v.to_le_bytes()),
        Scalar::Float(v) => put(out, slot, &v.to_le_bytes()),
        Scalar::Double(v) => put(out, slot, &v.to_le_bytes()),
        Scalar::String(s) => bytes(out, slot, s.as_bytes()),
        Scalar::Bytes(b) => bytes(out, slot, b),
        Scalar::Decimal {
            unscaled: v,
            precision,
        } => {
            if precision <= COMPACT_DECIMAL_PRECISION {
                put(out, slot, &(v as i64).to_le_bytes());
            } else {
                let cursor = out.len();
                let mut buf = [0; 16];
                let digits = bigint::to_bytes(v, &mut buf);
                out.extend_from_slice(digits);
                out.resize(cursor + 16, 0);
                offset_and_size(out, slot, cursor, digits.len() as u32);
            }
        }
        Scalar::Time(nanos) => put(out, slot, &((nanos / 1_000_000) as i32).to_le_bytes()),
        Scalar::Timestamp { at, precision } => {
            if precision <= COMPACT_TIMESTAMP_PRECISION {
                put(out, slot, &at.millis.to_le_bytes());
            } else {
                let cursor = out.len();
                out.extend_from_slice(&at.millis.to_le_bytes());
                offset_and_size(out, slot, cursor, at.nanos);
            }
        }
    }
}

fn bytes(out: &mut Vec<u8>, slot: usize, bytes: &[u8]) {
    if bytes.len() <= INLINE_LIMIT {
        out[slot..slot + bytes.len()].copy_from_slice(bytes);
        out[slot + 7] = bytes.len() as u8 | 0x80;
    } else {
        let cursor = out.len();
        out.extend_from_slice(bytes);
        out.resize(cursor + bytes.len().next_multiple_of(8), 0);
        offset_and_size(out, slot, cursor, bytes.len() as u32);
    }
}

fn offset_and_size(out: &mut [u8], slot: usize, offset: usize, size: u32) {
    let packed = ((offset as u64) << 32) | u64::from(size);
    put(out, slot, &packed.to_le_bytes());
}

fn put(out: &mut [u8], slot: usize, bytes: &[u8]) {
    out[slot..slot + bytes.len()].copy_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use mink_types::DataType;

    use super::*;

    fn column(name: &str, data_type: DataType) -> KeyColumn {
        KeyColumn {
            index: 0,
            name: name.into(),
            data_type,
        }
    }

    #[test]
    fn null_sets_the_bit_after_the_row_kind_byte() {
        let columns = [column("a", DataType::int()), column("b", DataType::int())];
        let mut out = Vec::new();
        encode(&mut out, &columns, |position| {
            (position == 0).then_some(Scalar::Int(-1))
        });
        assert_eq!(out.len(), 8 + 16);
        assert_eq!(out[0], 0);
        assert_eq!(out[1], 0b10);
        assert_eq!(&out[8..12], &[0xff; 4]);
        assert_eq!(&out[16..24], &[0; 8]);
    }

    #[test]
    fn header_grows_at_57_fields() {
        let columns: Vec<_> = (0..57)
            .map(|i| column(&i.to_string(), DataType::int()))
            .collect();
        let mut out = Vec::new();
        encode(&mut out, &columns, |_| Some(Scalar::Int(0)));
        assert_eq!(out.len(), 16 + 8 * 57);
    }
}
