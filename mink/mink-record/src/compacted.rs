//! Scalar encoding and decoding of the compacted row format: varints for integers, length-prefixed bytes.

use std::str;

use mink_common::{bigint, varint};
use mink_types::{DataType, Family, Kind};

use crate::scalar::{COMPACT_DECIMAL_PRECISION, COMPACT_TIMESTAMP_PRECISION};
use crate::{Error, Scalar, Timestamp};

pub(crate) fn supports(data_type: &DataType) -> bool {
    data_type.is(Family::Predefined)
}

pub(crate) fn write(out: &mut Vec<u8>, scalar: Scalar<'_>) {
    match scalar {
        Scalar::Boolean(v) => out.push(u8::from(v)),
        Scalar::TinyInt(v) => out.push(v as u8),
        Scalar::SmallInt(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::Int(v) | Scalar::Date(v) => varint::put_i32(out, v),
        Scalar::BigInt(v) => varint::put_i64(out, v),
        Scalar::Float(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::Double(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::String(s) => bytes(out, s.as_bytes()),
        Scalar::Bytes(b) => bytes(out, b),
        Scalar::Decimal {
            unscaled: v,
            precision,
        } => {
            if precision <= COMPACT_DECIMAL_PRECISION {
                varint::put_i64(out, v as i64);
            } else {
                bytes(out, bigint::to_bytes(v, &mut [0; 16]));
            }
        }
        Scalar::Time(nanos) => varint::put_i32(out, (nanos / 1_000_000) as i32),
        Scalar::Timestamp { at, precision } => {
            varint::put_i64(out, at.millis);
            if precision > COMPACT_TIMESTAMP_PRECISION {
                varint::put_i32(out, at.nanos as i32);
            }
        }
    }
}

fn bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    varint::put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

pub(crate) fn read<'a>(input: &mut &'a [u8], data_type: &DataType) -> Result<Scalar<'a>, Error> {
    let scalar = match data_type.kind() {
        Kind::Boolean => Scalar::Boolean(take::<1>(input)?[0] != 0),
        Kind::TinyInt => Scalar::TinyInt(take::<1>(input)?[0] as i8),
        Kind::SmallInt => Scalar::SmallInt(i16::from_le_bytes(take(input)?)),
        Kind::Int => Scalar::Int(int(input)?),
        Kind::Date => Scalar::Date(int(input)?),
        Kind::BigInt => Scalar::BigInt(long(input)?),
        Kind::Float => Scalar::Float(f32::from_le_bytes(take(input)?)),
        Kind::Double => Scalar::Double(f64::from_le_bytes(take(input)?)),
        Kind::Char(_) | Kind::String => {
            let raw = slice(input)?;
            Scalar::String(str::from_utf8(raw).map_err(|_| Error::Utf8)?)
        }
        Kind::Binary(_) | Kind::Bytes => Scalar::Bytes(slice(input)?),
        Kind::Decimal(decimal) => {
            let precision = decimal.precision();
            let unscaled = if precision <= COMPACT_DECIMAL_PRECISION {
                i128::from(long(input)?)
            } else {
                let raw = slice(input)?;
                bigint::from_bytes(raw).ok_or(Error::Unscaled(raw.len()))?
            };
            Scalar::Decimal {
                unscaled,
                precision,
            }
        }
        Kind::Time(_) => Scalar::Time(i64::from(int(input)?) * 1_000_000),
        Kind::Timestamp(p) | Kind::TimestampLtz(p) => {
            let precision = p.get();
            let millis = long(input)?;
            let nanos = if precision > COMPACT_TIMESTAMP_PRECISION {
                u32::try_from(int(input)?).map_err(|_| Error::Nanos)?
            } else {
                0
            };
            Scalar::Timestamp {
                at: Timestamp { millis, nanos },
                precision,
            }
        }
        Kind::Array(_) | Kind::Map { .. } | Kind::Row(_) => {
            return Err(Error::RowType(data_type.clone()));
        }
    };

    Ok(scalar)
}

fn take<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], Error> {
    let (head, rest) = input.split_at_checked(N).ok_or(Error::Truncated {
        needed: N,
        found: input.len(),
    })?;
    *input = rest;
    Ok(head.try_into().expect("split at N"))
}

fn slice<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let len = varint::get_u32(input).ok_or(Error::Varint)? as usize;
    let (head, rest) = input.split_at_checked(len).ok_or(Error::Truncated {
        needed: len,
        found: input.len(),
    })?;
    *input = rest;
    Ok(head)
}

fn int(input: &mut &[u8]) -> Result<i32, Error> {
    varint::get_i32(input).ok_or(Error::Varint)
}

fn long(input: &mut &[u8]) -> Result<i64, Error> {
    varint::get_i64(input).ok_or(Error::Varint)
}
