//! Key byte encoding matching Iceberg bucket transform input.

use mink_common::bigint;
use mink_types::{DataType, Root};

use crate::Scalar;

pub(crate) fn supports(data_type: &DataType) -> bool {
    matches!(
        data_type.root(),
        Root::TinyInt
            | Root::SmallInt
            | Root::Int
            | Root::BigInt
            | Root::Float
            | Root::Double
            | Root::Char
            | Root::String
            | Root::Binary
            | Root::Bytes
            | Root::Decimal
            | Root::Date
            | Root::Time
            | Root::Timestamp
            | Root::TimestampLtz
    )
}

pub(crate) fn write(out: &mut Vec<u8>, scalar: Scalar<'_>) {
    match scalar {
        Scalar::Boolean(v) => out.push(u8::from(v)),
        Scalar::TinyInt(v) => out.extend_from_slice(&i64::from(v).to_le_bytes()),
        Scalar::SmallInt(v) => out.extend_from_slice(&i64::from(v).to_le_bytes()),
        Scalar::Int(v) | Scalar::Date(v) => out.extend_from_slice(&i64::from(v).to_le_bytes()),
        Scalar::BigInt(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::Float(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::Double(v) => out.extend_from_slice(&v.to_le_bytes()),
        Scalar::String(s) => out.extend_from_slice(s.as_bytes()),
        Scalar::Bytes(b) => out.extend_from_slice(b),
        Scalar::Decimal { unscaled: v, .. } => {
            out.extend_from_slice(bigint::to_bytes(v, &mut [0; 16]));
        }
        Scalar::Time(nanos) => out.extend_from_slice(&(nanos / 1_000).to_le_bytes()),
        Scalar::Timestamp { at, .. } => out.extend_from_slice(&at.micros().to_le_bytes()),
    }
}
