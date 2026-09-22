//! The stored value layout, a schema id followed by the encoded row, and decoding stored values to Arrow.

use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::{BufMut, Bytes, BytesMut};
use mink_record::{Remap, RowCodec, Rows, row_codec};
use mink_table::{KvFormat, Schema, SchemaId};
use mink_types::Fields;

use crate::Error;

const SCHEMA_ID_LEN: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    pub schema_id: SchemaId,
    pub row: Bytes,
}

impl Value {
    pub fn new(schema_id: SchemaId, row: impl Into<Bytes>) -> Self {
        Value {
            schema_id,
            row: row.into(),
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(SCHEMA_ID_LEN + self.row.len());
        out.put_i16_le(self.schema_id.0 as i16);
        out.extend_from_slice(&self.row);
        out.freeze()
    }

    pub fn decode(mut bytes: Bytes) -> Result<Self, Error> {
        if bytes.len() < SCHEMA_ID_LEN {
            return Err(Error::ValueTooShort(bytes.len()));
        }

        let row = bytes.split_off(SCHEMA_ID_LEN);
        let schema_id = i16::from_le_bytes([bytes[0], bytes[1]]);

        Ok(Value {
            schema_id: SchemaId(schema_id as u32),
            row,
        })
    }
}

pub fn values_to_arrow<I>(
    values: I,
    latest: &Fields,
    format: KvFormat,
    mut schema: impl FnMut(SchemaId) -> Option<Arc<Schema>>,
) -> Result<RecordBatch, Error>
where
    I: IntoIterator<Item = Bytes>,
{
    let values = values.into_iter();
    let mut rows = Rows::new(latest, values.size_hint().0)?;
    let mut codecs: Vec<(SchemaId, Box<dyn RowCodec>, Remap)> = Vec::new();

    for bytes in values {
        let value = Value::decode(bytes)?;
        let (_, codec, remap) = match codecs.iter().position(|(id, ..)| *id == value.schema_id) {
            Some(i) => &codecs[i],
            None => {
                let schema =
                    schema(value.schema_id).ok_or(Error::SchemaNotExist(value.schema_id))?;
                codecs.push((
                    value.schema_id,
                    row_codec(format, schema.fields())?,
                    Remap::new(schema.fields(), latest),
                ));
                codecs.last().expect("just pushed")
            }
        };
        rows.push(&remap.row(&codec.decode(&value.row)?))?;
    }

    Ok(rows.finish()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_java_value_encoder() {
        let value = Value::new(SchemaId(7), vec![0xff, 0xff, 0x07]);
        assert_eq!(value.encode().as_ref(), [0x07, 0x00, 0xff, 0xff, 0x07]);
        assert_eq!(Value::decode(value.encode()).unwrap(), value);
    }

    #[test]
    fn rejects_short_input() {
        assert!(matches!(
            Value::decode(Bytes::from_static(&[1])),
            Err(Error::ValueTooShort(1))
        ));
        let empty = Value::decode(Bytes::from_static(&[3, 0])).unwrap();
        assert_eq!(empty.schema_id, SchemaId(3));
        assert!(empty.row.is_empty());
    }
}
