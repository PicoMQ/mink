//! Extracts bucket key bytes from Arrow rows in the encoding of the chosen bucketing scheme.

use arrow_array::RecordBatch;
use mink_table::Bucketing;
use mink_types::{DataType, Fields};

use crate::{Error, Scalar, compacted, iceberg, paimon, scalar::Reader};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyColumn {
    pub index: usize,
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyEncoder {
    format: Bucketing,
    columns: Vec<KeyColumn>,
}

impl KeyEncoder {
    pub fn new(fields: &Fields, keys: &[String], format: Bucketing) -> Result<Self, Error> {
        if keys.is_empty() {
            return Err(Error::EmptyKey);
        }
        if format == Bucketing::Iceberg && keys.len() != 1 {
            return Err(Error::IcebergKeyArity(keys.len()));
        }

        let supports = match format {
            Bucketing::Native => compacted::supports,
            Bucketing::Paimon => paimon::supports,
            Bucketing::Iceberg => iceberg::supports,
        };
        let columns = resolve(fields, keys, supports, |column, data_type| Error::KeyType {
            format,
            column,
            data_type,
        })?;

        Ok(KeyEncoder { format, columns })
    }

    pub fn format(&self) -> Bucketing {
        self.format
    }

    pub fn columns(&self) -> &[KeyColumn] {
        &self.columns
    }

    pub fn bind<'a>(&'a self, batch: &'a RecordBatch) -> Result<Bound<'a>, Error> {
        Ok(Bound {
            encoder: self,
            readers: readers(&self.columns, batch)?,
            rows: batch.num_rows(),
        })
    }
}

pub(crate) fn resolve(
    fields: &Fields,
    keys: &[String],
    supports: fn(&DataType) -> bool,
    unsupported: impl Fn(String, DataType) -> Error,
) -> Result<Vec<KeyColumn>, Error> {
    keys.iter()
        .map(|name| {
            let index = fields
                .index_of(name)
                .ok_or_else(|| Error::UnknownColumn(name.clone()))?;
            let data_type = fields[index].data_type().clone();
            if !supports(&data_type) {
                return Err(unsupported(name.clone(), data_type));
            }
            Ok(KeyColumn {
                index,
                name: name.clone(),
                data_type,
            })
        })
        .collect()
}

pub(crate) fn readers<'a>(
    columns: &[KeyColumn],
    batch: &'a RecordBatch,
) -> Result<Vec<Reader<'a>>, Error> {
    columns
        .iter()
        .map(|column| {
            let array = batch
                .columns()
                .get(column.index)
                .ok_or(Error::ColumnIndex {
                    index: column.index,
                    found: batch.num_columns(),
                })?;
            Reader::new(array, &column.data_type, &column.name)
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct Bound<'a> {
    encoder: &'a KeyEncoder,
    readers: Vec<Reader<'a>>,
    rows: usize,
}

impl Bound<'_> {
    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn encode(&self, row: usize, out: &mut Vec<u8>) -> Result<(), Error> {
        if row >= self.rows {
            return Err(Error::RowIndex {
                row,
                rows: self.rows,
            });
        }
        out.clear();

        let columns = &self.encoder.columns;
        let value = |position: usize| -> Option<Scalar<'_>> { self.readers[position].get(row) };
        let write = match self.encoder.format {
            Bucketing::Native => compacted::write,
            Bucketing::Iceberg => iceberg::write,
            Bucketing::Paimon => {
                paimon::encode(out, columns, value);
                return Ok(());
            }
        };

        for (position, column) in columns.iter().enumerate() {
            let scalar = value(position).ok_or_else(|| Error::NullKey(column.name.clone()))?;
            write(out, scalar);
        }

        Ok(())
    }

    pub fn encode_vec(&self, row: usize) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        self.encode(row, &mut out)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
    use mink_types::Field;

    use super::*;

    fn fields() -> Fields {
        Fields::new(vec![
            Field::new("id", DataType::int()).unwrap(),
            Field::new("name", DataType::string()).unwrap(),
            Field::new("tags", DataType::array(DataType::string())).unwrap(),
            Field::new("flag", DataType::boolean()).unwrap(),
        ])
        .unwrap()
    }

    fn keys(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn construction_rules() {
        let fields = fields();
        assert_eq!(
            KeyEncoder::new(&fields, &[], Bucketing::Native).unwrap_err(),
            Error::EmptyKey
        );
        assert_eq!(
            KeyEncoder::new(&fields, &keys(&["nope"]), Bucketing::Native).unwrap_err(),
            Error::UnknownColumn("nope".into())
        );
        assert_eq!(
            KeyEncoder::new(&fields, &keys(&["id", "name"]), Bucketing::Iceberg).unwrap_err(),
            Error::IcebergKeyArity(2)
        );
        assert!(matches!(
            KeyEncoder::new(&fields, &keys(&["tags"]), Bucketing::Native).unwrap_err(),
            Error::KeyType { column, .. } if column == "tags"
        ));
        assert!(matches!(
            KeyEncoder::new(&fields, &keys(&["flag"]), Bucketing::Iceberg).unwrap_err(),
            Error::KeyType {
                format: Bucketing::Iceberg,
                ..
            }
        ));
        assert!(KeyEncoder::new(&fields, &keys(&["flag"]), Bucketing::Paimon).is_ok());

        let encoder = KeyEncoder::new(&fields, &keys(&["name", "id"]), Bucketing::Native).unwrap();
        assert_eq!(
            encoder
                .columns()
                .iter()
                .map(|c| c.index)
                .collect::<Vec<_>>(),
            [1, 0]
        );
    }

    #[test]
    fn bind_checks_the_batch_and_rows() {
        let fields = fields();
        let encoder = KeyEncoder::new(&fields, &keys(&["id", "name"]), Bucketing::Native).unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", arrow_schema::DataType::Int32, true),
            ArrowField::new("name", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(7), None])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let bound = encoder.bind(&batch).unwrap();
        assert_eq!(bound.rows(), 2);
        assert_eq!(bound.encode_vec(0).unwrap(), [0x07, 0x01, b'a']);
        assert_eq!(
            bound.encode_vec(1).unwrap_err(),
            Error::NullKey("id".into())
        );
        assert_eq!(
            bound.encode_vec(2).unwrap_err(),
            Error::RowIndex { row: 2, rows: 2 }
        );

        let narrow = batch.project(&[0]).unwrap();
        assert_eq!(
            encoder.bind(&narrow).unwrap_err(),
            Error::ColumnIndex { index: 1, found: 1 }
        );
    }
}
