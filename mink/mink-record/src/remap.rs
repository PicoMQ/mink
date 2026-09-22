//! Moving rows and batches written under one schema version into the layout of another, matching
//! columns by field id so that renamed, dropped, added and promoted columns all land where they belong.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, new_null_array};
use arrow_cast::cast;
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use mink_types::{DataType, Field, Fields, Kind};

use crate::{Error, Row, Scalar};

#[derive(Debug, Clone)]
pub struct Remap {
    sources: Vec<Option<usize>>,
    casts: Vec<Option<DataType>>,
    target: Fields,
    arrow: SchemaRef,
    source_len: usize,
    identity: bool,
}

impl Remap {
    pub fn new(from: &Fields, to: &Fields) -> Self {
        let mut sources = Vec::with_capacity(to.len());
        let mut casts = Vec::with_capacity(to.len());
        for field in to.iter() {
            let source = from.iter().position(|f| same_column(f, field));
            sources.push(source);
            casts.push(source.and_then(|i| {
                let from_type = from[i].data_type();
                (from_type != field.data_type()).then(|| from_type.clone())
            }));
        }
        let identity = from.len() == to.len()
            && sources.iter().enumerate().all(|(i, s)| *s == Some(i))
            && casts.iter().all(Option::is_none);

        Remap {
            sources,
            casts,
            target: to.clone(),
            arrow: Arc::new(ArrowSchema::from(to)),
            source_len: from.len(),
            identity,
        }
    }

    pub fn select(&self, columns: &[usize]) -> Result<Self, Error> {
        let mut sources = Vec::with_capacity(columns.len());
        let mut casts = Vec::with_capacity(columns.len());
        let mut fields = Vec::with_capacity(columns.len());
        for &column in columns {
            let field = self.target.get(column).ok_or(Error::Projection(format!(
                "column {column} is out of range for {} columns",
                self.target.len()
            )))?;
            sources.push(self.sources[column]);
            casts.push(self.casts[column].clone());
            fields.push(field.clone());
        }
        let target = Fields::new(fields).map_err(|e| Error::Projection(e.to_string()))?;
        let identity = self.identity && columns.iter().enumerate().all(|(i, c)| i == *c);

        Ok(Remap {
            sources,
            casts,
            arrow: Arc::new(ArrowSchema::from(&target)),
            target,
            source_len: self.source_len,
            identity,
        })
    }

    pub fn is_identity(&self) -> bool {
        self.identity
    }

    pub fn target(&self) -> &Fields {
        &self.target
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.arrow
    }

    pub fn position(&self, source: usize) -> Option<usize> {
        self.sources.iter().position(|s| *s == Some(source))
    }

    pub fn decode_columns(&self) -> Option<Vec<usize>> {
        if self.identity {
            return None;
        }
        let mut columns: Vec<usize> = self.sources.iter().flatten().copied().collect();
        columns.sort_unstable();
        columns.dedup();
        Some(columns)
    }

    pub fn batch(&self, batch: RecordBatch) -> Result<RecordBatch, Error> {
        if self.identity {
            return Ok(batch);
        }
        let decoded = self.decode_columns().unwrap_or_default();
        let batch = if batch.num_columns() == self.source_len && decoded.len() != self.source_len {
            batch
                .project(&decoded)
                .map_err(|e| Error::Ipc(e.to_string()))?
        } else {
            batch
        };
        let rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.sources.len());
        for (index, (source, field)) in self.sources.iter().zip(self.arrow.fields()).enumerate() {
            let Some(source) = source else {
                columns.push(new_null_array(field.data_type(), rows));
                continue;
            };
            let position = decoded
                .binary_search(source)
                .map_err(|_| Error::Projection(format!("source column {source} not decoded")))?;
            let column = batch.column(position);
            if self.casts[index].is_some() && column.data_type() != field.data_type() {
                columns
                    .push(cast(column, field.data_type()).map_err(|e| Error::Ipc(e.to_string()))?);
            } else {
                columns.push(Arc::clone(column));
            }
        }

        RecordBatch::try_new(Arc::clone(&self.arrow), columns)
            .map_err(|e| Error::Ipc(e.to_string()))
    }

    pub fn row<'a>(&self, row: &[Option<Scalar<'a>>]) -> Row<'a> {
        if self.identity {
            return row.to_vec();
        }
        self.sources
            .iter()
            .zip(&self.casts)
            .zip(self.target.iter())
            .map(|((source, from), field)| {
                let value = source.and_then(|i| row.get(i).copied().flatten())?;
                Some(match from {
                    Some(from) => promote(value, from, field.data_type()),
                    None => value,
                })
            })
            .collect()
    }
}

fn same_column(a: &Field, b: &Field) -> bool {
    match (a.id(), b.id()) {
        (Some(x), Some(y)) => x == y,
        _ => a.name() == b.name(),
    }
}

fn promote<'a>(value: Scalar<'a>, from: &DataType, to: &DataType) -> Scalar<'a> {
    match (value, from.kind(), to.kind()) {
        (Scalar::TinyInt(v), _, Kind::SmallInt) => Scalar::SmallInt(i16::from(v)),
        (Scalar::TinyInt(v), _, Kind::Int) => Scalar::Int(i32::from(v)),
        (Scalar::TinyInt(v), _, Kind::BigInt) => Scalar::BigInt(i64::from(v)),
        (Scalar::SmallInt(v), _, Kind::Int) => Scalar::Int(i32::from(v)),
        (Scalar::SmallInt(v), _, Kind::BigInt) => Scalar::BigInt(i64::from(v)),
        (Scalar::Int(v), _, Kind::BigInt) => Scalar::BigInt(i64::from(v)),
        (Scalar::Float(v), _, Kind::Double) => Scalar::Double(f64::from(v)),
        (Scalar::Decimal { unscaled, .. }, _, Kind::Decimal(d)) => Scalar::Decimal {
            unscaled,
            precision: d.precision(),
        },
        (Scalar::Timestamp { at, .. }, _, Kind::Timestamp(p) | Kind::TimestampLtz(p)) => {
            Scalar::Timestamp {
                at,
                precision: p.get(),
            }
        }
        (value, _, _) => value,
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int32Array, Int64Array, StringArray};
    use mink_types::FieldId;

    use super::*;

    fn fields(spec: &[(&str, u32, DataType)]) -> Fields {
        Fields::new(
            spec.iter()
                .map(|(name, id, data_type)| {
                    Field::new(*name, data_type.clone())
                        .unwrap()
                        .with_id(FieldId(*id))
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn renames_drops_adds_and_promotes_by_field_id() {
        let from = fields(&[
            ("k", 0, DataType::big_int()),
            ("v", 1, DataType::string()),
            ("w", 2, DataType::int()),
        ]);
        let to = fields(&[
            ("k", 0, DataType::big_int()),
            ("w", 2, DataType::big_int()),
            ("value", 1, DataType::string()),
            ("x", 3, DataType::string()),
        ]);
        let remap = Remap::new(&from, &to);
        assert!(!remap.is_identity());
        assert_eq!(remap.decode_columns(), Some(vec![0, 1, 2]));

        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::from(&from)),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let out = remap.batch(batch).unwrap();
        assert_eq!(out.schema(), *remap.schema());
        assert_eq!(
            out.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10, 20]
        );
        assert_eq!(
            out.column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "b"
        );
        assert_eq!(out.column(3).null_count(), 2);

        let row = remap.row(&[
            Some(Scalar::BigInt(1)),
            Some(Scalar::String("a")),
            Some(Scalar::Int(10)),
        ]);
        assert_eq!(
            row,
            vec![
                Some(Scalar::BigInt(1)),
                Some(Scalar::BigInt(10)),
                Some(Scalar::String("a")),
                None
            ]
        );

        let selected = remap.select(&[3, 1]).unwrap();
        assert_eq!(selected.decode_columns(), Some(vec![2]));
        let decoded = RecordBatch::try_new(
            Arc::new(ArrowSchema::from(&from).project(&[2]).unwrap()),
            vec![Arc::new(Int32Array::from(vec![10]))],
        )
        .unwrap();
        let out = selected.batch(decoded).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.column(0).null_count(), 1);
        assert_eq!(
            out.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
    }

    #[test]
    fn same_layout_is_identity() {
        let f = fields(&[("k", 0, DataType::big_int()), ("v", 1, DataType::string())]);
        let remap = Remap::new(&f, &f);
        assert!(remap.is_identity());
        assert_eq!(remap.decode_columns(), None);
        assert!(!remap.select(&[1]).unwrap().is_identity());
        assert!(remap.select(&[0, 1]).unwrap().is_identity());
    }
}
