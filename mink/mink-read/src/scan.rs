//! Read options and the column bookkeeping: what to read, what to output, where the keys sit.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Field, Schema as ArrowSchema, SchemaRef};
use mink_table::Schema;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Options {
    pub projection: Option<Vec<usize>>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct Columns {
    pub read: Vec<usize>,
    pub output: Vec<usize>,
    pub keys: Vec<usize>,
    pub read_schema: SchemaRef,
    pub output_schema: SchemaRef,
}

impl Columns {
    pub fn new(schema: &Schema, options: &Options) -> Result<Self> {
        let all = Arc::new(ArrowSchema::from(schema.fields()));
        let wanted: Vec<usize> = match &options.projection {
            Some(columns) => {
                for &c in columns {
                    if c >= all.fields().len() {
                        return Err(Error::Schema(format!(
                            "column {c} is out of range for {} columns",
                            all.fields().len()
                        )));
                    }
                }
                columns.clone()
            }
            None => (0..all.fields().len()).collect(),
        };

        let mut read = wanted.clone();
        let key_positions = schema.primary_key_indexes();
        read.extend(key_positions.iter().copied());
        read.sort_unstable();
        read.dedup();
        let output = wanted
            .iter()
            .map(|c| read.iter().position(|r| r == c).expect("wanted ⊆ read"))
            .collect();
        let keys = key_positions
            .iter()
            .map(|k| read.iter().position(|r| r == k).expect("keys ⊆ read"))
            .collect();

        Ok(Columns {
            read_schema: Arc::new(all.project(&read)?),
            output_schema: Arc::new(all.project(&wanted)?),
            read,
            output,
            keys,
        })
    }

    pub fn needs_final_projection(&self) -> bool {
        self.output.len() != self.read.len() || self.output.iter().enumerate().any(|(i, o)| i != *o)
    }

    pub fn finish(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if !self.needs_final_projection() {
            return Ok(batch);
        }

        project(&batch, &self.output)
    }

    pub fn normalize(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let fields = self.read_schema.fields();
        if batch.num_columns() < fields.len() {
            return Err(Error::Schema(format!(
                "batch has {} columns, the read needs {}",
                batch.num_columns(),
                fields.len()
            )));
        }

        let columns = batch
            .columns()
            .iter()
            .zip(fields.iter())
            .map(|(column, field): (_, &Arc<Field>)| {
                if column.data_type() == field.data_type() {
                    Ok(column.clone())
                } else {
                    Ok(arrow_cast::cast(column, field.data_type())?)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new_with_options(
            self.read_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?)
    }
}

pub fn project(batch: &RecordBatch, columns: &[usize]) -> Result<RecordBatch> {
    let schema = Arc::new(batch.schema().project(columns)?);
    let arrays = columns.iter().map(|&c| batch.column(c).clone()).collect();

    Ok(RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}
