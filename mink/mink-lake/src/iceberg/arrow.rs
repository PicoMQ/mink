//! Converts a tiered batch to the Iceberg table's Arrow layout: casts columns and null-fills added ones.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::cast;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::Schema;

use crate::error::{Error, Result};
use crate::writer::TieredBatch;

#[derive(Debug, Clone)]
pub(crate) struct Layout {
    schema: ArrowSchemaRef,
}

impl Layout {
    pub(crate) fn new(table_schema: &Schema) -> Result<Self> {
        Ok(Layout {
            schema: Arc::new(schema_to_arrow_schema(table_schema)?),
        })
    }

    pub(crate) fn schema(&self) -> &ArrowSchemaRef {
        &self.schema
    }

    pub(crate) fn convert(&self, batch: &TieredBatch) -> Result<RecordBatch> {
        let rows = &batch.rows;
        let fields = self.schema.fields();
        if rows.num_columns() > fields.len() {
            return Err(Error::invalid(format!(
                "batch has {} columns but the Iceberg table has {}",
                rows.num_columns(),
                fields.len()
            )));
        }

        let n = rows.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            if index >= rows.num_columns() {
                columns.push(arrow_array::new_null_array(field.data_type(), n));
                continue;
            }
            let column = rows.column(index);
            if column.data_type() == field.data_type() {
                columns.push(column.clone());
            } else {
                columns.push(cast(column, field.data_type())?);
            }
        }

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}
