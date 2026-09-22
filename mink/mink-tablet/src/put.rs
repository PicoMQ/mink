//! A put request: rows, per-row operation, writer identity and optional target columns.

use arrow_array::RecordBatch;
use mink_record::header::{NO_BATCH_SEQUENCE, NO_WRITER_ID};
use mink_table::SchemaId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Upsert,
    Delete,
}

#[derive(Debug, Clone)]
pub struct Put {
    pub schema_id: SchemaId,
    pub writer_id: i64,
    pub batch_sequence: i32,
    pub rows: RecordBatch,
    pub ops: Vec<Op>,
    pub target_columns: Option<Vec<usize>>,
}

impl Put {
    pub fn upsert(schema_id: SchemaId, rows: RecordBatch) -> Self {
        let ops = vec![Op::Upsert; rows.num_rows()];
        Put {
            schema_id,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
            rows,
            ops,
            target_columns: None,
        }
    }

    pub fn delete(schema_id: SchemaId, rows: RecordBatch) -> Self {
        let ops = vec![Op::Delete; rows.num_rows()];
        Put {
            ops,
            ..Put::upsert(schema_id, rows)
        }
    }

    pub fn with_ops(mut self, ops: Vec<Op>) -> Self {
        self.ops = ops;
        self
    }

    pub fn with_writer(mut self, writer_id: i64, batch_sequence: i32) -> Self {
        self.writer_id = writer_id;
        self.batch_sequence = batch_sequence;
        self
    }

    pub fn with_target_columns(mut self, columns: Vec<usize>) -> Self {
        self.target_columns = Some(columns);
        self
    }
}
