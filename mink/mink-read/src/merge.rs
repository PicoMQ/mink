//! Merges the lake stream with the collected log tail of a primary-key bucket: the tail holds the
//! latest change per key, lake rows for those keys are dropped as they stream past, and the tail's
//! surviving rows are emitted once the lake is exhausted.

use std::collections::HashMap;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_row::{RowConverter, SortField};
use arrow_select::filter::filter_record_batch;
use arrow_select::interleave::interleave;
use futures::StreamExt;
use futures::stream::BoxStream;
use mink_lake::TieredBatch;

use crate::error::Result;
use crate::scan::Columns;

type Location = Option<(usize, usize)>;

pub(crate) fn merged(
    lake: BoxStream<'static, Result<RecordBatch>>,
    tail: Vec<TieredBatch>,
    columns: Columns,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let converter = RowConverter::new(
        columns
            .keys
            .iter()
            .map(|&k| SortField::new(columns.read_schema.field(k).data_type().clone()))
            .collect(),
    )?;

    let mut log: Vec<RecordBatch> = Vec::with_capacity(tail.len());
    let mut latest: HashMap<Box<[u8]>, Location> = HashMap::new();
    for batch in &tail {
        let rows = columns.normalize(&batch.rows)?;
        let keys = converter.convert_columns(&key_columns(&rows, &columns.keys))?;
        let index = log.len();
        for row in 0..rows.num_rows() {
            let location = batch.changes.get(row).is_add().then_some((index, row));
            latest.insert(Box::from(keys.row(row).as_ref()), location);
        }
        log.push(rows);
    }

    let state = Merge {
        columns,
        converter,
        log,
        latest,
    };

    Ok(Box::pin(futures::stream::unfold(
        (lake, Some(state)),
        |(mut lake, state)| async move {
            let state = state?;
            match lake.next().await {
                Some(Ok(batch)) => {
                    let out = state.survivors(&batch);
                    Some((out, (lake, Some(state))))
                }
                Some(Err(e)) => Some((Err(e), (lake, None))),
                None => Some((state.tail(), (lake, None))),
            }
        },
    )))
}

struct Merge {
    columns: Columns,
    converter: RowConverter,
    log: Vec<RecordBatch>,
    latest: HashMap<Box<[u8]>, Location>,
}

impl Merge {
    fn survivors(&self, lake: &RecordBatch) -> Result<RecordBatch> {
        let keys = self
            .converter
            .convert_columns(&key_columns(lake, &self.columns.keys))?;
        let keep = BooleanArray::from_iter(
            (0..lake.num_rows()).map(|row| Some(!self.latest.contains_key(keys.row(row).as_ref()))),
        );

        self.columns.finish(filter_record_batch(lake, &keep)?)
    }

    fn tail(&self) -> Result<RecordBatch> {
        let mut picks: Vec<(usize, usize)> = self.latest.values().flatten().copied().collect();
        picks.sort_unstable();
        let schema = self.columns.read_schema.clone();
        if picks.is_empty() {
            return self.columns.finish(RecordBatch::new_empty(schema));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for c in 0..schema.fields().len() {
            let arrays: Vec<&dyn Array> = self.log.iter().map(|b| b.column(c).as_ref()).collect();
            columns.push(interleave(&arrays, &picks)?);
        }

        self.columns.finish(RecordBatch::try_new(schema, columns)?)
    }
}

fn key_columns(batch: &RecordBatch, keys: &[usize]) -> Vec<ArrayRef> {
    keys.iter().map(|&k| batch.column(k).clone()).collect()
}
