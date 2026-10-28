use arrow_array::RecordBatch;
use bytes::Bytes;
use mink_core::{File, Partition, Record, TimePartition};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub(crate) fn hour_records(
    offsets: std::ops::Range<i64>,
    hour_of: impl Fn(i64) -> i64,
) -> Vec<Record> {
    offsets
        .map(|offset| {
            let timestamp = hour_of(offset) * 3_600_000 + offset;
            let value = Bytes::from(format!("value-{offset}"));

            Record::new(offset, timestamp, None, Some(value), Vec::new()).unwrap()
        })
        .collect()
}

pub(crate) fn inside(records: &[Record], file: &File) -> Vec<Record> {
    records
        .iter()
        .filter(|record| file.range().includes(record.offset()))
        .cloned()
        .collect()
}

pub(crate) fn hourly(start: i64, end: i64, bucket: i32) -> File {
    let partition = Partition::new("orders", 0).unwrap();

    File::v1(
        partition,
        start,
        end,
        TimePartition::new(0, Some(bucket)),
        0,
    )
    .unwrap()
}

pub(crate) fn rows_of(bytes: &Bytes) -> RecordBatch {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let schema = reader.schema().clone();
    let batches: Vec<RecordBatch> = reader.build().unwrap().collect::<Result<_, _>>().unwrap();

    arrow_select::concat::concat_batches(&schema, &batches).unwrap()
}
