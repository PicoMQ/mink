use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int32Array, Int64Array, LargeBinaryArray, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use arrow_buffer::OffsetBuffer;
use mink_core::{Header, Part, Partition, Record};
use mink_schema::{
    ColumnValue, HEADERS_ID, KEY_ID, OFFSET_ID, PARTITION_ID, PartValues, TIMESTAMP_ID, VALUE_ID,
};

use crate::Error;
use crate::arrays::array;
use crate::schema::{BINARY_ARROW, FileSchema};

const MICROS_PER_MILLI: i64 = 1000;

struct PartColumns<'a> {
    raw: Option<&'a [u8]>,
    columns: HashMap<i32, ColumnValue>,
}

pub struct Decoded<'a> {
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    columns: HashMap<i32, ColumnValue>,
}

impl Decoded<'_> {
    pub fn key(&self) -> Option<&[u8]> {
        self.key
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value
    }

    pub fn column(&self, id: i32) -> Option<&ColumnValue> {
        self.columns.get(&id)
    }
}

pub fn decode<'a>(schema: &FileSchema, record: &'a Record) -> Result<Decoded<'a>, Error> {
    let part = |part| record.part(part).map(AsRef::as_ref);

    let key = part_columns(schema, Part::Key, part(Part::Key), record.headers())?;
    let value = part_columns(schema, Part::Value, part(Part::Value), record.headers())?;

    let mut columns = key.columns;
    columns.extend(value.columns);
    Ok(Decoded {
        key: key.raw,
        value: value.raw,
        columns,
    })
}

pub(crate) fn rows(
    partition: &Partition,
    records: &[Record],
    schema: &FileSchema,
) -> Result<RecordBatch, Error> {
    let timestamps = timestamps(records)?;

    let decoded = records
        .iter()
        .map(|record| decode(schema, record))
        .collect::<Result<Vec<_>, _>>()?;

    let mut columns: Vec<ArrayRef> = Vec::new();
    for (field, nested) in schema
        .arrow()
        .fields()
        .iter()
        .zip(schema.schema().as_struct().fields())
    {
        let column: ArrayRef = match nested.id {
            PARTITION_ID => Arc::new(Int32Array::from(vec![partition.index(); records.len()])),
            OFFSET_ID => Arc::new(Int64Array::from_iter_values(
                records.iter().map(Record::offset),
            )),
            TIMESTAMP_ID => Arc::new(
                TimestampMicrosecondArray::from(timestamps.clone())
                    .with_timezone_opt(BINARY_ARROW.timezone.clone()),
            ),
            KEY_ID => Arc::new(LargeBinaryArray::from_iter(
                decoded.iter().map(Decoded::key),
            )),
            HEADERS_ID => headers(records)?,
            VALUE_ID => Arc::new(LargeBinaryArray::from_iter(
                decoded.iter().map(Decoded::value),
            )),
            id => {
                let column_values: Vec<Option<&ColumnValue>> =
                    decoded.iter().map(|decoded| decoded.column(id)).collect();

                array(field, &nested.field_type, &column_values)?
            }
        };

        columns.push(column);
    }

    Ok(RecordBatch::try_new(schema.arrow().clone(), columns)?)
}

fn part_columns<'a>(
    schema: &FileSchema,
    part: Part,
    bytes: Option<&'a [u8]>,
    headers: &[Header],
) -> Result<PartColumns<'a>, Error> {
    let raw = |raw: Option<&'a [u8]>| PartColumns {
        raw,
        columns: HashMap::new(),
    };
    let FileSchema::Typed(typed) = schema else {
        return Ok(raw(bytes));
    };

    let columns = match typed
        .decoders()
        .values(part, bytes, headers, typed.schema())?
    {
        PartValues::Null => raw(None),
        PartValues::Raw(bytes) => raw(Some(bytes)),
        PartValues::Columns(columns) => PartColumns { raw: None, columns },
    };

    Ok(columns)
}

fn timestamps(records: &[Record]) -> Result<Vec<i64>, Error> {
    records
        .iter()
        .map(|record| {
            record
                .timestamp_ms()
                .checked_mul(MICROS_PER_MILLI)
                .ok_or(Error::TimestampOverflow {
                    offset: record.offset(),
                })
        })
        .collect()
}

fn headers(records: &[Record]) -> Result<ArrayRef, Error> {
    let all: Vec<_> = records.iter().flat_map(Record::headers).collect();
    let keys = StringArray::from_iter_values(all.iter().map(|header| header.key()));
    let values = LargeBinaryArray::from_iter(all.iter().map(|header| header.value()));
    let lengths = records.iter().map(|record| record.headers().len());

    let entries = StructArray::try_new(
        BINARY_ARROW.header_fields.clone(),
        vec![Arc::new(keys), Arc::new(values)],
        None,
    )?;
    let list = ListArray::try_new(
        BINARY_ARROW.header_element.clone(),
        OffsetBuffer::from_lengths(lengths),
        Arc::new(entries),
        None,
    )?;

    Ok(Arc::new(list))
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use bytes::Bytes;
    use mink_core::Header;

    use super::*;

    fn column<T: Clone + 'static>(rows: &RecordBatch, name: &str) -> T {
        rows.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<T>()
            .unwrap()
            .clone()
    }

    #[test]
    fn rows_follow_the_records() {
        let partition = Partition::new("orders", 7).unwrap();
        let records = [
            Record::new(
                40,
                1_700_000_000_123,
                Some(Bytes::from_static(b"k")),
                Some(Bytes::from_static(b"v")),
                vec![
                    Header::new("a", Some(Bytes::from_static(b"1"))),
                    Header::new("a", None),
                    Header::new("b", Some(Bytes::new())),
                ],
            )
            .unwrap(),
            Record::new(41, 5, None, None, Vec::new()).unwrap(),
        ];

        let rows = rows(&partition, &records, &FileSchema::Binary).unwrap();

        let partitions: Int32Array = column(&rows, "_kafka_partition");
        let offsets: Int64Array = column(&rows, "_kafka_offset");
        let timestamps: TimestampMicrosecondArray = column(&rows, "_kafka_timestamp");
        let keys: LargeBinaryArray = column(&rows, "_kafka_key");
        let values: LargeBinaryArray = column(&rows, "_kafka_value");
        let headers: ListArray = column(&rows, "_kafka_headers");

        let first = headers.value(0);
        let first = first.as_any().downcast_ref::<StructArray>().unwrap();
        let header_keys = first
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let header_values = first
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();

        assert_eq!(partitions.values(), &[7, 7]);
        assert_eq!(offsets.values(), &[40, 41]);
        assert_eq!(timestamps.values(), &[1_700_000_000_123_000, 5_000]);
        assert_eq!((keys.value(0), keys.is_null(1)), (&b"k"[..], true));
        assert_eq!((values.value(0), values.is_null(1)), (&b"v"[..], true));
        assert_eq!(
            header_keys.iter().flatten().collect::<Vec<_>>(),
            ["a", "a", "b"]
        );
        assert_eq!(
            (
                header_values.value(0),
                header_values.is_null(1),
                header_values.value(2)
            ),
            (&b"1"[..], true, &b""[..])
        );
        assert_eq!(headers.value(1).len(), 0);
    }

    #[test]
    fn a_timestamp_that_overflows_microseconds_is_an_error() {
        let partition = Partition::new("orders", 0).unwrap();
        let records = [Record::new(9, i64::MAX, None, None, Vec::new()).unwrap()];

        let rows = rows(&partition, &records, &FileSchema::Binary);

        assert!(
            matches!(rows, Err(Error::TimestampOverflow { offset: 9 })),
            "{rows:?}"
        );
    }
}
