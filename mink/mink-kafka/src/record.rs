//! The one place that knows a topic's row shape: Kafka records to an Arrow batch and a log batch
//! back to Kafka records, with headers packed into a single binary column.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, TimestampMillisecondBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record,
    RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, RecordSet, TimestampType,
};
use mink_common::codec::{get_bytes, get_u32, put_bytes};
use mink_record::{Batch, ChangeType, Header, Spec, build, codec};
use mink_table::{LogFormat, SchemaId};

use crate::error::Error;
use crate::topic;

const SCHEMA: SchemaId = SchemaId(0);

pub(crate) fn schema() -> Result<SchemaRef, Error> {
    Ok(Arc::new(ArrowSchema::from(topic::schema()?.fields())))
}

pub(crate) fn decode_kafka(bytes: &mut Bytes) -> Result<Vec<RecordSet>, Error> {
    RecordBatchDecoder::decode_all(bytes).map_err(|e| Error::Protocol(e.to_string()))
}

pub(crate) fn to_log(records: &[Record], now_ms: i64) -> Result<Bytes, Error> {
    let batch = to_arrow(records, now_ms)?;
    let codec = codec(LogFormat::Arrow, Default::default());
    let changes = vec![ChangeType::AppendOnly; batch.num_rows()];

    Ok(build(Spec::new(SCHEMA, true), &changes, &batch, codec.as_ref())?.into())
}

pub(crate) fn to_arrow(records: &[Record], now_ms: i64) -> Result<RecordBatch, Error> {
    let mut keys = BinaryBuilder::new();
    let mut values = BinaryBuilder::new();
    let mut headers = BinaryBuilder::new();
    let mut timestamps = TimestampMillisecondBuilder::new().with_timezone("UTC");
    for record in records {
        keys.append_option(record.key.as_deref());
        values.append_option(record.value.as_deref());
        headers.append_option(pack(&record.headers).as_deref());
        timestamps.append_value(if record.timestamp < 0 {
            now_ms
        } else {
            record.timestamp
        });
    }

    Ok(RecordBatch::try_new(
        schema()?,
        vec![
            Arc::new(keys.finish()),
            Arc::new(values.finish()),
            Arc::new(headers.finish()),
            Arc::new(timestamps.finish()),
        ],
    )?)
}

pub(crate) fn from_log(bytes: Bytes) -> Result<(Header, Vec<Record>), Error> {
    let batch = Batch::parse(bytes)?;
    let header = *batch.header();
    let codec = codec(LogFormat::Arrow, Default::default());
    let rows = batch.records(codec.as_ref(), schema()?, None)?.batch;
    let keys = rows.column(0).as_binary::<i32>();
    let values = rows.column(1).as_binary::<i32>();
    let headers = rows.column(2).as_binary::<i32>();
    let timestamps = rows.column(3).as_primitive::<TimestampMillisecondType>();

    let mut records = Vec::with_capacity(rows.num_rows());
    for i in 0..rows.num_rows() {
        let slice = |array: &arrow_array::BinaryArray| {
            (!array.is_null(i)).then(|| Bytes::copy_from_slice(array.value(i)))
        };
        records.push(Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: header.leader_epoch.max(NO_PARTITION_LEADER_EPOCH),
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: header.base_offset + i as i64,
            sequence: i as i32,
            timestamp: timestamps.value(i),
            key: slice(keys),
            value: slice(values),
            headers: slice(headers).map(unpack).transpose()?.unwrap_or_default(),
        });
    }

    Ok((header, records))
}

pub(crate) fn to_kafka(records: &[Record]) -> Result<Bytes, Error> {
    let mut out = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut out,
        records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .map_err(|e| Error::Protocol(e.to_string()))?;

    Ok(out.freeze())
}

fn pack(headers: &IndexMap<StrBytes, Option<Bytes>>) -> Option<Bytes> {
    if headers.is_empty() {
        return None;
    }
    let mut out = BytesMut::new();
    out.extend_from_slice(&(headers.len() as u32).to_le_bytes());
    for (key, value) in headers {
        put_bytes(&mut out, key.as_bytes());
        match value {
            Some(value) => {
                out.extend_from_slice(&[1]);
                put_bytes(&mut out, value);
            }
            None => out.extend_from_slice(&[0]),
        }
    }

    Some(out.freeze())
}

fn unpack(mut bytes: Bytes) -> Result<IndexMap<StrBytes, Option<Bytes>>, Error> {
    let corrupt = |e: mink_common::codec::Error| Error::Protocol(format!("headers: {e}"));
    let count = get_u32(&mut bytes).map_err(corrupt)?;
    let mut headers = IndexMap::with_capacity(count as usize);
    for _ in 0..count {
        let key = get_bytes(&mut bytes).map_err(corrupt)?;
        let key = StrBytes::try_from(key).map_err(|e| Error::Protocol(format!("headers: {e}")))?;
        let value = match mink_common::codec::get_u8(&mut bytes).map_err(corrupt)? {
            0 => None,
            _ => Some(get_bytes(&mut bytes).map_err(corrupt)?),
        };
        headers.insert(key, value);
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(offset: i64, key: Option<&str>, value: &str) -> Record {
        let mut headers = IndexMap::new();
        headers.insert(
            StrBytes::from_static_str("h"),
            Some(Bytes::from_static(b"v")),
        );
        headers.insert(StrBytes::from_static_str("empty"), None);
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: -1,
            timestamp: 1_700_000_000_000 + offset,
            key: key.map(|k| Bytes::copy_from_slice(k.as_bytes())),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers,
        }
    }

    #[test]
    fn records_round_trip_through_the_log_batch() {
        let records = vec![record(0, Some("a"), "1"), record(1, None, "2")];
        let bytes = to_log(&records, 5).unwrap();
        let (header, decoded) = from_log(bytes).unwrap();
        assert_eq!(header.record_count, 2);
        for (a, b) in records.iter().zip(&decoded) {
            assert_eq!(a.key, b.key);
            assert_eq!(a.value, b.value);
            assert_eq!(a.timestamp, b.timestamp);
            assert_eq!(a.headers, b.headers);
        }
    }

    #[test]
    fn kafka_batches_round_trip() {
        let mut a = record(10, Some("k"), "v");
        let mut b = record(11, Some("k2"), "v2");
        a.sequence = 0;
        b.sequence = 1;
        let records = vec![a, b];
        let mut bytes = to_kafka(&records).unwrap();
        let sets = decode_kafka(&mut bytes).unwrap();
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].records[0].offset, 10);
        assert_eq!(sets[0].records[1].value, records[1].value);
    }

    #[test]
    fn missing_timestamps_take_the_append_time() {
        let mut r = record(0, None, "x");
        r.timestamp = -1;
        let (_, decoded) = from_log(to_log(&[r], 77).unwrap()).unwrap();
        assert_eq!(decoded[0].timestamp, 77);
    }
}
