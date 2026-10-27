use std::ops::RangeInclusive;

use bytes::{Buf, Bytes};
use kafka_protocol::compression::{Decompressor, Gzip, Lz4, Snappy, Zstd};
use mink_core::{Header, Record};

use crate::Error;

const PREFIX_SIZE: usize = 12;
const BATCH_HEADER_SIZE: usize = 61;
const MAGIC: i8 = 2;

const COMPRESSION: i16 = 0b0000_0111;
const LOG_APPEND_TIME: i16 = 0b0000_1000;
const TRANSACTIONAL: i16 = 0b0001_0000;
const CONTROL: i16 = 0b0010_0000;

const UNCOMPRESSED: i16 = 0;
const GZIP: i16 = 1;
const SNAPPY: i16 = 2;
const LZ4: i16 = 3;
const ZSTD: i16 = 4;

const CONTROL_KEY_VERSION: i16 = 0;
const ABORT: i16 = 0;
const COMMIT: i16 = 1;

const NULL_LENGTH: i32 = -1;
const VARINT_WIDTH: usize = 5;
const VARLONG_WIDTH: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Batch {
    pub(crate) base_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) producer_id: i64,
    pub(crate) transactional: bool,
    pub(crate) batch_type: BatchType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BatchType {
    Data(Vec<Record>),
    Control(ControlType),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlType {
    Abort,
    Commit,
    Other,
}

impl Batch {
    pub(crate) fn parse(mut bytes: Bytes) -> Result<Vec<Batch>, Error> {
        let mut batches = Vec::new();

        while bytes.len() >= PREFIX_SIZE {
            let base_offset = (&bytes[0..8]).get_i64();
            let length = (&bytes[8..12]).get_i32();
            let size = usize::try_from(length)
                .ok()
                .map(|length| PREFIX_SIZE + length)
                .filter(|size| *size >= BATCH_HEADER_SIZE)
                .ok_or(Error::CorruptBatch { base_offset })?;
            if bytes.len() < size {
                break;
            }

            batches.push(Batch::parse_one(bytes.split_to(size))?);
        }

        Ok(batches)
    }

    fn parse_one(mut bytes: Bytes) -> Result<Batch, Error> {
        let base_offset = bytes.get_i64();
        let corrupt = || Error::CorruptBatch { base_offset };
        let _batch_length = bytes.get_i32();
        let _partition_leader_epoch = bytes.get_i32();
        if bytes.get_i8() != MAGIC {
            return Err(corrupt());
        }

        let crc = bytes.get_u32();
        if crc32c::crc32c(&bytes) != crc {
            return Err(corrupt());
        }

        let attributes = bytes.get_i16();
        let last_offset = Some(bytes.get_i32())
            .filter(|delta| *delta >= 0 && base_offset >= 0)
            .and_then(|delta| base_offset.checked_add(i64::from(delta)))
            .ok_or_else(corrupt)?;
        let base_timestamp = bytes.get_i64();
        let max_timestamp = bytes.get_i64();
        let producer_id = bytes.get_i64();
        let _producer_epoch = bytes.get_i16();
        let _base_sequence = bytes.get_i32();
        let count = usize::try_from(bytes.get_i32()).map_err(|_| corrupt())?;

        let mut body = decompress(attributes & COMPRESSION, bytes).ok_or_else(corrupt)?;
        let timestamp = |delta: i64| match attributes & LOG_APPEND_TIME != 0 {
            true => Some(max_timestamp),
            false => base_timestamp.checked_add(delta),
        };
        let mut records = Vec::with_capacity(count.min(body.len()));
        for _ in 0..count {
            records.push(record(&mut body, base_offset..=last_offset, timestamp)?);
        }

        if body.has_remaining() {
            return Err(corrupt());
        }

        let batch_type = match attributes & CONTROL != 0 {
            false => BatchType::Data(records),
            true => BatchType::Control(control_type(&records).ok_or_else(corrupt)?),
        };

        Ok(Batch {
            base_offset,
            last_offset,
            producer_id,
            transactional: attributes & TRANSACTIONAL != 0,
            batch_type,
        })
    }
}

fn record(
    bytes: &mut Bytes,
    offsets: RangeInclusive<i64>,
    timestamp: impl Fn(i64) -> Option<i64>,
) -> Result<Record, Error> {
    let base_offset = *offsets.start();
    let corrupt = || Error::CorruptBatch { base_offset };
    let length = varint(bytes)
        .and_then(|length| usize::try_from(length).ok())
        .filter(|length| *length <= bytes.len())
        .ok_or_else(corrupt)?;
    let mut bytes = bytes.split_to(length);

    let _attributes = bytes.try_get_i8().map_err(|_| corrupt())?;
    let timestamp_ms = varlong(&mut bytes)
        .and_then(&timestamp)
        .ok_or_else(corrupt)?;
    let offset = varint(&mut bytes)
        .and_then(|delta| base_offset.checked_add(i64::from(delta)))
        .filter(|offset| offsets.contains(offset))
        .ok_or_else(corrupt)?;

    let key = field(&mut bytes).ok_or_else(corrupt)?;
    let value = field(&mut bytes).ok_or_else(corrupt)?;

    let count = varint(&mut bytes)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(corrupt)?;
    let mut headers = Vec::with_capacity(count.min(bytes.len()));
    for _ in 0..count {
        let key = field(&mut bytes).flatten().ok_or_else(corrupt)?;
        let key =
            String::from_utf8(key.to_vec()).map_err(|_| Error::InvalidHeaderKey { offset })?;
        let value = field(&mut bytes).ok_or_else(corrupt)?;
        headers.push(Header::new(key, value));
    }

    if bytes.has_remaining() {
        return Err(corrupt());
    }

    Record::new(offset, timestamp_ms, key, value, headers).map_err(|_| corrupt())
}

fn control_type(records: &[Record]) -> Option<ControlType> {
    let [record] = records else { return None };
    let mut key = record.key().filter(|key| key.len() == 4)?.clone();

    if key.get_i16() != CONTROL_KEY_VERSION {
        return None;
    }

    Some(match key.get_i16() {
        ABORT => ControlType::Abort,
        COMMIT => ControlType::Commit,
        _ => ControlType::Other,
    })
}

fn decompress(codec: i16, mut bytes: Bytes) -> Option<Bytes> {
    let take = |out: &mut Bytes| Ok(out.split_off(0));

    match codec {
        UNCOMPRESSED => Some(bytes),
        GZIP => Gzip::decompress(&mut bytes, take).ok(),
        SNAPPY => Snappy::decompress(&mut bytes, take).ok(),
        LZ4 => Lz4::decompress(&mut bytes, take).ok(),
        ZSTD => Zstd::decompress(&mut bytes, take).ok(),
        _ => None,
    }
}

fn field(bytes: &mut Bytes) -> Option<Option<Bytes>> {
    match varint(bytes)? {
        NULL_LENGTH => Some(None),
        length => {
            let length = usize::try_from(length).ok()?;

            (length <= bytes.len()).then(|| Some(bytes.split_to(length)))
        }
    }
}

fn varint(bytes: &mut Bytes) -> Option<i32> {
    let value = u32::try_from(unsigned(bytes, VARINT_WIDTH)?).ok()?;

    Some((value >> 1) as i32 ^ -((value & 1) as i32))
}

fn varlong(bytes: &mut Bytes) -> Option<i64> {
    let value = unsigned(bytes, VARLONG_WIDTH)?;

    Some((value >> 1) as i64 ^ -((value & 1) as i64))
}

fn unsigned(bytes: &mut Bytes, width: usize) -> Option<u64> {
    let mut value = 0u64;

    for index in 0..width {
        let byte = bytes.try_get_u8().ok()?;
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{
        Compression, Record as Encoded, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    use super::*;

    const LENGTH: std::ops::Range<usize> = 8..12;
    const MAGIC_POSITION: usize = 16;
    const CRC: std::ops::Range<usize> = 17..21;
    const ATTRIBUTES_LOW_BYTE: usize = 22;
    const LAST_OFFSET_DELTA: std::ops::Range<usize> = 23..27;
    const RECORDS_COUNT: std::ops::Range<usize> = 57..61;
    const TRUNCATED_BYTES: usize = 7;

    fn set(range: std::ops::Range<usize>, value: i32) -> impl FnOnce(&mut Vec<u8>) {
        move |bytes| bytes[range].copy_from_slice(&value.to_be_bytes())
    }

    fn encoded(offset: i64, timestamp: i64) -> Encoded {
        let mut headers = IndexMap::new();
        headers.insert(
            StrBytes::from_static_str("trace"),
            Some(Bytes::from_static(b"t1")),
        );
        headers.insert(StrBytes::from_static_str("empty"), None);

        Encoded {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: offset as i32,
            timestamp,
            key: (offset % 2 == 0).then(|| Bytes::from(format!("k{offset}"))),
            value: (offset % 3 != 0).then(|| Bytes::from(format!("v{offset}"))),
            headers,
        }
    }

    fn encode(records: &[Encoded], compression: Compression) -> Bytes {
        let mut buffer = BytesMut::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression,
        };

        RecordBatchEncoder::encode(&mut buffer, records, &options).unwrap();

        buffer.freeze()
    }

    fn rewritten(bytes: &Bytes, change: impl FnOnce(&mut Vec<u8>)) -> Bytes {
        let mut bytes = bytes.to_vec();
        change(&mut bytes);

        let crc = crc32c::crc32c(&bytes[CRC.end..]);
        bytes[CRC].copy_from_slice(&crc.to_be_bytes());

        Bytes::from(bytes)
    }

    fn expected(record: &Encoded) -> Record {
        Record::new(
            record.offset,
            record.timestamp,
            record.key.clone(),
            record.value.clone(),
            vec![
                Header::new("trace", Some(Bytes::from_static(b"t1"))),
                Header::new("empty", None),
            ],
        )
        .unwrap()
    }

    fn data(batches: Vec<Batch>) -> Vec<Record> {
        batches
            .into_iter()
            .filter_map(|batch| match batch.batch_type {
                BatchType::Data(records) => Some(records),
                BatchType::Control(_) => None,
            })
            .flatten()
            .collect()
    }

    #[test]
    fn encoded_batches_parse_to_their_records_with_every_codec() {
        let records: Vec<_> = (10..15)
            .map(|offset| encoded(offset, 1_000 + offset))
            .collect();
        let want: Vec<_> = records.iter().map(expected).collect();

        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let batches = Batch::parse(encode(&records, compression)).unwrap();

            assert_eq!(batches.len(), 1, "{compression:?}");
            assert_eq!(
                (batches[0].base_offset, batches[0].last_offset),
                (10, 14),
                "{compression:?}"
            );
            assert_eq!(data(batches), want, "{compression:?}");
        }
    }

    #[test]
    fn log_append_time_gives_every_record_the_batch_max_timestamp() {
        let encoded = encode(&[encoded(0, 5_000), encoded(1, 7_000)], Compression::None);
        let bytes = rewritten(&encoded, |bytes| {
            bytes[ATTRIBUTES_LOW_BYTE] |= LOG_APPEND_TIME as u8
        });

        let records = data(Batch::parse(bytes).unwrap());

        let timestamps: Vec<_> = records.iter().map(Record::timestamp_ms).collect();
        assert_eq!(timestamps, [7_000, 7_000]);
    }

    #[test]
    fn corrupt_batches_are_errors() {
        let good = encode(&[encoded(3, 1)], Compression::None);
        let two = encode(&[encoded(3, 1), encoded(4, 1)], Compression::None);
        let mut flipped = good.to_vec();
        *flipped.last_mut().unwrap() ^= 0xff;

        let cases = [
            ("flipped_byte", Bytes::from(flipped)),
            (
                "old_magic",
                rewritten(&good, |bytes| bytes[MAGIC_POSITION] = 1),
            ),
            ("short_length", rewritten(&good, set(LENGTH, 10))),
            (
                "unknown_codec",
                rewritten(&good, |bytes| bytes[ATTRIBUTES_LOW_BYTE] |= 5),
            ),
            (
                "invalid_gzip",
                rewritten(&good, |bytes| bytes[ATTRIBUTES_LOW_BYTE] |= GZIP as u8),
            ),
            (
                "more_records_than_bytes",
                rewritten(&good, set(RECORDS_COUNT, 2)),
            ),
            (
                "fewer_records_than_bytes",
                rewritten(&good, set(RECORDS_COUNT, 0)),
            ),
            (
                "negative_last_offset_delta",
                rewritten(&good, set(LAST_OFFSET_DELTA, -1)),
            ),
            (
                "record_past_the_last_offset",
                rewritten(&two, set(LAST_OFFSET_DELTA, 0)),
            ),
        ];

        for (name, bytes) in cases {
            let parsed = Batch::parse(bytes);

            assert!(
                matches!(parsed, Err(Error::CorruptBatch { base_offset: 3 })),
                "{name}: {parsed:?}"
            );
        }
    }

    #[test]
    fn a_header_key_that_is_not_utf8_is_an_error() {
        let good = encode(&[encoded(6, 1)], Compression::None);
        let key = good
            .windows(b"trace".len())
            .position(|window| window == b"trace")
            .unwrap();

        let bytes = rewritten(&good, |bytes| bytes[key] = 0xff);

        let parsed = Batch::parse(bytes);
        assert!(
            matches!(parsed, Err(Error::InvalidHeaderKey { offset: 6 })),
            "{parsed:?}"
        );
    }

    #[test]
    fn a_truncated_last_batch_is_dropped() {
        let first = encode(&[encoded(0, 1)], Compression::None);
        let second = encode(&[encoded(1, 1)], Compression::None);
        let mut bytes = BytesMut::from(&first[..]);
        bytes.extend_from_slice(&second[..second.len() - TRUNCATED_BYTES]);

        let batches = Batch::parse(bytes.freeze()).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].base_offset, 0);
    }

    #[test]
    fn fewer_bytes_than_a_batch_prefix_are_no_batches() {
        let batches = Batch::parse(Bytes::from_static(&[0; 11])).unwrap();

        assert!(batches.is_empty());
    }

    #[test]
    fn control_batches_of_other_types_keep_their_offsets_and_type() {
        let mut marker = encoded(4, 1);
        marker.control = true;
        marker.key = Some(Bytes::from_static(&[0, 0, 0, 2]));
        marker.value = Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0]));

        let batches = Batch::parse(encode(&[marker], Compression::None)).unwrap();

        assert_eq!(
            batches[0].batch_type,
            BatchType::Control(ControlType::Other)
        );
        assert_eq!(batches[0].last_offset, 4);
    }

    #[test]
    fn fixtures_parse_to_exactly_their_records() {
        for fixture in mink_conformance::record_fixtures() {
            let name = fixture.name();

            let parsed = Batch::parse(fixture.batches().clone());

            if let Some(base_offset) = fixture.corrupt_at() {
                assert!(
                    matches!(parsed, Err(Error::CorruptBatch { base_offset: at }) if at == base_offset),
                    "{name}: {parsed:?}"
                );
                continue;
            }

            if !fixture.aborted().is_empty() {
                continue;
            }

            let fetched: Vec<_> = data(parsed.unwrap())
                .into_iter()
                .filter(|record| record.offset() >= fixture.fetch_offset())
                .collect();
            assert_eq!(fetched, fixture.expected(), "{name}");
        }
    }
}
