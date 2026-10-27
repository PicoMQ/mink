# Records

How Mink turns the record bytes of a Fetch response into the committed records of a partition, and those records into rows. Conformance fixtures in `../conformance/fixtures/records/` pin the behaviour against bytes captured from Apache Kafka 3.9.1.

Mink parses record batches itself. The parser in the `kafka-protocol` crate keeps headers in a map, so a key repeated within one record keeps only one of its values. Kafka allows repeated keys, so that parser would lose data. Mink still uses that crate's codecs to decompress.

## Record batch

Only message format v2 (magic `2`) is supported. Integers are big-endian unless marked as a varint.

| Pos | Size | Field | Notes |
| --- | --- | --- | --- |
| 0 | 8 | `base_offset` | |
| 8 | 4 | `batch_length` | Bytes after this field. The batch is `12 + batch_length` bytes. |
| 12 | 4 | `partition_leader_epoch` | |
| 16 | 1 | `magic` | Must be `2` |
| 17 | 4 | `crc` | CRC-32C (Castagnoli) of every byte from `attributes` to the end of the batch |
| 21 | 2 | `attributes` | Bits 0–2 compression, bit 3 timestamp type, bit 4 transactional, bit 5 control |
| 23 | 4 | `last_offset_delta` | |
| 27 | 8 | `base_timestamp` | |
| 35 | 8 | `max_timestamp` | |
| 43 | 8 | `producer_id` | `-1` if the batch has no producer |
| 51 | 2 | `producer_epoch` | |
| 53 | 4 | `base_sequence` | |
| 57 | 4 | `records_count` | |
| 61 | | `records` | Compressed as a whole when the compression bits are not 0 |

Compression: `0` none, `1` gzip, `2` snappy (Kafka's xerial framing), `3` lz4 (LZ4 frame), `4` zstd. Any other value is an error.

### Record

| Field | Encoding |
| --- | --- |
| `length` | varint, the bytes of the record after this field |
| `attributes` | int8, unused |
| `timestamp_delta` | varlong |
| `offset_delta` | varint |
| `key` | varint length, `-1` for null, then the bytes |
| `value` | varint length, `-1` for null, then the bytes |
| `headers` | varint count, then for each header: a varint length and a UTF-8 key, then a varint length (`-1` for null) and the value bytes |

Varints are zigzag-encoded, as in the Kafka protocol.

- `last_offset = base_offset + last_offset_delta`. A negative `base_offset` or `last_offset_delta` makes the batch corrupt.
- `offset = base_offset + offset_delta`, which must lie in `[base_offset, last_offset]`, or the batch is corrupt.
- When bit 3 of `attributes` is 0 (create time), `timestamp_ms = base_timestamp + timestamp_delta`. When it is 1 (log append time), `timestamp_ms = max_timestamp` for every record in the batch.
- Headers keep their wire order, and a key may repeat.

### Control batches

A control batch (attribute bit 5) holds exactly one record. Its key is `version: int16` (must be `0`) followed by `type: int16`: `0` is an abort marker, `1` a commit marker. A control batch of any other type is kept as an other control batch, so its offset still moves a read forward. No control batch ever produces a row, and only abort markers change which records are kept.

## Parsing a fetch

1. Read batches one after another from the start of the `records` bytes.
2. If fewer than 12 bytes, or fewer than `12 + batch_length` bytes, remain, the last batch is incomplete. The broker is allowed to truncate it, so Mink drops it without an error.
3. A batch whose magic is not `2`, whose CRC does not match, whose record count disagrees with its records, or whose bytes fail to decompress is an error: `corrupt_batch` with the batch's base offset. Mink never skips a corrupt batch.

## Committed records

Fetch returns the partition's aborted transactions as `(producer_id, first_offset)` pairs. Mink keeps a record if and only if a `READ_COMMITTED` Kafka consumer would return it. The rule follows the Apache Kafka consumer:

1. Sort the aborted transactions by `first_offset`, and start with an empty set of aborted producers.
2. For each batch in offset order, first move every aborted transaction with `first_offset <= last_offset` of the batch into the set (where `last_offset = base_offset + last_offset_delta`).
3. If the batch is a control batch: an abort marker removes its `producer_id` from the set. The batch itself is dropped.
4. If the batch is transactional and its `producer_id` is in the set, drop the batch.
5. Otherwise keep every record of the batch whose `offset >= fetch_offset`. A batch can start before the fetch offset, and the records before it are not part of the fetch.

## Rows

Each committed record is one row of the binary schema (`schema.md`). A topic with a typed schema fills its rows as `decoding.md` Rows says, and these columns the same way:

| Column | Value |
| --- | --- |
| `_kafka_partition` | The partition index |
| `_kafka_offset` | `offset` |
| `_kafka_timestamp` | `timestamp_ms * 1000`, in microseconds since the epoch, UTC |
| `_kafka_key` | The key bytes, or null |
| `_kafka_headers` | Every header in wire order, repeats kept, each as `{key, value}` with a null value when the length was `-1` |
| `_kafka_value` | The value bytes, or null |

A header key that is not valid UTF-8 is an error, `invalid_header_key`, with the record's offset. Mink does not alter or drop it.

A create-time timestamp is `base_timestamp + timestamp_delta`. If that overflows, the batch is corrupt. If `timestamp_ms * 1000` overflows, building the rows fails with `timestamp_overflow` and the record's offset.

Rows are built from records by `mink-iceberg`. The Kafka client returns records and knows nothing about the table schema.

## Invariants

1. A record is kept if and only if a `READ_COMMITTED` Kafka consumer returns it.
2. The committed records of an offset range are the same whatever offset the fetches start at and however the broker splits its responses.
3. Every committed record in a range appears exactly once, in offset order.
4. Corruption is always an error. Only a truncated last batch is dropped silently.
5. Every byte of the key, the value and every header is preserved, together with header order and repeated keys.

## Conformance

`conformance/fixtures/records/cases.json` lists 13 cases. Each case has the `records` bytes (`<case>.bin`), the fetch offset, the aborted transactions, the committed records a `READ_COMMITTED` consumer returned, and the error the bytes must produce, if any.

11 cases are captured from Apache Kafka 3.9.1:

- each codec (`none`, `gzip`, `snappy`, `lz4`, `zstd`)
- create time and log append time
- a committed transaction, an aborted transaction, and two producers interleaved with only one aborting
- a fetch starting in the middle of a batch
- repeated header keys, null keys, null values, empty values and null header values

2 cases are derived from the captured `codec_none` bytes: `truncated_last_batch`, with its last 7 bytes removed, and `corrupt_crc`, with the last byte of its first batch flipped.

`cargo run -p mink-conformance -- generate records --kafka-image apache/kafka:3.9.1 --bless` regenerates the fixtures against a broker of that image at `localhost:29092`. librdkafka produces the records, including aborted transactions, and a separate librdkafka consumer in `read_committed` mode supplies the expected records, so the expected values never come from Mink's own code.

Tests:

- `mink-kafka` `batch::tests::fixtures_parse_to_exactly_their_records`: the batch format, codecs, timestamps and invariant 5, with exact equality for every case without aborted transactions.
- `mink-kafka` `batch::tests::encoded_batches_parse_to_their_records_with_every_codec`: every codec against batches written by the `kafka-protocol` encoder.
- `mink-kafka` `batch::tests::log_append_time_gives_every_record_the_batch_max_timestamp`: the log append time rule.
- `mink-kafka` `batch::tests::corrupt_batches_are_errors`: invariant 4. Covers a flipped byte, an old magic, a short length, an unknown codec, bytes that fail to decompress, a record count that disagrees with the records, a negative `last_offset_delta`, and a record past the batch's last offset.
- `mink-kafka` `batch::tests::a_truncated_last_batch_is_dropped` and `batch::tests::fewer_bytes_than_a_batch_prefix_are_no_batches`: parsing step 2.
- `mink-kafka` `batch::tests::a_header_key_that_is_not_utf8_is_an_error`: the `invalid_header_key` rule.
- `mink-kafka` `batch::tests::control_batches_of_other_types_keep_their_offsets_and_type`: the control batch rule.
- `mink-kafka` `transaction::tests::fixtures_keep_exactly_the_committed_records`: invariants 1 and 3.
- `mink-kafka` `transaction::tests::aborted_records_are_dropped_and_committed_ones_kept` and `transaction::tests::markers_never_become_records`: the committed records rule.
- `mink-kafka` `transaction::tests::committed_records_do_not_depend_on_the_fetch_start`: invariant 2, a property over start offsets and response splits.
- `mink-iceberg` `rows::tests::rows_follow_the_records` and `rows::tests::a_timestamp_that_overflows_microseconds_is_an_error`: the row mapping.
- `mink-conformance` `kafka_reads_match_a_read_committed_consumer`: invariant 1 against a real consumer.
