# Decoding

A record's key and value are its two parts. A part written with a schema from the registry decodes into typed columns; any other part keeps its raw bytes. This document pins how Mink finds a part's schema, decodes it, and fills a row of a typed table schema (`schema.md`). Conformance fixtures in `../conformance/fixtures/payload/` pin it.

## Wire formats

A part names its schema in one of two ways, the ones Confluent's serializers write:

| Format | Where | Bytes |
| --- | --- | --- |
| Payload prefix | The part itself | Byte 0 is `0`, bytes 1 to 4 are the schema id (big-endian `u32`), the rest is the encoded payload |
| Header | A record header, `__key_schema_id` for the key or `__value_schema_id` for the value | Byte 0 is `1`, bytes 1 to 16 are the schema GUID, and any bytes after them (a Protobuf schema's message indexes) come before the part, which is the rest of the encoded payload |

The header wins when both are present, as it does for Confluent's deserializers. The last header of that name is the one used. A header whose value is shorter than 17 bytes or does not start with `1` is ignored.

A Protobuf payload starts with the message indexes, which pick the message type inside the schema: a zigzag varint count followed by that many zigzag varint indexes, each the position of a message among the messages of the file, then of the message before it. A single `0` byte stands for `[0]`, the first message of the file.

## Decoding a part

1. A null part has no columns: its raw column is null and its typed columns are null.
2. Find the schema reference: the header GUID, or the payload prefix's id. A part with neither is undecodable.
3. Look up the topic's result for that reference and part (`schema.md` Evolution). A rejected reference leaves the part undecodable. A reference the topic has not merged fails the build with `unmerged_schema`: the indexer and the tail merge first, so only a file no snapshot names can hit it, and a file is never built with a result that could still change.
4. Decode the payload with that schema: Avro binary encoding with the schema as the writer schema, Protobuf with the message the indexes pick, JSON as UTF-8 JSON text. Bytes left over after an Avro or Protobuf payload, malformed bytes, a JSON value that is not valid JSON, a value nested deeper than 64 levels inside a recursive Avro type, a Protobuf timestamp beyond the range of microseconds, or a Protobuf well-known value with no JSON form (an `Any` whose type Mink cannot resolve) make the part undecodable.
5. Put every value of the decoded payload into the column whose field id the merge gave its writer path, so a record keeps filling the column its schema was merged into even after a later schema changes the field's type (`schema.md` Evolution rule 3). A value that does not fit its column's type, such as a JSON number with a fraction for a `long` column or a string that is not a `date`, makes the part undecodable.

An undecodable part is never an error: the row keeps the part's raw bytes and leaves its typed columns null.

## Rows

A record of a topic with a typed table schema is one row of the file's schema (`file.md`):

| Column | Value |
| --- | --- |
| `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_kafka_headers` | As `records.md` Rows says |
| `_kafka_key` | The raw key bytes, or null when the key is null or decoded |
| `_kafka_decoded_key` | The decoded key, or null when the key is null or undecodable |
| `_kafka_value` | The raw value bytes, or null when the value is null or decoded |
| Value columns | The decoded value's fields, each in its column. Null for every column when the value is null or undecodable, and for a column the record's schema does not have |

A decoded field whose column the file's schema does not have is left out.

## Errors

| Error | When |
| --- | --- |
| `unmerged_schema` | A part names a schema reference its topic has not merged |

## Values

| Column type | Value |
| --- | --- |
| Any, for a null value | Null, a JSON column included |
| `string` holding JSON (`schema.md` Mappings) | Compact JSON with no insignificant whitespace: the Avro JSON encoding with members in schema order, the Protobuf JSON mapping with members sorted by key, or the JSON value itself with members sorted by key |
| `map` from Protobuf or JSON Schema | Entries sorted by key |
| `timestamptz`, `timestamp` | Microseconds since the epoch; milliseconds are multiplied by 1000 |
| `time` | Microseconds since midnight |
| `decimal(p, s)` | The unscaled integer of the source value at scale `s` |
| `string` from an enum | The symbol's name. A Protobuf enum number with no name is its decimal form |

## Invariants

1. Decoding a part depends only on its bytes, the record's headers, and stored schemas, so a file rebuilds to the same rows.
2. A part either decodes completely or keeps all of its raw bytes; a row never holds part of a part.
3. Every committed record is a row, decodable or not.

## Conformance

- `conformance/fixtures/payload/{avro,protobuf,json}/cases.json`: for each format, the registered schema versions, records as written by Confluent's serializers (both wire formats, and undecodable parts), and the row each record becomes. The fixtures are generated with Confluent's Python client against the compose registry, never by Mink: from `mink/mink-conformance`, `python -m payload http://localhost:8081 [avro|protobuf|json]`, with `confluent-kafka`, `grpcio-tools` and `protobuf` installed.
- `mink-iceberg` `payload::avro_fixtures_decode_to_their_rows`, `payload::protobuf_fixtures_decode_to_their_rows` and `payload::json_fixtures_decode_to_their_rows`: Decoding a part, Rows and Values.
- `mink-schema` `wire::tests::wire_formats_find_the_schema`: Wire formats, including the header rules and a header's leading bytes.
- `mink-schema` `protobuf::tests::message_indexes_name_the_message`: message indexes.
- `mink-schema` `avro::tests::deep_recursive_payloads_are_undecodable`: the nesting limit of step 4.
- `mink-schema` `value::tests::json_columns_sort_their_keys`: JSON members sorted by key.
- `mink-schema` `decoders::tests::undecodable_parts_keep_their_raw_bytes`: invariants 2 and 3.
- `mink-schema` `decoders::tests::an_unmerged_schema_fails_the_build`: Decoding a part, step 3.
