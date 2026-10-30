# Schema

Every table is an Iceberg schema. A topic whose records carry no registry schema keeps the binary schema. A topic whose keys or values are written with registry schemas gets typed table schemas, which grow as producers register new schema versions. Conformance fixtures in `../conformance/fixtures/schema/` and `../conformance/fixtures/payload/` pin them.

## Binary schema

Schema id `0`.

| Field id | Name | Type | Required |
| --- | --- | --- | --- |
| 1 | `_kafka_partition` | `int` | yes |
| 2 | `_kafka_offset` | `long` | yes |
| 3 | `_kafka_timestamp` | `timestamptz` (microseconds) | yes |
| 4 | `_kafka_key` | `binary` | no |
| 5 | `_kafka_headers` | `list<struct>`, element id 6 | yes |
| 7 | `_kafka_headers.element.key` | `string` | yes |
| 8 | `_kafka_headers.element.value` | `binary` | no |
| 9 | `_kafka_value` | `binary` | no |

The list element (id 6) is required: a header is never null, but its value can be. How each column is filled from a Kafka record is in `records.md`.

## Typed schemas

A topic's typed schemas have ids 1, 2, 3 and so on, in the order the topic merges them (Evolution). Each holds:

| Field id | Name | Type | Required |
| --- | --- | --- | --- |
| 1 to 9 | The binary schema's columns | As above | As above |
| from 10 | `_kafka_decoded_key` | The key schema's type (Mappings) | no |
| from 10 | One column per top-level field of the value schema | The field's type | no |

`_kafka_decoded_key` exists once the topic has merged a key schema; value columns once it has merged a value schema. `_kafka_key` and `_kafka_value` keep their meaning: they hold the raw bytes of a part that did not decode (`decoding.md`). The reserved names are these seven columns, and the names Iceberg reserves for its metadata columns: `_file`, `_pos`, `_deleted`, `_spec_id`, `_partition`, `_row_id` and `_last_updated_sequence_number`.

A value schema that is not a record, message or object maps to a single column named `value`. Every decoded column, struct field, list element and map value is optional; map keys are required, as Iceberg needs.

## Mappings

Each format maps to Iceberg types as below. Anything a table does not name becomes a `string` column holding the value as JSON (`decoding.md` Values), so nothing a producer writes is lost.

### Avro

| Avro | Iceberg |
| --- | --- |
| `boolean`, `int`, `long`, `float`, `double` | `boolean`, `int`, `long`, `float`, `double` |
| `bytes`, `string` | `binary`, `string` |
| `record` | `struct`, fields in schema order |
| `enum` | `string` |
| `array`, `map` | `list`, `map<string, …>` |
| `fixed` of size `n` | `fixed[n]` |
| Union of `null` and one other type | That type, optional |
| `decimal(p, s)` on `bytes` or `fixed`, `p <= 38` | `decimal(p, s)` |
| `uuid` | `uuid` |
| `date` | `date` |
| `time-millis`, `time-micros` | `time` |
| `timestamp-millis`, `timestamp-micros` | `timestamptz` |
| `local-timestamp-millis`, `local-timestamp-micros` | `timestamp` |
| `timestamp-nanos`, `local-timestamp-nanos` | `long`, nanoseconds |
| `duration` | `fixed[12]` |
| Any other union, `null` alone, `decimal` with `p > 38`, and a record's reference to itself or to a record that contains it | `string` holding JSON |

A field's identity is its name; a field whose `aliases` name an existing field of the same struct takes that field's identity.

### Protobuf

| Protobuf | Iceberg |
| --- | --- |
| `bool`, `float`, `double`, `string`, `bytes` | `boolean`, `float`, `double`, `string`, `binary` |
| `int32`, `sint32`, `sfixed32` | `int` |
| `int64`, `sint64`, `sfixed64`, `uint32`, `fixed32` | `long` |
| `uint64`, `fixed64` | `decimal(20, 0)` |
| `enum` | `string` |
| message | `struct`, fields in field number order |
| `repeated` | `list` |
| `map<K, V>` | `map<K', V'>` |
| `oneof` | Each member is its own field |
| `google.protobuf.Timestamp` | `timestamptz` |
| `google.protobuf.BoolValue`, `Int32Value`, `Int64Value`, `UInt32Value`, `UInt64Value`, `FloatValue`, `DoubleValue`, `StringValue`, `BytesValue` | The wrapped type |
| `google.protobuf.Any`, `Struct`, `Value`, `ListValue`, `Duration`, a message that contains itself | `string` holding JSON |

A schema maps through its first message, the one the message indexes `[0]` name; records written with another message of the schema are undecodable (`decoding.md`). A field's identity is its field number. Unknown fields and extensions are dropped. A scalar field without explicit presence that is not set holds its default, as Protobuf defines; a message field, an `optional` field or a `oneof` member that is not set is null.

### JSON Schema

| JSON Schema | Iceberg |
| --- | --- |
| `boolean`, `string` | `boolean`, `string` |
| `integer`, `number` | `long`, `double` |
| `string` with `format` `date-time`, `date`, `uuid` | `timestamptz`, `date`, `uuid` |
| `object` with `properties` | `struct`, properties in schema order |
| `object` with only an `additionalProperties` schema | `map<string, …>` |
| `array` with one `items` schema | `list` |
| `enum` of strings | `string` |
| `type` of one type and `null` | That type, optional |
| `$ref` to a definition in the schema or a referenced schema | The definition's type |
| `oneOf`, `anyOf`, `allOf`, `not`, `if`, tuple `items` or `prefixItems`, several types, no `type` (other than an object with `properties` or an `enum` of strings), an object with neither `properties` nor an `additionalProperties` schema, and a `$ref` back to a definition that contains it | `string` holding JSON |

A field's identity is its property name.

## Field ids

Every field of a typed schema, at every depth, has a field id the topic never gives to another field. A field keeps its id across schema versions as long as its identity path, the identities of the fields from the top of the part down to it, keeps the same Iceberg type or a type it can be promoted to. The ids are kept in Postgres, and new ids continue from the highest the topic has used. New fields get their ids in Iceberg's fresh-id order: the new fields of a struct first, in order, then the nested types of each of them in order, a list giving its element an id and a map its key then its value.

An identity path is the identities from the top of the part down to the field, joined by `.`; a list's element is `element`, a map's key and value are `key` and `value`. The part's root is the empty path: `_kafka_decoded_key` for a key, `value` for a value schema that is not a struct.

## Evolution

A topic merges a registry schema the first time a record that names it is indexed or read into a tail. Merging a schema of a part adds its mapping to the topic's latest table schema:

1. A field whose identity path is known and whose type is unchanged, or narrower than its column's by a promotion (an `int` written into a `long` column), keeps its column.
2. A field whose type is a promotion of its column's type, `int` to `long`, `float` to `double`, or `decimal(p, s)` to `decimal(p', s)` with `p' > p`, promotes the column and keeps its id.
3. A field whose type changed in any other way gets a new id and takes the column's name; the old column stays, renamed `{name}_s{N}` where `N` is the last table schema it was current in. Old files still read their values under the new name.
4. A field with a new identity path is added, optional, at the end of its struct.
5. A column whose field the new schema no longer has stays; records of the new schema leave it null.
6. If anything changed, the result is the topic's next table schema and becomes current; otherwise the registry schema maps into the current one.

A schema id is rejected, and its parts are undecodable, when the topic cannot merge its schema:

| Rejection | When |
| --- | --- |
| `missing` | The registry does not have the schema: a Confluent id or GUID it does not know, a schema a Confluent schema references, or a Glue version that is unknown, not available, or has no definition |
| `not_accepted` | The schema is not the part's. For a Confluent registry, it is not registered under the subject `<topic>-key` or `<topic>-value`. For Glue, it is not a version of the schema named after the topic in the configured registry. |
| `unsupported` | Mink cannot read the schema: a malformed definition, a format this version does not decode, a mapping of more than 10,000 fields or nested deeper than 64 levels, or a merged schema Iceberg rejects, such as a field named `a.b` beside a struct `a` with a field `b` |
| `reserved_name` | The schema would add a top-level value column with a reserved name |
| `other_registry` | The schema id is the other registry type's: a Glue version with a Confluent registry, or a Confluent id or GUID with Glue |

Columns are never dropped, so every file the topic ever wrote reads under the current schema. A topic merges each registry schema once, and two merges racing on one topic are serialized by Postgres.

## Errors

| Error | When |
| --- | --- |
| `unknown_table_schema` | A file names a table schema id its topic does not have |

## Invariants

1. The binary schema never changes, and every typed schema starts with it, field ids 1 to 9 included. A file written with schema 0 reads the same forever.
2. A field id is never given to two fields of one topic, and a field's id never changes type except by promotion.
3. A topic's table schemas only grow: each holds every column of the one before it, under the same id.
4. Merging a registry schema that is already merged changes nothing.

## Conformance

- `conformance/fixtures/schema/binary.json`: the binary schema in Iceberg's JSON form.
- `conformance/fixtures/payload/{avro,protobuf,json}/cases.json`: each case's sequence of registry schemas and the table schema, in Iceberg's JSON form, after merging each one, or the rejection, every rejection included.
- `mink-schema` `binary::tests::binary_schema_matches_the_fixture`: the binary schema and invariant 1.
- `mink-iceberg` `payload::avro_fixtures_merge_to_their_table_schemas`, `payload::protobuf_fixtures_merge_to_their_table_schemas` and `payload::json_fixtures_merge_to_their_table_schemas`: Mappings, Field ids and Evolution.
- `mink-schema` `merge::tests::incompatible_changes_keep_the_old_column_under_a_new_name`: Evolution rule 3.
- `mink-schema` `merge::tests::reserved_names_reject_the_schema` and `merge::tests::names_iceberg_cannot_tell_apart_reject_the_schema`: rejections.
- `mink-schema` `avro::tests::schemas_past_the_limits_are_rejected`: the field limit of `unsupported`.
- `mink-schema` `merge::tests::schemas_only_grow_and_ids_are_never_reused`: invariants 2 to 4, over generated sequences of schema versions.
- `mink-store` `schemas::tests::racing_merges_number_schemas_without_gaps`: serialized merges.
- `mink-conformance` `engines_read_typed_columns_across_schema_versions`: DuckDB and PyIceberg read the typed columns of files written under three table schemas, with older rows null for newer fields, an undecodable record kept in `_kafka_value`, and a record naming the first schema by GUID that adds no table schema.
