# Tables

A table is a Kafka topic added with `mink table add`. It has the topic's name in each of three namespaces. Its schema grows as producers register new schema versions.

## Namespaces

<div class="mink-diagram">
<svg viewBox="0 0 570 170" width="570" role="img" aria-label="A partition reads left to right in offsets: the external table holds the tiered history, then Mink's files and the tail hold the records from Kafka. The lake namespace is the external table, the stream namespace is Mink's files and the tail, and the merged namespace is both.">
  <defs>
    <marker id="namespaces-offsets" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="285" y="22" text-anchor="middle" class="label">merged</text>
  <path d="M30 34 L30 28 L540 28 L540 34" class="edge"/>
  <rect x="30" y="42" width="260" height="40" class="edge-soft"/>
  <text x="160" y="67" text-anchor="middle" class="label">external table</text>
  <rect x="290" y="42" width="180" height="40" class="box"/>
  <text x="380" y="67" text-anchor="middle" class="label">Mink's files</text>
  <rect x="470" y="42" width="70" height="40" class="box-accent"/>
  <text x="505" y="67" text-anchor="middle" class="sub">tail</text>
  <path d="M30 90 L30 96 L288 96 L288 90" class="edge"/>
  <text x="160" y="114" text-anchor="middle" class="label">lake</text>
  <path d="M292 90 L292 96 L540 96 L540 90" class="edge"/>
  <text x="416" y="114" text-anchor="middle" class="label">stream</text>
  <path d="M30 142 L536 142" class="edge" marker-end="url(#namespaces-offsets)"/>
  <text x="30" y="162" class="sub">offsets</text>
</svg>
</div>

| Namespace | Rows | Real time | Time travel |
| --- | --- | --- | --- |
| `merged` | Every record once: the [tiered](/docs/design/tiering) history, then the records from Kafka | Yes | Every retained snapshot |
| `stream` | The records from Kafka, from the tier boundary on | Yes | The current snapshot |
| `lake` | The tiered history in the external table | Moves with each tier commit | The current snapshot |

- Every table is in all three. Without an external table, `stream` holds what `merged` holds and `lake` is empty.
- `stream` and `lake` load separately, so a tier commit between the two loads moves the boundary. `merged` is the one consistent read of both.

```sql
SELECT count(*) FROM mink.merged.orders;
SELECT count(*) FROM mink.stream.orders;
SELECT count(*) FROM mink.lake.orders;
```

## Columns

Every table starts with these columns, in this order.

| Column | Type | Value |
| --- | --- | --- |
| `_kafka_partition` | `int` | The Kafka partition |
| `_kafka_offset` | `long` | The record's offset |
| `_kafka_timestamp` | `timestamptz` | The record's timestamp, in microseconds |
| `_kafka_key` | `binary` | The raw key, or null when the key is null or decoded |
| `_kafka_headers` | `list<struct<key: string, value: binary>>` | Every header in order, repeats kept |
| `_kafka_value` | `binary` | The raw value, or null when the value is null or decoded |

A topic written with Schema Registry schemas gets more columns after these.

| Column | Type | Value |
| --- | --- | --- |
| `_kafka_decoded_key` | The key schema's type | The decoded key |
| One per top-level field of the value schema | The field's type | The field's value |
| `value` | The value schema's type | The decoded value, when the value schema is not a record, message or object |

## Schema Registry

Mink reads the schema id the way Confluent's and AWS Glue's serializers write it.

| Format | Where |
| --- | --- |
| Header | `__key_schema_id` or `__value_schema_id`, holding byte `1` and the schema's GUID. The header wins over a prefix. |
| Glue prefix | Byte `3`, the compression byte (`0` none, `5` zlib), then the schema version UUID as 16 bytes, then the payload |
| Confluent prefix | Byte `0`, then the schema id as 4 bytes, then the payload |

A part without a schema id, with a schema Mink cannot read, or with bytes that do not decode keeps its raw bytes in `_kafka_key` or `_kafka_value`, and its typed columns are null.

Without a `schema_registry` setting, new records keep their raw bytes.

### Avro

| Avro | Iceberg |
| --- | --- |
| `boolean`, `int`, `long`, `float`, `double` | Same |
| `bytes`, `string` | `binary`, `string` |
| `record` | `struct` |
| `enum` | `string` |
| `array`, `map` | `list`, `map<string, …>` |
| `fixed` of size `n` | `fixed[n]` |
| Union of `null` and one type | That type, optional |
| `decimal(p, s)`, `p <= 38` | `decimal(p, s)` |
| `uuid`, `date` | `uuid`, `date` |
| `time-millis`, `time-micros` | `time` |
| `timestamp-millis`, `timestamp-micros` | `timestamptz` |
| `local-timestamp-millis`, `local-timestamp-micros` | `timestamp` |
| `timestamp-nanos`, `local-timestamp-nanos` | `long`, nanoseconds |
| `duration` | `fixed[12]` |
| Other unions, `decimal` with `p > 38`, recursive records | `string`, as JSON |

### Protobuf

| Protobuf | Iceberg |
| --- | --- |
| `bool`, `float`, `double`, `string`, `bytes` | `boolean`, `float`, `double`, `string`, `binary` |
| `int32`, `sint32`, `sfixed32` | `int` |
| `int64`, `sint64`, `sfixed64`, `uint32`, `fixed32` | `long` |
| `uint64`, `fixed64` | `decimal(20, 0)` |
| `enum` | `string` |
| message | `struct` |
| `repeated`, `map<K, V>` | `list`, `map` |
| `oneof` | One field per member |
| `google.protobuf.Timestamp` | `timestamptz` |
| Wrapper types such as `Int64Value` | The wrapped type |
| `Any`, `Struct`, `Value`, `ListValue`, `Duration`, recursive messages | `string`, as JSON |

The schema's first message is the table's. Records written with another message of the schema keep their raw bytes.

### JSON Schema

| JSON Schema | Iceberg |
| --- | --- |
| `boolean`, `string` | `boolean`, `string` |
| `integer`, `number` | `long`, `double` |
| `string` with `format` `date-time`, `date`, `uuid` | `timestamptz`, `date`, `uuid` |
| `object` with `properties` | `struct` |
| `object` with only `additionalProperties` | `map<string, …>` |
| `array` with one `items` schema | `list` |
| `enum` of strings | `string` |
| One type and `null` | That type, optional |
| `$ref` | The definition's type |
| `oneOf`, `anyOf`, `allOf`, several types, no `type`, recursive definitions | `string`, as JSON |

## Schema evolution

A new schema version is merged into the table the first time a record uses it. Columns are never dropped, so every file reads under the current schema.

| Change | Result |
| --- | --- |
| Field unchanged | Same column |
| `int` to `long`, `float` to `double`, wider `decimal` precision | The column is promoted |
| Any other type change | A new column takes the name. The old column stays as `{name}_s{N}`, where `N` is the last schema it was current in. |
| New field | A new optional column at the end of its struct |
| Field removed | The column stays, null for new records |

A schema Mink cannot merge leaves its records with their raw bytes, for example one with a field named after a Mink column or a column Iceberg reserves. The exact rules are in [Schemas](/docs/formats/schema) and [Data files](/docs/formats/data#values).

## States

| State | Meaning |
| --- | --- |
| `indexing` | Indexed, committed and served |
| `missing` | Kafka no longer has the topic. Back to `indexing` when the topic returns. |
| `replaced` | The topic was deleted and created again. Remove the table and add it again. |
| `removing` | Removed. Its objects and rows are deleted once its leases expire. |

Only `indexing` tables are listed by the catalog.
