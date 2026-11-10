# Data files

A data file is a Parquet file built from Kafka. Its path names everything it is built from, so every build gives the same bytes.

## Path

```
{warehouse}/{topic}/data/p={index}/{start}-{end}.t{spec}[_{bucket}].s{schema}[.a{assembly}].v{version}.parquet
```

| Part | Value |
| --- | --- |
| `p={index}` | The Kafka partition |
| `{start}-{end}` | The offsets `[start, end)` |
| `t{spec}` | The time spec number, see [Partition specs](/docs/formats/metadata#partition-specs) |
| `_{bucket}` | The time bucket, present exactly when the spec has a time field. It can be negative. |
| `s{schema}` | The schema id, see [Schemas](/docs/formats/schema) |
| `.a{assembly}` | Assembled files only: the first 16 lowercase hex digits of the SHA-256 of the parts, one `start:end` line each in offset order |
| `v{version}` | The file version, `1` |

- Numbers are base 10, without padding or leading zeros, and only the bucket has a sign.
- A path parses only if it renders back to the same path, so a file has exactly one path.

```
s3://mink/kafka/orders.v2/data/p=3/1200000-1400000.t0_499260.s2.v1.parquet
```

## File version 1

| Setting | Value |
| --- | --- |
| Columns | The file's schema, each Parquet field with its Iceberg field id |
| Rows | One per committed record, in offset order, valued as [Values](#values) says |
| Compression | ZSTD, level 3 |
| Row groups | At most 131,072 rows each |
| Dictionary encoding | On |
| Statistics | Parquet column chunk statistics |
| Page index | None |
| Writer version | Parquet 1.0 |
| `created_by` | `mink file version 1` |
| Key-value metadata | None, no embedded Arrow schema |
| Everything else | The defaults of the `parquet` crate in `Cargo.lock` |

A `parquet` crate upgrade that changes any byte is file version 2, not new fixtures.

## Bytes

<div class="mink-diagram">
<svg viewBox="0 0 660 214" width="660" role="img" aria-label="A file the indexer wrote is PAR1, its row groups, zero padding, the footer, the footer's length and PAR1. An assembled file is PAR1, zeros up to byte 4096, each part's row groups at a multiple of 4096 with zeros between, then padding, the merged footer, its length and PAR1. Engines' first reads start inside the padding.">
  <text x="30" y="22" class="sub">written by an indexer</text>
  <rect x="30" y="32" width="44" height="40" class="box-accent"/>
  <text x="52" y="56" text-anchor="middle" class="sub">PAR1</text>
  <rect x="74" y="32" width="236" height="40" class="box"/>
  <text x="192" y="56" text-anchor="middle" class="label">row groups</text>
  <rect x="310" y="32" width="140" height="40" class="edge-soft"/>
  <text x="380" y="56" text-anchor="middle" class="sub">padding p(U)</text>
  <rect x="450" y="32" width="118" height="40" class="box"/>
  <text x="509" y="56" text-anchor="middle" class="label">footer</text>
  <rect x="568" y="32" width="62" height="40" class="box-accent"/>
  <text x="599" y="56" text-anchor="middle" class="sub">len PAR1</text>
  <text x="30" y="110" class="sub">assembled</text>
  <rect x="30" y="120" width="44" height="40" class="box-accent"/>
  <text x="52" y="144" text-anchor="middle" class="sub">PAR1</text>
  <rect x="74" y="120" width="26" height="40" class="edge-soft"/>
  <rect x="100" y="120" width="90" height="40" class="box"/>
  <text x="145" y="144" text-anchor="middle" class="label">part 1</text>
  <rect x="190" y="120" width="22" height="40" class="edge-soft"/>
  <rect x="212" y="120" width="98" height="40" class="box"/>
  <text x="261" y="144" text-anchor="middle" class="label">part 2</text>
  <rect x="310" y="120" width="140" height="40" class="edge-soft"/>
  <text x="380" y="144" text-anchor="middle" class="sub">padding p(U)</text>
  <rect x="450" y="120" width="118" height="40" class="box"/>
  <text x="509" y="144" text-anchor="middle" class="label">merged footer</text>
  <rect x="568" y="120" width="62" height="40" class="box-accent"/>
  <text x="599" y="144" text-anchor="middle" class="sub">len PAR1</text>
  <text x="100" y="178" class="sub">at 4096</text>
  <text x="212" y="178" class="sub">next 4096</text>
  <text x="380" y="200" text-anchor="middle" class="sub">first reads start here</text>
</svg>
</div>

- The padding is zeros, `p(U) = max(65,536, 2^⌈log₂(U / 512)⌉) + 4,096` bytes for a file of `U` bytes without it.
- Every engine's first read lands in the padding, so it reads only zeros and the footer.

| Engine | First read of a file of `L` bytes |
| --- | --- |
| PyIceberg 0.12, through Arrow | The last 65,536 bytes |
| Trino 484 | The last 49,152 bytes |
| DuckDB 2.0 | The last `max(16,384, 2^⌈log₂(L / 1,024)⌉)` bytes, its start rounded down to a multiple of 4,096 |

An assembled file joins the files of one partition, bucket, spec and schema, see [Assembly](/docs/design/layout#assembly):

- Each part is the row groups of its file, built from the same records. The first starts at byte 4,096 and each next one at the following multiple of 4,096, with zeros between.
- The merged footer is the parts' `FileMetaData`, with their row groups in part order, each `ordinal` its position, `num_rows` the sum, and each row group's `file_offset` and each column chunk's `data_page_offset` and `dictionary_page_offset` moved to where its part starts. A column chunk's own `file_offset` is 0.

## Values

Columns hold what [Columns](/docs/design/tables#columns) says. A decoded column is null when its part is null or does not decode, or when the record's schema lacks it.

| Value | Stored as |
| --- | --- |
| `_kafka_timestamp` | The record's milliseconds times 1,000 |
| A header without a value | A null `value` |
| `timestamptz`, `timestamp` | Microseconds since the epoch |
| `time` | Microseconds since midnight |
| `decimal(p, s)` | The unscaled integer at scale `s` |
| `string` from an enum | The symbol's name, or a Protobuf enum number without a name in base 10 |
| `map` from Protobuf or JSON Schema | Entries sorted by key |
| `string` holding JSON | Compact JSON. Avro keeps schema order, Protobuf and JSON Schema sort members by key. |
