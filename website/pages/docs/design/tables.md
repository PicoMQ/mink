# Tables

A table is a schema, an optional primary key, an optional partition key list, a bucketing rule and a set of options. Tables live in databases and are addressed as `database.table`. A table descriptor is JSON and is stored in the metadata log. `mink table describe` prints it and `mink table create --descriptor` accepts it.

## Layout

<div class="mink-diagram">
<svg viewBox="0 0 640 260" width="640" role="img" aria-label="A table splits into partitions, each partition into buckets, and each bucket is one stream in object storage.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="100" width="130" height="56" class="box-accent"/>
  <text x="85" y="124" text-anchor="middle" class="label">shop.orders</text>
  <text x="85" y="142" text-anchor="middle" class="sub">table</text>
  <rect x="210" y="40" width="130" height="56" class="box"/>
  <text x="275" y="64" text-anchor="middle" class="label">region=us</text>
  <text x="275" y="82" text-anchor="middle" class="sub">partition</text>
  <rect x="210" y="160" width="130" height="56" class="box"/>
  <text x="275" y="184" text-anchor="middle" class="label">region=eu</text>
  <text x="275" y="202" text-anchor="middle" class="sub">partition</text>
  <rect x="400" y="20" width="100" height="40" class="box"/>
  <text x="450" y="45" text-anchor="middle" class="label">bucket 0</text>
  <rect x="400" y="76" width="100" height="40" class="box"/>
  <text x="450" y="101" text-anchor="middle" class="label">bucket 1</text>
  <rect x="400" y="140" width="100" height="40" class="box"/>
  <text x="450" y="165" text-anchor="middle" class="label">bucket 0</text>
  <rect x="400" y="196" width="100" height="40" class="box"/>
  <text x="450" y="221" text-anchor="middle" class="label">bucket 1</text>
  <rect x="540" y="20" width="80" height="40" class="box"/>
  <text x="580" y="45" text-anchor="middle" class="sub">stream</text>
  <rect x="540" y="76" width="80" height="40" class="box"/>
  <text x="580" y="101" text-anchor="middle" class="sub">stream</text>
  <rect x="540" y="140" width="80" height="40" class="box"/>
  <text x="580" y="165" text-anchor="middle" class="sub">stream</text>
  <rect x="540" y="196" width="80" height="40" class="box"/>
  <text x="580" y="221" text-anchor="middle" class="sub">stream</text>
  <path d="M150 120 L202 76" class="edge" marker-end="url(#arr)"/>
  <path d="M150 136 L202 180" class="edge" marker-end="url(#arr)"/>
  <path d="M340 60 L392 44" class="edge" marker-end="url(#arr)"/>
  <path d="M340 76 L392 92" class="edge" marker-end="url(#arr)"/>
  <path d="M340 180 L392 164" class="edge" marker-end="url(#arr)"/>
  <path d="M340 196 L392 212" class="edge" marker-end="url(#arr)"/>
  <path d="M500 40 L532 40" class="edge" marker-end="url(#arr)"/>
  <path d="M500 96 L532 96" class="edge" marker-end="url(#arr)"/>
  <path d="M500 160 L532 160" class="edge" marker-end="url(#arr)"/>
  <path d="M500 216 L532 216" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

- A **partition** is a value of the partition key columns, for example `region=us`. Partitions are created explicitly, on first write, or by the auto-partition scheduler. Each partition has its own `bucket_count` buckets.
- A **bucket** is one ordered log with its own offsets and, for primary-key tables, its own KV tablet. It is the unit of leadership, parallelism and tiering.
- A **stream** is the s3stream object behind a bucket.

## Schema

| Rule | Detail |
| --- | --- |
| Columns | Name, [logical type](#types), nullable by default, optional aggregate function |
| Primary key | One or more columns, forced non-nullable, no aggregate on a key column |
| Partition keys | Distinct columns of a predefined scalar type other than `DECIMAL`. Must be part of the primary key when one exists |
| Bucket keys | Distinct columns, not partition keys. Must be a subset of the primary key when one exists. Empty bucket keys on a primary-key table means the primary key minus the partition keys |
| Auto increment | At most one `INT` or `BIGINT` column, requires a primary key, not part of it |
| Field ids | Every column has a stable id. Ids survive rename and are carried into Arrow and Parquet metadata |

## Types

| Logical | Arrow | Notes |
| --- | --- | --- |
| `BOOLEAN` | Boolean | |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | Int8, Int16, Int32, Int64 | |
| `FLOAT`, `DOUBLE` | Float32, Float64 | |
| `DECIMAL(p, s)` | Decimal128 | |
| `CHAR(n)`, `STRING` | Utf8 | |
| `BINARY(n)` | FixedSizeBinary | |
| `BYTES` | Binary | |
| `DATE` | Date32 | |
| `TIME(p)` | Time32 or Time64 | Unit by precision: 0 s, 1-3 ms, 4-6 µs, else ns |
| `TIMESTAMP(p)` | Timestamp, no zone | Same precision rule |
| `TIMESTAMP_LTZ(p)` | Timestamp, UTC | Same precision rule |
| `ARRAY<T>` | List | |
| `MAP<K, V>` | Map | |
| `ROW<a T, b U>` | Struct | |

`NOT NULL` follows the type: `id BIGINT NOT NULL`, `ARRAY<INT NOT NULL>`.

## Bucketing

| Rule | Hash | Used when |
| --- | --- | --- |
| Native | Mink's own 32-bit hash of the encoded key, scrambled, modulo `bucket_count` | No lake, or Lance |
| Paimon | Paimon's hash of the encoded key modulo `bucket_count` | `lake = paimon` |
| Iceberg | Iceberg bucket transform on a single key column | `lake = iceberg` |

A table without bucket keys and without a primary key routes each batch to one bucket and moves to the next bucket for the next batch. Bucket rules are chosen at create time from the lake format so that rows land in the same bucket in Mink and in the lake table.

## Options

| Option | Default | Values |
| --- | --- | --- |
| `log_format` | `arrow` | `arrow` |
| `kv_format` | `compacted` | `compacted` |
| `merge_engine` | none, last write wins | `first_row`, `versioned` with a column, `aggregation` |
| `delete_behavior` | `allow`, or `ignore` when a merge engine is set | `allow`, `ignore`, `disable` |
| `changelog_image` | `full` | `full`, `wal` |
| `log_ttl_ms` | 7 days | Duration, or unset for forever |
| `lake` | none | `iceberg`, `paimon`, `lance` |
| `lake_freshness_ms` | 3 minutes | Duration |
| `lake_auto_compaction` | `false` | Bool |
| `lake_attach` | `false` | Bool, attach to an existing lake table instead of creating one |
| `auto_partition` | none | `key`, `time_unit` (`hour`, `day`, `month`, `quarter`, `year`), `time_zone` (UTC), `num_precreate`, `num_retention` (7) |

Constraints: `merge_engine` and `delete_behavior` require a primary key. The `versioned` column must be `INT`, `BIGINT`, `TIMESTAMP` or `TIMESTAMP_LTZ`. `aggregation` cannot be combined with `changelog_image = wal`. `delete_behavior = allow` cannot be combined with `first_row` or `versioned`. The tiering worker writes Iceberg. `paimon` and `lance` select a bucketing rule but have no writer.

Free-form `key=value` properties are stored under `custom` and are not interpreted.

### Merge engines

| Engine | On a second write to the same key |
| --- | --- |
| Default | New row replaces old. Delete removes the key |
| `first_row` | Old row kept, write ignored. Deletes rejected |
| `versioned` | Row with the larger version column wins. Deletes rejected |
| `aggregation` | Non-key columns folded by their aggregate function. A key column is always `last_value` |

Aggregate functions: `sum`, `product`, `max`, `min`, `last_value`, `last_value_ignore_nulls`, `first_value`, `first_value_ignore_nulls`, `list_agg` with a delimiter, `bool_and`, `bool_or`, `rbm32`, `rbm64`. A non-key column without a function defaults to `last_value_ignore_nulls`. Functions are declared per column in the descriptor.

## Alter

| Change | Rule |
| --- | --- |
| Add column | New name, nullable type, new field id |
| Drop column | Not the last column, not a primary key, partition key, bucket key, auto-increment or versioned column |
| Rename column | Not one of the referenced columns above |
| Modify column | Type promotion only, field id kept. Nullable to non-null is rejected |
| Set option | `table.datalake.enabled` and `table.datalake.freshness`. Any other `table.*` key is rejected, other keys go to `custom` |

Promotions: `TINYINT` → `SMALLINT` → `INT` → `BIGINT`. `FLOAT` → `DOUBLE`. `DECIMAL` to a larger precision at the same scale. `CHAR` to a longer `CHAR` or `STRING`. `BINARY` to a longer `BINARY` or `BYTES`. `TIME`, `TIMESTAMP`, `TIMESTAMP_LTZ` to a higher precision. `ARRAY`, `MAP` values and `ROW` fields recursively.

Every alter produces a new schema version. Record batches carry the schema id they were written with, and readers remap old batches to the current schema, so the log never has to be rewritten.
