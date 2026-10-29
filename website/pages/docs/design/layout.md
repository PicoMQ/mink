# Layout and pruning

A table's data files are partitioned by time and carry min/max bounds per column. Engines use both to skip files a filter rules out. Once an hour has ended, its small files are assembled into larger ones.

## Time partitioning

| Granularity | Partition fields |
| --- | --- |
| `hour`, the default | `_kafka_partition`, `_kafka_timestamp_hour` |
| `day` | `_kafka_partition`, `_kafka_timestamp_day` |
| `month` | `_kafka_partition`, `_kafka_timestamp_month` |
| `none` | `_kafka_partition` |

Buckets are UTC hours, days or months of `_kafka_timestamp`. Set a table's granularity with `mink table add <topic> --granularity day`, or change it later with `mink granularity <topic> <granularity>`. Files already written keep their granularity, and new files use the new one.

## Statistics

Every data file records, per column:

| Statistic | Columns |
| --- | --- |
| Min and max | `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, and every typed primitive column, except `binary`, `fixed` and columns inside lists and maps. String bounds keep their first 16 characters. |
| Null count | Every leaf column |
| NaN count | Every `float` and `double` leaf column |

The Parquet footer also holds min and max per row group. Each table also publishes distinct-value estimates per column for cost-based planners. They do not skip files.

## What prunes

| Filter | Skips |
| --- | --- |
| `_kafka_timestamp` range | Time buckets and files outside the range |
| `_kafka_partition = p` | Files of other partitions |
| `_kafka_offset` range | Files outside the range |
| A typed column, such as `id = 1234` | Files whose min and max exclude the value. That needs values that grow with offset or time, such as sequential ids or event times. |
| Raw `_kafka_key` or `_kafka_value`, or JSON decoded from them | Nothing. Binary columns carry no bounds. |

Measured on a table of 3,600 records in 36 files, 3 partitions, 100 consecutive ids per file, before assembly:

| Query | Files read |
| --- | --- |
| `WHERE id = 1234` | 1 of 36 |
| `WHERE id BETWEEN 2000 AND 2400` | 5 of 36 |
| `WHERE _kafka_partition = 1 AND id < 600` | 2 of 36 |
| `WHERE _kafka_offset BETWEEN 1000 AND 1050` | 3 of 36 |
| `WHERE amount > 990`, amounts scattered | 12 of 36 |

The console's SQL page shows these numbers with Profile. See [Console](/docs/operations/console).

## Assembly

A time bucket's files are assembled once the bucket ended `assembly_delay` ago, 10 minutes by default. For each partition, consecutive files are joined into one file of at most `assembled_bytes`, 1 GiB by default. Assembly reads no Kafka records: the new file's row groups are the old files' row groups, byte for byte.

<div class="mink-diagram">
<svg viewBox="0 0 660 230" width="660" role="img" aria-label="Before assembly, one hour of a partition is many small data files. After the hour ends, they become one assembled file whose row groups are the original files.">
  <defs>
    <marker id="layout-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="20" y="56" class="sub">hour open</text>
  <rect x="140" y="34" width="80" height="36" class="box"/>
  <rect x="230" y="34" width="80" height="36" class="box"/>
  <rect x="320" y="34" width="80" height="36" class="box"/>
  <rect x="410" y="34" width="80" height="36" class="box"/>
  <rect x="500" y="34" width="80" height="36" class="box"/>
  <text x="180" y="56" text-anchor="middle" class="sub">file</text>
  <text x="270" y="56" text-anchor="middle" class="sub">file</text>
  <text x="360" y="56" text-anchor="middle" class="sub">file</text>
  <text x="450" y="56" text-anchor="middle" class="sub">file</text>
  <text x="540" y="56" text-anchor="middle" class="sub">file</text>
  <path d="M180 78 L180 86 L540 86 L540 78" class="edge-soft"/>
  <path d="M360 86 L360 140" class="edge" marker-end="url(#layout-arrow)"/>
  <text x="370" y="118" class="sub">hour ended 10m ago</text>
  <text x="20" y="170" class="sub">assembled</text>
  <rect x="140" y="144" width="440" height="52" class="box-accent"/>
  <path d="M228 144 L228 196" class="edge-soft"/>
  <path d="M316 144 L316 196" class="edge-soft"/>
  <path d="M404 144 L404 196" class="edge-soft"/>
  <path d="M492 144 L492 196" class="edge-soft"/>
  <text x="360" y="216" text-anchor="middle" class="sub">one file, one row group per original file</text>
</svg>
</div>

After assembly, a filter on a typed column reads the assembled file. The file keeps each original file's min and max as its row group's statistics, for engines that skip row groups. On the table above, once its hour was assembled:

| Query | Files read |
| --- | --- |
| `WHERE id = 1234` | 3 of 3 |
| `WHERE _kafka_partition = 1` | 1 of 3 |

## Manifests

Each manifest holds the files of one time bucket, so engines skip whole manifests outside a time range. An assembled bucket has one manifest. A bucket still open has at most 16.

The exact rules are in [`layout.md`](https://github.com/PicoMQ/mink/blob/main/specification/layout.md) and [`file.md`](https://github.com/PicoMQ/mink/blob/main/specification/file.md).
