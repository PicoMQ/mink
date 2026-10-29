# Indexing

Indexing turns a partition's committed records into data files with exact statistics. Each partition has one indexer at a time. Records are read once and not kept: Postgres gets each file's offsets, statistics and Parquet footer, and nothing is written to S3.

<div class="mink-diagram">
<svg viewBox="0 0 660 200" width="660" role="img" aria-label="A partition's indexed range runs from its start offset to its end offset as data files. Backfill extends it backwards towards Kafka's earliest offset. Live indexing extends it forwards towards the last stable offset.">
  <defs>
    <marker id="indexing-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="40" y="50" width="120" height="44" class="edge-soft"/>
  <text x="100" y="77" text-anchor="middle" class="sub">history</text>
  <rect x="160" y="50" width="60" height="44" class="box"/>
  <rect x="220" y="50" width="60" height="44" class="box"/>
  <rect x="280" y="50" width="60" height="44" class="box"/>
  <rect x="340" y="50" width="60" height="44" class="box"/>
  <rect x="400" y="50" width="60" height="44" class="box"/>
  <rect x="460" y="50" width="60" height="44" class="box"/>
  <text x="340" y="38" text-anchor="middle" class="label">indexed range, as data files</text>
  <rect x="520" y="50" width="100" height="44" class="box-accent"/>
  <text x="570" y="77" text-anchor="middle" class="sub">in memory</text>
  <text x="40" y="114" text-anchor="middle" class="sub">earliest</text>
  <text x="160" y="114" text-anchor="middle" class="sub">start</text>
  <text x="520" y="114" text-anchor="middle" class="sub">end</text>
  <text x="620" y="114" text-anchor="middle" class="sub">last stable</text>
  <path d="M156 150 L44 150" class="edge" marker-end="url(#indexing-arrow)"/>
  <path d="M524 150 L616 150" class="edge" marker-end="url(#indexing-arrow)"/>
  <text x="100" y="172" text-anchor="middle" class="sub">backfill</text>
  <text x="570" y="172" text-anchor="middle" class="sub">live</text>
</svg>
</div>

## Where an indexer starts

| Partition | Start |
| --- | --- |
| Of a newly added table | Its last stable offset. Backfill reads what came before. |
| Already indexed | The end of its stored range |
| Added to the topic later | Its last stable offset |

## Data files

An indexer groups records into offset ranges in offset order. A range ends at the first of these:

| Trigger | Setting | Default |
| --- | --- | --- |
| Key, value and header bytes reach | `file_bytes` | 128 MiB |
| Record timestamps span | `file_window` | `60s` |
| The range's first record was read this long ago | `file_window` | `60s` |
| A record falls in a later time bucket | `granularity` | `hour` |

A range becomes one data file per time bucket among its records. Offsets with no committed record, such as aborted records and transaction markers, belong to the file before them.

Each file is built once in memory to take its statistics: row count, size, and per-column min/max, null and NaN counts. The bytes are then dropped. Engines get the same bytes later, built again from Kafka on read.

## Backfill

Backfill indexes history backwards from the range's start, `backfill_offsets` at a time, 100,000 by default. Live indexing never waits for it.

| `history` | Backfill goes down to |
| --- | --- |
| `all`, the default | Kafka's earliest offset |
| `{ since = <RFC 3339 time> }` | The first offset at or after that time |
| `none` | Nothing. The table starts at the live end. |

## Schemas

Before writing files, the indexer merges every new schema its records name into the table schema. While the Schema Registry cannot be reached, the indexer waits instead of writing files.

## Kafka retention

Every `retention_check`, 60 seconds by default, the indexer reads Kafka's earliest offset. When retention has deleted records, the range's start moves up to the next file boundary. The records between leave the table with their file, at most one file per partition.

## Failures

| Event | Effect |
| --- | --- |
| Indexer stops | Records read but not written are only in memory. The next indexer reads again from the stored end. |
| Node loses the lease | The indexer stops at once. Another node takes the partition when the lease expires, after `lease_ttl`, 30 seconds by default. |
| Lost lease, late write | Rejected by Postgres. An indexer without its lease adds no file. |
