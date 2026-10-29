# Introduction

Mink is a read-only Iceberg catalog over Kafka. Iceberg engines query topics as tables, and the records stay in Kafka. A Mink node runs next to an existing Kafka cluster, with Postgres for its state and an S3-compatible bucket for the Iceberg metadata.

<div class="mink-diagram">
<svg viewBox="0 0 660 370" width="660" role="img" aria-label="Engines load tables from the REST catalog and read data files from the S3 endpoint. The catalog reads its state from Postgres. The S3 endpoint builds data files from Kafka. Indexers read Kafka, and writers commit Iceberg metadata to the bucket.">
  <defs>
    <marker id="intro-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="10" y="44" width="150" height="134" class="box"/>
  <text x="85" y="106" text-anchor="middle" class="label">engines</text>
  <text x="85" y="124" text-anchor="middle" class="sub">DuckDB, PyIceberg</text>
  <rect x="215" y="14" width="230" height="262" class="edge-soft"/>
  <text x="227" y="32" class="sub">mink node</text>
  <rect x="230" y="44" width="200" height="56" class="box"/>
  <text x="330" y="68" text-anchor="middle" class="label">REST catalog</text>
  <text x="330" y="86" text-anchor="middle" class="sub">tables, snapshots</text>
  <rect x="230" y="122" width="200" height="56" class="box"/>
  <text x="330" y="146" text-anchor="middle" class="label">S3 endpoint</text>
  <text x="330" y="164" text-anchor="middle" class="sub">data files, built on read</text>
  <rect x="230" y="200" width="200" height="56" class="box-accent"/>
  <text x="330" y="224" text-anchor="middle" class="label">indexers and writers</text>
  <text x="330" y="242" text-anchor="middle" class="sub">offsets into snapshots</text>
  <rect x="500" y="44" width="150" height="56" class="box"/>
  <text x="575" y="68" text-anchor="middle" class="label">Postgres</text>
  <text x="575" y="86" text-anchor="middle" class="sub">tables, files, leases</text>
  <rect x="500" y="122" width="150" height="134" class="box"/>
  <text x="575" y="184" text-anchor="middle" class="label">Kafka</text>
  <text x="575" y="202" text-anchor="middle" class="sub">the records</text>
  <rect x="230" y="300" width="200" height="56" class="box"/>
  <text x="330" y="324" text-anchor="middle" class="label">bucket</text>
  <text x="330" y="342" text-anchor="middle" class="sub">Iceberg metadata</text>
  <path d="M160 72 L226 72" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M160 150 L226 150" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M430 72 L496 72" class="edge-soft" marker-end="url(#intro-arrow)"/>
  <path d="M430 150 L496 150" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M430 228 L496 228" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M330 256 L330 296" class="edge-soft" marker-end="url(#intro-arrow)"/>
  <text x="193" y="64" text-anchor="middle" class="sub">load</text>
  <text x="193" y="142" text-anchor="middle" class="sub">read</text>
  <text x="472" y="64" text-anchor="middle" class="sub">state</text>
  <text x="472" y="142" text-anchor="middle" class="sub">build</text>
  <text x="472" y="220" text-anchor="middle" class="sub">index</text>
  <text x="338" y="290" class="sub">commit</text>
</svg>
</div>

## How it works

| Step | What happens |
| --- | --- |
| Add a topic | `mink table add orders` makes the topic a table. Indexing starts at the live end, and backfill reads the history. |
| Index | One indexer per partition reads each committed record once. Offset ranges become data files with exact statistics. Only metadata is stored. |
| Commit | A writer commits a snapshot every 10 seconds by default, as Iceberg metadata in the bucket. |
| Load | An engine loads the table from the REST catalog. Offsets newer than the latest snapshot come as a tail on it. |
| Read | The engine reads data files from the S3 endpoint. Mink builds each file's Parquet from Kafka, the same bytes every time, and caches it. |

## Features

- **Standard Iceberg.** Engines connect with REST catalog and S3 settings. No plugin and no client library.
- **Schema Registry.** Avro, Protobuf and JSON Schema payloads become typed columns. New schema versions evolve the table. A record that does not decode keeps its raw bytes.
- **No data copies.** Data files are built from Kafka on read. Kafka keeps the only copy of the records.
- **Real time.** A table load covers every record committed up to that moment.
- **Committed records only.** Aborted transactions and control records never show up.
- **Pruning.** Files are partitioned by time and carry min/max bounds per column. Engines skip the files a filter rules out.
- **Cache.** Built files are cached in memory, and on disk when configured.
- **Stateless nodes.** Any node can stop. Its partitions move to the others through leases in Postgres.
- **Admin console.** Tables, partitions, files, snapshots, schemas, records and nodes, plus a SQL page that runs DuckDB in the browser.

## Use cases

- SQL over live topics, without a sink pipeline or a second copy of the data.
- Joining Kafka data with lakehouse tables in one engine.
- Finding records by key, offset, time or field value.
