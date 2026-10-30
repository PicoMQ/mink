# Introduction

Mink is a read-only Iceberg catalog over Kafka. Iceberg engines query topics as tables, and the records stay in Kafka. A Mink node runs next to an existing Kafka cluster, with Postgres for its state and an S3-compatible bucket for the Iceberg metadata.

<div class="mink-diagram">
<svg viewBox="0 0 660 326" width="660" role="img" aria-label="Engines load tables from the REST catalog and read data files from the S3 endpoint. The catalog reads its state from Postgres. Indexers read Kafka and record files and leases in Postgres, and writers commit Iceberg metadata to the bucket. The S3 endpoint builds data files from Kafka.">
  <defs>
    <marker id="intro-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="30" y="10" width="600" height="56" class="box"/>
  <text x="330" y="34" text-anchor="middle" class="label">engines</text>
  <text x="330" y="52" text-anchor="middle" class="sub">DuckDB, PyIceberg</text>
  <rect x="10" y="100" width="640" height="102" class="edge-soft"/>
  <text x="22" y="118" class="sub">mink node</text>
  <rect x="30" y="126" width="160" height="56" class="box"/>
  <text x="110" y="150" text-anchor="middle" class="label">REST catalog</text>
  <text x="110" y="168" text-anchor="middle" class="sub">tables, snapshots</text>
  <rect x="230" y="126" width="200" height="56" class="box-accent"/>
  <text x="330" y="150" text-anchor="middle" class="label">indexers and writers</text>
  <text x="330" y="168" text-anchor="middle" class="sub">offsets into snapshots</text>
  <rect x="470" y="126" width="160" height="56" class="box"/>
  <text x="550" y="150" text-anchor="middle" class="label">S3 endpoint</text>
  <text x="550" y="168" text-anchor="middle" class="sub">built on read</text>
  <rect x="30" y="260" width="250" height="56" class="box"/>
  <text x="155" y="284" text-anchor="middle" class="label">Postgres</text>
  <text x="155" y="302" text-anchor="middle" class="sub">tables, files, leases</text>
  <rect x="300" y="260" width="80" height="56" class="box"/>
  <text x="340" y="284" text-anchor="middle" class="label">bucket</text>
  <text x="340" y="302" text-anchor="middle" class="sub">metadata</text>
  <rect x="400" y="260" width="230" height="56" class="box"/>
  <text x="515" y="284" text-anchor="middle" class="label">Kafka</text>
  <text x="515" y="302" text-anchor="middle" class="sub">the records</text>
  <path d="M110 66 L110 122" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M550 66 L550 122" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M110 182 L110 256" class="edge-soft" marker-end="url(#intro-arrow)"/>
  <path d="M255 182 L255 256" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M340 182 L340 256" class="edge-soft" marker-end="url(#intro-arrow)"/>
  <path d="M415 182 L415 256" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M550 182 L550 256" class="edge" marker-end="url(#intro-arrow)"/>
  <text x="118" y="90" class="sub">load</text>
  <text x="558" y="90" class="sub">read</text>
  <text x="118" y="236" class="sub">state</text>
  <text x="263" y="236" class="sub">files</text>
  <text x="348" y="236" class="sub">commit</text>
  <text x="423" y="236" class="sub">index</text>
  <text x="558" y="236" class="sub">build</text>
</svg>
</div>

## How it works

| Step | What happens |
| --- | --- |
| Add a topic | `mink table add orders` makes the topic a table. Indexing starts at the last stable offset, and backfill reads the history. |
| Index | One indexer per partition reads each committed record once. Offset ranges become data files with exact statistics. Only metadata is stored. |
| Commit | A writer commits a snapshot every 10 seconds by default, as Iceberg metadata in the bucket. |
| Load | An engine loads the table from the REST catalog, up to the last stable offset. See [Snapshots](/docs/design/snapshots). |
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
