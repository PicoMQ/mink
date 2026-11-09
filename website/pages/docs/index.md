# Introduction

Mink is a read-only Iceberg catalog over Kafka. Iceberg engines query topics as tables, with older records tiered into Iceberg and read as one table.

<div class="mink-diagram">
<svg viewBox="0 0 660 326" width="660" role="img" aria-label="Engines load tables from the REST catalog and read data files from the S3 endpoint. The catalog reads its state from Postgres. Workers record files and leases in Postgres, commit Iceberg metadata to the bucket and index Kafka. The S3 endpoint builds data files from Kafka and reads tiered files from the external table, which older records tier into from Kafka.">
  <defs>
    <marker id="intro-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="30" y="10" width="600" height="56" class="box"/>
  <text x="330" y="34" text-anchor="middle" class="label">engines</text>
  <text x="330" y="52" text-anchor="middle" class="sub">DuckDB, Trino, Spark, PyIceberg</text>
  <rect x="10" y="100" width="640" height="102" class="edge-soft"/>
  <text x="22" y="118" class="sub">mink node</text>
  <rect x="30" y="126" width="160" height="56" class="box"/>
  <text x="110" y="150" text-anchor="middle" class="label">REST catalog</text>
  <text x="110" y="168" text-anchor="middle" class="sub">tables, snapshots</text>
  <rect x="230" y="126" width="190" height="56" class="box-accent"/>
  <text x="325" y="150" text-anchor="middle" class="label">workers</text>
  <text x="325" y="168" text-anchor="middle" class="sub">index, commit, tier</text>
  <rect x="450" y="126" width="180" height="56" class="box"/>
  <text x="540" y="150" text-anchor="middle" class="label">S3 endpoint</text>
  <text x="540" y="168" text-anchor="middle" class="sub">virtual files</text>
  <rect x="30" y="260" width="230" height="56" class="box"/>
  <text x="145" y="284" text-anchor="middle" class="label">Postgres</text>
  <text x="145" y="302" text-anchor="middle" class="sub">tables, files, leases</text>
  <rect x="280" y="260" width="60" height="56" class="box"/>
  <text x="310" y="284" text-anchor="middle" class="label">bucket</text>
  <text x="310" y="302" text-anchor="middle" class="sub">metadata</text>
  <rect x="360" y="260" width="120" height="56" class="box"/>
  <text x="420" y="284" text-anchor="middle" class="label">Kafka</text>
  <text x="420" y="302" text-anchor="middle" class="sub">the records</text>
  <rect x="520" y="260" width="120" height="56" class="box"/>
  <text x="580" y="284" text-anchor="middle" class="label">external table</text>
  <text x="580" y="302" text-anchor="middle" class="sub">older records</text>
  <path d="M110 66 L110 122" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M540 66 L540 122" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M110 182 L110 256" class="edge-soft" marker-end="url(#intro-arrow)"/>
  <path d="M245 182 L245 256" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M310 182 L310 256" class="edge-soft" marker-end="url(#intro-arrow)"/>
  <path d="M400 182 L400 256" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M465 182 L465 256" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M580 182 L580 256" class="edge" marker-end="url(#intro-arrow)"/>
  <path d="M480 288 L516 288" class="edge" marker-end="url(#intro-arrow)"/>
  <text x="118" y="90" class="sub">load</text>
  <text x="548" y="90" class="sub">read</text>
  <text x="118" y="236" class="sub">state</text>
  <text x="253" y="236" class="sub">files</text>
  <text x="318" y="236" class="sub">commit</text>
  <text x="408" y="236" class="sub">index</text>
  <text x="473" y="236" class="sub">build</text>
  <text x="588" y="236" class="sub">read</text>
  <text x="500" y="280" text-anchor="middle" class="sub">tier</text>
</svg>
</div>

## How it works

| Step | What happens |
| --- | --- |
| Add a topic | `mink table add orders` makes the topic a table. Indexing starts at the last stable offset, and backfill reads the history. |
| Index | One indexer per partition reads each committed record once. Offset ranges become data files with exact statistics. Only metadata is stored. |
| Commit | A writer commits a snapshot every 10 seconds by default, as Iceberg metadata in the bucket. |
| Tier | With an external table, tier workers write records past `tier_after` to it, and the writer commits them every `tier_interval`. See [Tiering](/docs/design/tiering). |
| Load | An engine loads the table from the REST catalog, in the `merged`, `stream` or `lake` namespace, up to the last stable offset. See [Snapshots](/docs/design/snapshots). |
| Read | The engine reads data files from the S3 endpoint. Mink builds each file's Parquet from Kafka, the same bytes every time, and caches it. Tiered files come from the external table's bucket. |

## Features

- **Topics as tables.** Query Kafka topics as Iceberg tables from any Iceberg-compatible engine, with typed columns from Schema Registry and support for schema evolution.
- **Virtual files.** Kafka records form Iceberg data files on read, served from an S3-compatible endpoint, with only metadata stored and built files cached in memory and on disk.
- **Tiering.** Kafka records past a configurable age tier into an Iceberg table in an external REST catalog, with `merged`, `stream` and `lake` namespaces for the whole table and each part.
- **Real time.** Kafka records are queryable as Iceberg tables as soon as they're published, consistent across partitions.
- **Committed records only.** Aborted transactions and control records never show up.
- **Pruning.** Files are partitioned by time and carry min/max bounds per column. Engines skip the files a filter rules out.
- **Stateless nodes.** Mink nodes hold only caches, with metadata in Postgres. Indexing and tiering of each Kafka partition move between nodes through leases, and every cache rebuilds from Kafka and Postgres.
- **Admin console.** Tables, partitions, files, snapshots, schemas, records and nodes, plus a SQL page that runs DuckDB in the browser.

## Use cases

- SQL over live topics, without a sink pipeline.
- Joining Kafka data with lakehouse tables in one engine.
- Finding records by key, offset, time or field value.
