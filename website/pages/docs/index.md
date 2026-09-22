# Introduction

Mink is lakehouse-native streaming storage on object storage. Clients create log tables and primary-key tables, write rows over the Kafka protocol or Arrow Flight, and read them back as a log, a snapshot, or one table that spans the hot log and the lakehouse. Every byte is stored on S3-compatible object storage, cluster coordination goes through a SQL database, and history is tiered into Apache Iceberg. A node is a single binary with no local state worth backing up.

<div class="mink-diagram">
<svg viewBox="0 0 720 320" width="720" role="img" aria-label="Clients talk Kafka, Arrow Flight or Flight SQL to mink nodes. Nodes write to object storage, coordinate through a SQL metadata log, and tier history into an Iceberg lakehouse.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="124" width="160" height="72" class="box"/>
  <text x="100" y="150" text-anchor="middle" class="label">clients</text>
  <text x="100" y="168" text-anchor="middle" class="sub">Kafka, Arrow Flight</text>
  <text x="100" y="183" text-anchor="middle" class="sub">Flight SQL</text>
  <rect x="270" y="40" width="160" height="56" class="box-accent"/>
  <text x="350" y="64" text-anchor="middle" class="label">mink node 1</text>
  <text x="350" y="82" text-anchor="middle" class="sub">tablets + frontends</text>
  <rect x="270" y="224" width="160" height="56" class="box-accent"/>
  <text x="350" y="248" text-anchor="middle" class="label">mink node 2</text>
  <text x="350" y="266" text-anchor="middle" class="sub">tablets + frontends</text>
  <rect x="540" y="20" width="160" height="72" class="box"/>
  <text x="620" y="46" text-anchor="middle" class="label">SQL metadata log</text>
  <text x="620" y="64" text-anchor="middle" class="sub">Postgres or SQLite</text>
  <text x="620" y="79" text-anchor="middle" class="sub">commands and views</text>
  <rect x="540" y="124" width="160" height="72" class="box"/>
  <text x="620" y="150" text-anchor="middle" class="label">object storage</text>
  <text x="620" y="168" text-anchor="middle" class="sub">S3 compatible</text>
  <text x="620" y="183" text-anchor="middle" class="sub">WAL, log, snapshots</text>
  <rect x="540" y="228" width="160" height="72" class="box"/>
  <text x="620" y="254" text-anchor="middle" class="label">lakehouse</text>
  <text x="620" y="272" text-anchor="middle" class="sub">Iceberg catalog</text>
  <text x="620" y="287" text-anchor="middle" class="sub">tiered tables</text>
  <path d="M180 148 L262 80" class="edge" marker-end="url(#arr)"/>
  <path d="M180 172 L262 240" class="edge" marker-end="url(#arr)"/>
  <path d="M430 60 L532 56" class="edge" marker-end="url(#arr)"/>
  <path d="M430 84 L532 150" class="edge" marker-end="url(#arr)"/>
  <path d="M430 236 L532 170" class="edge" marker-end="url(#arr)"/>
  <path d="M430 260 L532 264" class="edge" marker-end="url(#arr)"/>
  <path d="M430 244 L532 76" class="edge-soft"/>
</svg>
</div>

## Model

A table is the unit of storage. Each table is split into buckets, and each bucket is an independent, ordered log on object storage. A primary-key table adds a key-value tablet per bucket that materializes the latest row per key and emits a changelog into the same log. History leaves the log on a schedule and lands in the lakehouse as an Iceberg table with the user's schema. A snapshot property records the log offset each bucket was tiered to. A reader that wants the whole table gets the lake snapshot plus the log tail after it, per bucket, stitched at that offset.

## Features

- **Two table types.** Log tables for append-only rows. Primary-key tables for upsert, partial update, aggregation, delete, point lookup and prefix lookup, with a changelog.
- **Zero-disk nodes.** The write-ahead log, log segments and KV snapshots live on S3-compatible object storage through the [s3stream](https://github.com/PicoMQ/s3stream) engine. A node keeps caches and a local KV working copy, both rebuildable.
- **SQL as the control plane.** Cluster metadata is an ordered command log in Postgres, or SQLite for a single node. Nodes tail it and rebuild the same view. There is no consensus protocol.
- **Lakehouse tiering.** A worker copies closed log data and KV state into Iceberg on a per-table freshness schedule. Existing Iceberg tables can be attached instead of created.
- **Union read.** Lake snapshot plus log tail, per bucket, served as one table over Arrow Flight, and as SQL over Flight SQL by the `mink-query` engine on DataFusion.
- **Kafka and Arrow Flight.** Kafka producers and consumers, including consumer groups and idempotent producers, map onto log tables. Arrow Flight carries typed Arrow batches, lookups and admin over gRPC.
- **Fencing everywhere.** Node epochs and stream epochs keep a replaced process from writing to a bucket it no longer owns.
- **One binary per role.** `mink` is the node, the client and the admin CLI. `mink-query` is the SQL engine.

## What is in the box

| Component | Role |
| --- | --- |
| `s3stream` | Stream engine: WAL, object layout, caching, compaction |
| `mink-log`, `mink-tablet`, `mink-kv` | Log tablet over s3stream, KV tablet with changelog, merge engines and snapshots |
| `mink-metadata`, `mink-sql` | Command log state machine, Postgres and SQLite sink, leases, heartbeats |
| `mink-coordinator` | Bucket assignment, rebalance, auto partitioning, tiering schedule |
| `mink-lake` | Tiering worker, Iceberg writer, committer, compaction, lake source |
| `mink-read` | Union reader: lake snapshot plus log tail |
| `mink-flight`, `mink-kafka` | Arrow Flight and Kafka frontends on the same node service |
| `mink-client`, `mink-cli` | Rust client and the `mink` binary |
| `mink-query`, `mink-query-cli` | DataFusion catalog and table provider, and the `mink-query` binary |

## Boundaries

- Append latency is object-store latency. A write is acknowledged when it is durable in the WAL on object storage, typically tens of milliseconds.
- Kafka reads are served from the log. A fetch older than the table's `log_ttl` returns `OFFSET_OUT_OF_RANGE`. History older than that is read through the lakehouse.
- Kafka transactions are not implemented. Idempotent producers are.
- Authentication is not implemented. Listeners are expected on a private network.
