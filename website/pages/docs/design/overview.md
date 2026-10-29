# Overview

A Mink deployment is Mink nodes next to a Kafka cluster, a Postgres database and an S3-compatible bucket. Nodes hold only caches. Any node can stop at any time, and the others take over its work.

| Component | Holds |
| --- | --- |
| Kafka | The records, the only copy |
| Postgres | Tables, indexed offset ranges, data files and their statistics, snapshots, leases, members |
| Bucket | Iceberg table metadata, manifest lists, manifests and statistics files |
| Node | Caches of built data files and metadata objects |
| Schema Registry, optional | The schemas typed columns come from |

## Anatomy of a node

A node runs any of four roles, all four by default.

| Role | Runs | Listens on |
| --- | --- | --- |
| `catalog` | The Iceberg REST catalog | `catalog_address`, `127.0.0.1:8181` |
| `s3` | The S3 endpoint that serves metadata objects and builds data files | `s3_address`, `127.0.0.1:9100` |
| `indexer` | Indexers, writers, and the pass that follows topics in Kafka | |
| `admin` | The [admin API](/docs/operations/admin), the [console](/docs/operations/console) and `/metrics` | `admin_address`, `127.0.0.1:9180` |

<div class="mink-diagram">
<svg viewBox="0 0 660 330" width="660" role="img" aria-label="A node runs the catalog, S3 and admin listeners. The S3 endpoint builds data files from Kafka through its cache, and serves metadata objects from the bucket. Indexers read Kafka and record files and leases in Postgres.">
  <defs>
    <marker id="overview-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="10" y="10" width="640" height="212" class="edge-soft"/>
  <text x="22" y="28" class="sub">mink node</text>
  <rect x="30" y="40" width="180" height="56" class="box"/>
  <text x="120" y="64" text-anchor="middle" class="label">catalog</text>
  <text x="120" y="82" text-anchor="middle" class="sub">REST, :8181</text>
  <rect x="240" y="40" width="180" height="56" class="box"/>
  <text x="330" y="64" text-anchor="middle" class="label">s3</text>
  <text x="330" y="82" text-anchor="middle" class="sub">S3 endpoint, :9100</text>
  <rect x="450" y="40" width="180" height="56" class="box"/>
  <text x="540" y="64" text-anchor="middle" class="label">admin</text>
  <text x="540" y="82" text-anchor="middle" class="sub">API, console, :9180</text>
  <rect x="30" y="146" width="180" height="56" class="box-accent"/>
  <text x="120" y="170" text-anchor="middle" class="label">indexers, writers</text>
  <text x="120" y="188" text-anchor="middle" class="sub">indexer role</text>
  <rect x="240" y="146" width="180" height="56" class="box"/>
  <text x="330" y="170" text-anchor="middle" class="label">cache</text>
  <text x="330" y="188" text-anchor="middle" class="sub">memory, optional disk</text>
  <rect x="30" y="264" width="120" height="56" class="box"/>
  <text x="90" y="288" text-anchor="middle" class="label">Postgres</text>
  <text x="90" y="306" text-anchor="middle" class="sub">state</text>
  <rect x="170" y="264" width="250" height="56" class="box"/>
  <text x="295" y="288" text-anchor="middle" class="label">Kafka</text>
  <text x="295" y="306" text-anchor="middle" class="sub">records</text>
  <rect x="450" y="264" width="180" height="56" class="box"/>
  <text x="540" y="288" text-anchor="middle" class="label">bucket</text>
  <text x="540" y="306" text-anchor="middle" class="sub">Iceberg metadata</text>
  <path d="M330 96 L330 142" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M330 202 L330 260" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M90 202 L90 260" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M190 202 L190 260" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M420 68 L436 68 L436 240 L540 240 L540 260" class="edge-soft" marker-end="url(#overview-arrow)"/>
  <text x="338" y="124" class="sub">data files</text>
  <text x="338" y="236" class="sub">build</text>
  <text x="98" y="236" class="sub">files, leases</text>
  <text x="198" y="236" class="sub">index</text>
  <text x="444" y="170" class="sub">metadata</text>
  <text x="444" y="186" class="sub">objects</text>
</svg>
</div>

Engines talk to the catalog and the S3 endpoint. Operators talk to the admin listener. The catalog reads Postgres and the bucket. The S3 endpoint serves metadata objects from the bucket and builds data files from Kafka.

## Background work

| Work | Runs on | Does |
| --- | --- | --- |
| Membership | Every node | Registers the node in Postgres and renews it every `lease_ttl / 3`. A stopped node drops out when its row expires. |
| Registration | `indexer` nodes | Follows each table's topic in Kafka: new partitions, deleted or recreated topics |
| Lease sharing | `indexer` nodes | Spreads each table's partition leases over the indexing nodes, and gives each table one writer |
| Indexer | Holder of a partition lease | Reads the partition's committed records once and records data files. See [Indexing](/docs/design/indexing). |
| Writer | Holder of a table's writer lease | Commits snapshots, assembles small files, publishes statistics, expires snapshots and collects old objects. See [Snapshots](/docs/design/snapshots). |

## Request routing

Any node with the `s3` role answers any read. Each data file has one owner among the `s3` nodes, chosen by hashing the node id with the file path. A node forwards a read of a file it does not own to the owner, which builds the file once and keeps it in its cache. When the owner cannot be reached, the receiving node builds the file itself. See [Reads](/docs/design/reads).
