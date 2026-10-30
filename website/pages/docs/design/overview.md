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
| `catalog` | The Iceberg REST catalog | `catalog_address` |
| `s3` | The S3 endpoint that serves metadata objects and builds data files | `s3_address` |
| `indexer` | Indexers, writers and topic following | |
| `admin` | The [admin API](/docs/operations/admin), the [console](/docs/operations/console) and `/metrics` | `admin_address` |

<div class="mink-diagram">
<svg viewBox="0 0 660 396" width="660" role="img" aria-label="A node runs indexers and writers, the S3 endpoint, the catalog and the admin listener. Indexers read Kafka and record files and snapshots in Postgres. The S3 endpoint builds data files from Kafka through its cache and serves metadata objects from the bucket. The catalog reads tables and snapshots from Postgres and metadata from the bucket.">
  <defs>
    <marker id="overview-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="30" y="10" width="600" height="56" class="box"/>
  <text x="330" y="34" text-anchor="middle" class="label">Postgres</text>
  <text x="330" y="52" text-anchor="middle" class="sub">tables, files, snapshots, leases</text>
  <rect x="10" y="84" width="640" height="204" class="edge-soft"/>
  <text x="22" y="102" class="sub">mink node</text>
  <rect x="30" y="120" width="180" height="56" class="box-accent"/>
  <text x="120" y="144" text-anchor="middle" class="label">indexers, writers</text>
  <text x="120" y="162" text-anchor="middle" class="sub">indexer role</text>
  <rect x="240" y="120" width="180" height="56" class="box"/>
  <text x="330" y="144" text-anchor="middle" class="label">s3</text>
  <text x="330" y="162" text-anchor="middle" class="sub">S3 endpoint, :9100</text>
  <rect x="450" y="120" width="180" height="56" class="box"/>
  <text x="540" y="144" text-anchor="middle" class="label">catalog</text>
  <text x="540" y="162" text-anchor="middle" class="sub">REST, :8181</text>
  <rect x="30" y="216" width="100" height="56" class="box"/>
  <text x="80" y="240" text-anchor="middle" class="label">admin</text>
  <text x="80" y="258" text-anchor="middle" class="sub">:9180</text>
  <rect x="240" y="216" width="150" height="56" class="box"/>
  <text x="315" y="240" text-anchor="middle" class="label">cache</text>
  <text x="315" y="258" text-anchor="middle" class="sub">memory, disk</text>
  <rect x="30" y="330" width="360" height="56" class="box"/>
  <text x="210" y="354" text-anchor="middle" class="label">Kafka</text>
  <text x="210" y="372" text-anchor="middle" class="sub">records</text>
  <rect x="400" y="330" width="230" height="56" class="box"/>
  <text x="515" y="354" text-anchor="middle" class="label">bucket</text>
  <text x="515" y="372" text-anchor="middle" class="sub">Iceberg metadata</text>
  <path d="M120 120 L120 70" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M540 120 L540 70" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M315 176 L315 212" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M315 272 L315 326" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M170 176 L170 326" class="edge" marker-end="url(#overview-arrow)"/>
  <path d="M410 176 L410 326" class="edge-soft" marker-end="url(#overview-arrow)"/>
  <path d="M540 176 L540 326" class="edge-soft" marker-end="url(#overview-arrow)"/>
  <text x="128" y="110" class="sub">files, snapshots</text>
  <text x="532" y="110" text-anchor="end" class="sub">tables, snapshots</text>
  <text x="323" y="200" class="sub">data files</text>
  <text x="323" y="310" class="sub">build</text>
  <text x="178" y="310" class="sub">index</text>
  <text x="418" y="310" class="sub">metadata</text>
  <text x="548" y="310" class="sub">metadata</text>
</svg>
</div>

## Background work

| Work | Runs on | Does |
| --- | --- | --- |
| Membership | Every node | Registers the node in Postgres. See [Cluster](/docs/design/cluster). |
| Topic following | `indexer` nodes | Follows each table's topic in Kafka: new partitions, deleted or recreated topics |
| Lease sharing | `indexer` nodes | Spreads each table's partition leases over the indexing nodes, and gives each table one writer. See [Cluster](/docs/design/cluster). |
| Indexer | Holder of a partition lease | Reads the partition's committed records once and records data files. See [Indexing](/docs/design/indexing). |
| Writer | Holder of a table's writer lease | Commits snapshots, assembles small files, publishes statistics, expires snapshots and collects old objects. See [Snapshots](/docs/design/snapshots). |
