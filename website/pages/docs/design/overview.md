# Overview

A Mink deployment has four components. Nodes serve clients and hold only caches and rebuildable working copies. An object store holds every record. A SQL database holds the metadata log that coordinates the nodes. A lakehouse catalog holds the tiered history.

## Anatomy of a node

Each node runs the same stack. An Arrow Flight listener serves typed batches, lookups and admin over gRPC, and a Kafka listener serves Kafka clients. Behind them, the node service decides whether this node owns a bucket or redirects, opens tablets under an epoch, and hosts the background loops. The coordinator runs on whichever node holds the lease. Every part reads cluster state from the same metadata view and proposes changes to the same command log. The `s3stream` engine moves records to and from object storage.

<div class="mink-diagram">
<svg viewBox="0 0 600 340" width="600" role="img" aria-label="A node runs an Arrow Flight and a Kafka listener, the node service and the coordinator, the tablets and the s3stream engine. The coordinator and the node service talk to the SQL metadata log. The engine writes to object storage.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="20" width="400" height="300" class="edge-soft"/>
  <text x="30" y="40" class="sub">mink node</text>
  <rect x="40" y="50" width="180" height="56" class="box"/>
  <text x="130" y="74" text-anchor="middle" class="label">Arrow Flight</text>
  <text x="130" y="92" text-anchor="middle" class="sub">DoPut, DoGet, actions</text>
  <rect x="230" y="50" width="180" height="56" class="box"/>
  <text x="320" y="74" text-anchor="middle" class="label">Kafka listener</text>
  <text x="320" y="92" text-anchor="middle" class="sub">produce, fetch, groups</text>
  <rect x="40" y="136" width="180" height="56" class="box"/>
  <text x="130" y="160" text-anchor="middle" class="label">node service</text>
  <text x="130" y="178" text-anchor="middle" class="sub">own, fence, redirect</text>
  <rect x="230" y="136" width="180" height="56" class="box"/>
  <text x="320" y="160" text-anchor="middle" class="label">coordinator</text>
  <text x="320" y="178" text-anchor="middle" class="sub">on the lease holder</text>
  <rect x="40" y="222" width="180" height="56" class="box"/>
  <text x="130" y="246" text-anchor="middle" class="label">tablets</text>
  <text x="130" y="264" text-anchor="middle" class="sub">log and KV per bucket</text>
  <rect x="230" y="222" width="180" height="56" class="box-accent"/>
  <text x="320" y="246" text-anchor="middle" class="label">s3stream engine</text>
  <text x="320" y="264" text-anchor="middle" class="sub">WAL, objects, cache</text>
  <rect x="440" y="120" width="150" height="56" class="box"/>
  <text x="515" y="144" text-anchor="middle" class="label">SQL metadata</text>
  <text x="515" y="162" text-anchor="middle" class="sub">Postgres, SQLite</text>
  <rect x="440" y="222" width="150" height="56" class="box"/>
  <text x="515" y="246" text-anchor="middle" class="label">object storage</text>
  <text x="515" y="264" text-anchor="middle" class="sub">S3 compatible</text>
  <path d="M130 106 L130 128" class="edge" marker-end="url(#arr)"/>
  <path d="M320 106 L320 128" class="edge" marker-end="url(#arr)"/>
  <path d="M130 192 L130 214" class="edge" marker-end="url(#arr)"/>
  <path d="M220 250 L230 250" class="edge" marker-end="url(#arr)"/>
  <path d="M410 150 L432 150" class="edge" marker-start="url(#arr)" marker-end="url(#arr)"/>
  <path d="M410 250 L432 250" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

## Tables and buckets

The unit of storage is a bucket. A table has `bucket_count` buckets, or `bucket_count` per partition when partitioned. Each bucket is one s3stream stream, an ordered log of record batches with offsets assigned by the bucket's leader. A primary-key table adds a KV tablet per bucket that materializes the latest row per key and writes its changelog into the same stream.

| Table type | Log tablet | KV tablet | Reads |
| --- | --- | --- | --- |
| Log table | Yes | No | Log scan, tail |
| Primary-key table | Yes, holds the changelog | Yes | Log scan, snapshot scan, lookup, prefix lookup, union |

See [Tables](/docs/design/tables) for the schema and options, [Log tablets](/docs/design/log-tablets) and [KV tablets](/docs/design/kv-tablets) for the storage.

## Ownership

Each bucket has one leader node. The coordinator assigns leaders round-robin across live nodes at create time, re-leads the buckets of a dead node on its next tick, and moves buckets between nodes when an operator runs `mink cluster rebalance`. The leader opens the stream with the bucket's leader epoch. S3stream rejects appends from any older epoch, so a replaced node cannot write. A request for a bucket that arrives at another node gets a redirect to the leader over Flight, or is forwarded when the write is table-routed. See [Ownership and failover](/docs/design/ownership).

## The metadata log

Cluster state is a single ordered log of commands in the SQL database: streams and objects for s3stream, node registrations, the catalog of databases, tables and partitions, bucket leaders, KV and lake snapshots, producer and group offsets. Every node appends commands and tails the log, applying each command to the same in-memory state machine. The lease holder takes periodic snapshots and truncates the log behind them. See [Metadata](/docs/design/metadata).

## Data paths

<div class="mink-diagram">
<svg viewBox="0 0 700 250" width="700" role="img" aria-label="Writes flow from clients to the leader's tablets to the WAL and stream objects. A tiering worker copies closed log data to Iceberg. Readers combine the lake snapshot and the log tail.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="40" width="120" height="56" class="box"/>
  <text x="80" y="64" text-anchor="middle" class="label">writer</text>
  <text x="80" y="82" text-anchor="middle" class="sub">Kafka, Flight</text>
  <rect x="200" y="40" width="150" height="56" class="box-accent"/>
  <text x="275" y="64" text-anchor="middle" class="label">bucket leader</text>
  <text x="275" y="82" text-anchor="middle" class="sub">log + KV tablet</text>
  <rect x="410" y="40" width="130" height="56" class="box"/>
  <text x="475" y="64" text-anchor="middle" class="label">WAL</text>
  <text x="475" y="82" text-anchor="middle" class="sub">object storage</text>
  <rect x="580" y="40" width="100" height="56" class="box"/>
  <text x="630" y="64" text-anchor="middle" class="label">stream</text>
  <text x="630" y="82" text-anchor="middle" class="sub">objects</text>
  <rect x="410" y="160" width="130" height="56" class="box"/>
  <text x="475" y="184" text-anchor="middle" class="label">tiering</text>
  <text x="475" y="202" text-anchor="middle" class="sub">closed data</text>
  <rect x="580" y="160" width="100" height="56" class="box"/>
  <text x="630" y="184" text-anchor="middle" class="label">Iceberg</text>
  <text x="630" y="202" text-anchor="middle" class="sub">lakehouse</text>
  <rect x="20" y="160" width="120" height="56" class="box"/>
  <text x="80" y="184" text-anchor="middle" class="label">reader</text>
  <text x="80" y="202" text-anchor="middle" class="sub">union read</text>
  <path d="M140 68 L192 68" class="edge" marker-end="url(#arr)"/>
  <path d="M350 68 L402 68" class="edge" marker-end="url(#arr)"/>
  <path d="M540 68 L572 68" class="edge" marker-end="url(#arr)"/>
  <path d="M630 96 L630 120 L475 120 L475 152" class="edge" marker-end="url(#arr)"/>
  <path d="M540 188 L572 188" class="edge" marker-end="url(#arr)"/>
  <path d="M140 176 L275 176 L275 104" class="edge" marker-end="url(#arr)"/>
  <path d="M140 200 L360 200 L360 230 L630 230 L630 224" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

- **Write.** The client routes each row to a bucket by key hash or round-robin and sends the batch to the leader. The leader assigns offsets, appends to the stream, waits for WAL durability on object storage, then updates the KV working copy for primary-key tables. See [Writes](/docs/design/writes).
- **Read.** A log read serves batches up to the high watermark from the s3stream cache or the object store. A snapshot read walks the KV working copy. A tail read waits for new data. See [Reads](/docs/design/reads).
- **Tiering.** A worker on the lease holder copies log data and KV state into Iceberg per table on a freshness schedule and records the tiered log offset per bucket in the snapshot. See [Tiering](/docs/design/tiering).
- **Union.** The lake snapshot plus the log tail after its recorded offset, per bucket, with primary-key deduplication. See [Union read](/docs/design/union-read).

## Background work

| Loop | Where | Interval |
| --- | --- | --- |
| WAL upload to stream objects | every node, s3stream | `wal_upload_interval`, 10s |
| KV snapshot upload | bucket leader | `kv_snapshot_interval`, 10m |
| Log retention trim | bucket leader | `log_retention_interval`, 5m |
| Coordinator tick: re-lead orphans, prune KV snapshots, auto partitions, expire offsets, tiering schedule | lease holder | `coordinator_tick`, 5s |
| Tiering worker | lease holder | `tiering_poll_interval`, 30s |
| Metadata snapshot and truncation, dead-object cleanup | lease holder | lease TTL derived |
| Heartbeat and lease renewal | every node | `lease_ttl` / 4 |
