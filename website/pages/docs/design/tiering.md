# Tiering

Tiering moves a table's older records into an Iceberg table in another REST catalog, the table's external table. The table lists the external table's files before its own, so it reaches further back than Kafka's retention. The external table stays an ordinary Iceberg table that any engine reads, writes and maintains.

<div class="mink-diagram">
<svg viewBox="0 0 660 160" width="660" role="img" aria-label="Each partition reads left to right in offsets: the external table, then Mink's files from the external table's end offset, then the tail. A partition whose records Kafka deleted before they were tiered has a skipped span between the external table and Mink's files.">
  <defs>
    <marker id="tiering-offsets" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="20" y="35" class="sub">partition 0</text>
  <rect x="120" y="14" width="240" height="36" class="box"/>
  <text x="240" y="37" text-anchor="middle" class="label">external table</text>
  <rect x="360" y="14" width="170" height="36" class="box"/>
  <text x="445" y="37" text-anchor="middle" class="label">Mink's files</text>
  <rect x="530" y="14" width="100" height="36" class="box-accent"/>
  <text x="580" y="37" text-anchor="middle" class="sub">tail</text>
  <text x="20" y="91" class="sub">partition 1</text>
  <rect x="120" y="70" width="160" height="36" class="box"/>
  <text x="200" y="93" text-anchor="middle" class="label">external table</text>
  <rect x="280" y="70" width="80" height="36" class="edge-soft"/>
  <text x="320" y="93" text-anchor="middle" class="sub">skipped</text>
  <rect x="360" y="70" width="190" height="36" class="box"/>
  <text x="455" y="93" text-anchor="middle" class="label">Mink's files</text>
  <rect x="550" y="70" width="80" height="36" class="box-accent"/>
  <text x="590" y="93" text-anchor="middle" class="sub">tail</text>
  <path d="M120 132 L626 132" class="edge" marker-end="url(#tiering-offsets)"/>
  <text x="120" y="152" class="sub">offsets</text>
</svg>
</div>

| Part | Holds or does |
| --- | --- |
| External table | The tiered records as Parquet, in a catalog under [`external_catalogs`](/docs/operations/configuration#tiering). Its partition spec, sort order and `write.*` properties shape the files. |
| Tier worker | One per partition, on nodes with the `tier` role. Reads records from Kafka and writes data files to the external table's location. |
| Writer | Commits the written files to the external table every `tier_interval`, then lists the external table in the table's next snapshot. |
| Postgres | Ranges written but not committed, and each partition's committed end offset |

<div class="mink-diagram">
<svg viewBox="0 0 660 276" width="660" role="img" aria-label="A tier worker reads records from Kafka, writes Parquet to the external table's bucket and records the range in Postgres. The writer collects the ranges, appends them to the external table through its catalog, and lists the external table in the table's next snapshot in Mink's bucket.">
  <defs>
    <marker id="tiering-flow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="30" y="10" width="180" height="56" class="box"/>
  <text x="120" y="34" text-anchor="middle" class="label">Kafka</text>
  <text x="120" y="52" text-anchor="middle" class="sub">records</text>
  <rect x="450" y="10" width="180" height="56" class="box"/>
  <text x="540" y="34" text-anchor="middle" class="label">external catalog</text>
  <text x="540" y="52" text-anchor="middle" class="sub">the external table</text>
  <rect x="30" y="110" width="180" height="56" class="box-accent"/>
  <text x="120" y="134" text-anchor="middle" class="label">tier worker</text>
  <text x="120" y="152" text-anchor="middle" class="sub">one per partition</text>
  <rect x="450" y="110" width="180" height="56" class="box-accent"/>
  <text x="540" y="134" text-anchor="middle" class="label">writer</text>
  <text x="540" y="152" text-anchor="middle" class="sub">one per table</text>
  <rect x="30" y="210" width="180" height="56" class="box"/>
  <text x="120" y="234" text-anchor="middle" class="label">external bucket</text>
  <text x="120" y="252" text-anchor="middle" class="sub">{location}/data</text>
  <rect x="240" y="210" width="180" height="56" class="box"/>
  <text x="330" y="234" text-anchor="middle" class="label">Postgres</text>
  <text x="330" y="252" text-anchor="middle" class="sub">tiered ranges</text>
  <rect x="450" y="210" width="180" height="56" class="box"/>
  <text x="540" y="234" text-anchor="middle" class="label">Mink's bucket</text>
  <text x="540" y="252" text-anchor="middle" class="sub">Iceberg metadata</text>
  <path d="M120 66 L120 106" class="edge" marker-end="url(#tiering-flow)"/>
  <path d="M120 166 L120 206" class="edge" marker-end="url(#tiering-flow)"/>
  <path d="M210 140 L290 140 L290 206" class="edge" marker-end="url(#tiering-flow)"/>
  <path d="M370 210 L370 140 L446 140" class="edge" marker-end="url(#tiering-flow)"/>
  <path d="M540 110 L540 70" class="edge" marker-end="url(#tiering-flow)"/>
  <path d="M540 166 L540 206" class="edge" marker-end="url(#tiering-flow)"/>
  <text x="112" y="92" text-anchor="end" class="sub">1. read records</text>
  <text x="112" y="192" text-anchor="end" class="sub">2. write Parquet</text>
  <text x="250" y="132" text-anchor="middle" class="sub">3. record</text>
  <text x="408" y="132" text-anchor="middle" class="sub">4. collect</text>
  <text x="532" y="92" text-anchor="end" class="sub">5. append</text>
  <text x="532" y="192" text-anchor="end" class="sub">6. list</text>
</svg>
</div>

## When records tier

`tier_after` and `tier_interval` are set per table when it is [attached](/docs/operations/cli#attach).

- A file tiers once its first record is older than `tier_after`, 1 hour by default. Newer records in the same file tier with it.
- A partition flushes once `tier_bytes` of such files wait, or once its oldest waiting record is older than `tier_after` plus `tier_interval`.
- A flush reads its offsets from Kafka, at most `tier_bytes` per read, and writes Parquet with the external table's partition spec, sort order and the `write.*` properties [Table metadata](/docs/formats/metadata#external-table) lists. Its range ends at a file boundary.
- A flush that cannot be recorded deletes its files.

## Commits

Every `tier_interval`, 1 hour by default, the writer appends each partition's written ranges to the external table in one commit. The same commit sets the table property `mink.partition.{p}.end-offset`, the first offset of partition `p` the external table does not hold. The next snapshot of the table starts each partition at that end offset and lists the external table's current snapshot. Each record is listed once.

| Mink changes in the external table | Mink leaves to the table's owner |
| --- | --- |
| Creates it, when attaching a new table | Its schema, partition spec and sort order after creation |
| Appends data files | Compaction, deletes and rewrites |
| Sets `mink.partition.{p}.end-offset` | Snapshot expiry, orphan file removal and other properties |

Changes by other writers are listed within one `tier_interval`. Mink keeps listing the last snapshot it accepted while the current one has equality deletes, a schema other than the table's latest, or a spec or partition field id that time specs take.

## Attach and detach

A table has at most one external table. [`mink table attach`](/docs/operations/cli) and the [admin API](/docs/operations/admin#external-table) attach one.

| Attaching | Rule |
| --- | --- |
| A new table | Created with the table's schema and the given partition terms, or unpartitioned when that is given explicitly. Purged again if the attach then fails. |
| An existing table | Has the table's schema with the same field ids (`mink table schema`), format version 2 and no equality deletes. If it holds data, it needs an end offset for every partition. |
| Either | The topic's retention meets the [Requirements](#requirements). The location is `s3://`, outside Mink's warehouse, and inside or around no other external table's location. |

Each attach is the table's next generation, the `mink.external.generation` a snapshot's summary names. A snapshot lists one generation:

- Detaching stops tiering and listing. The external table is left as it is, and files written but not committed stay for its orphan file removal.
- Snapshots that list a detached table stay readable for time travel until they expire.
- The next attach expires them, so time travel reaches back to the attach. When the latest snapshot still lists the detached table, one snapshot without it comes first, and for one `snapshot_interval` the table reaches back only as far as Kafka.

## Gaps

When Kafka deletes records before they tier, tiering continues from the start of the indexed range. The next commit's summary marks the gap: its `mink.partition.{p}.start-offset` is above the previous commit's end offset. `mink_tier_gaps_total` and `mink_tier_skipped_offsets_total` count gaps, and `mink_tier_lag_seconds` shows the age of the oldest record that waits to tier ([Metrics](/docs/operations/metrics#tiering)).

## Requirements

| Requirement | Reason |
| --- | --- |
| Kafka keeps records longer than `tier_after` plus twice `tier_interval`, on local disk for clusters with tiered storage | Records are read from Kafka when they tier |
| Snapshot expiry on the external table keeps snapshots longer than `tier_interval` and `snapshot_retention` | Mink's snapshots list the external table's manifests |
| Orphan file removal on the external table waits longer than `tier_interval` | Written files wait up to one interval for their commit |
| Regular `rewrite_data_files` and `rewrite_manifests` on the external table | Each commit adds files and manifests |
| Mink's `storage` credentials write the external location from `tier` and `indexer` nodes and read it from `s3` nodes | Mink writes the files and serves them to engines |
| A rollback of the external table is followed by a detach and an attach with the end offsets of the snapshot rolled back to | Mink stops tiering when the end offsets move back |

## Limits

| | Limit |
| --- | --- |
| Region | The external bucket is in Mink's `storage.region` |
| Namespace | One level |
| Equality deletes | Stop the listing |
