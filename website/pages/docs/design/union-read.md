# Union read

A union read returns one bucket of a table as it is now: the rows in the lake snapshot plus every change in the log after the offset that snapshot was tiered to. The seam is the `bucket_log_end_offset` recorded with each lake snapshot. Nothing is read twice and nothing is skipped.

## The seam

<div class="mink-diagram">
<svg viewBox="0 0 680 190" width="680" role="img" aria-label="The lake snapshot covers the bucket log up to the recorded log_end_offset. The union reader reads the lake for that range and the log from the recorded offset to the high watermark.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="20" y="44" class="sub">bucket log</text>
  <rect x="120" y="30" width="80" height="36" class="box"/>
  <text x="160" y="53" text-anchor="middle" class="sub">trimmed</text>
  <rect x="200" y="30" width="240" height="36" class="box"/>
  <text x="320" y="53" text-anchor="middle" class="sub">tiered, still retained</text>
  <rect x="440" y="30" width="200" height="36" class="box-accent"/>
  <text x="540" y="53" text-anchor="middle" class="label">log tail</text>
  <text x="20" y="130" class="sub">lake snapshot</text>
  <rect x="120" y="116" width="320" height="36" class="box-accent"/>
  <text x="280" y="139" text-anchor="middle" class="label">Iceberg data + delete files</text>
  <path d="M440 20 L440 162" class="edge"/>
  <text x="440" y="180" text-anchor="middle" class="sub">log_end_offset</text>
  <path d="M640 20 L640 76" class="edge"/>
  <text x="640" y="14" text-anchor="end" class="sub">high_watermark</text>
  <path d="M120 20 L120 76" class="edge-soft"/>
  <text x="120" y="14" text-anchor="middle" class="sub">log_start</text>
</svg>
</div>

The reader reads the lake for everything below the seam and the log from the seam up to the high watermark. Rows in the log below the seam are ignored even though the log still holds them. The lake is authoritative for that range.

## Plan

A `Plan` is built per bucket at request time from the metadata view and the bucket's offsets:

| Field | Source |
| --- | --- |
| `bucket`, `partition` | The request |
| `lake.snapshot_id`, `lake.log_end_offset` | The recorded `CommitLakeSnapshot` for the table, this bucket's entry. Absent when the table is not tiered |
| `split` | The Iceberg file scan tasks for this partition and bucket at `snapshot_id`, with the predicate pushed down |
| `log_from` | `max(lake.log_end_offset, log_start)` |
| `log_to` | The high watermark at plan time |

`log_to` is fixed when the plan is made, so a union read is a consistent point-in-time result even while writes continue. The Iceberg split is planned by the node from the catalog. A partition whose lake data is pruned by the predicate has an empty split and only its log tail is read.

## Log tables

The lake stream is followed by the log stream. Rows arrive in lake order, then in log offset order. No deduplication is needed: an append-only row exists exactly once, either in the lake or in the log tail.

## Primary-key tables

The log tail is a changelog and the lake holds rows, so the two are merged by key:

1. **Collect the tail.** Read the log from `log_from` to `log_to` into memory. For every row record the key and the location of its last change. A last change of `+I` or `+U` is a live row. `-U` or `-D` marks the key as gone.
2. **Stream the lake.** For each lake batch, drop every row whose key appears in the tail map. Emit the rest.
3. **Emit the tail.** After the lake is exhausted, emit the live rows from the tail, in log order.

A key updated in the tail appears once with its newest values. A key deleted in the tail appears nowhere. A key untouched since tiering comes from the lake. The tail is the only part held in memory, and its size is bounded by the tiering gap: at most `lake_freshness` worth of changes per bucket under normal operation.

## Columns

The read schema is the requested projection plus the primary-key columns, which the merge needs even when the caller did not ask for them. Lake batches and log batches are normalized to that schema (casts for promoted types, nulls for added columns) before merging, and the key columns are projected away at the end when they were not requested.

## Where it runs

| Caller | Path |
| --- | --- |
| `Union` DoGet ticket | The bucket leader plans and merges, streams the result over Flight |
| `mink read --mode union` | One `Union` per bucket |
| `mink-query` | One `Union` split per bucket, merged in the engine. Lake files are read from object storage, the tail from the leader. See [SQL](/docs/design/query) |
| Iceberg engines | Read the lake table directly and see data up to the last tiering round, without the tail |

## Without a lake

A table with no `lake` option has no snapshot, so `log_from` is `log_start` and the lake stream is empty. `Union` then returns the whole log for a log table, and for a primary-key table the changelog folded to its live rows. `mink-query` reads the KV `Snapshot` instead for primary-key tables without a lake.
