# Tiering

Tiering copies table data from the bucket logs into an Iceberg table behind a catalog. A worker on the lease holder runs it per table on a freshness schedule, one Iceberg commit per round, with the log offset each bucket was tiered to written into the snapshot. The log keeps its own retention. The lake keeps history.

## Schedule

<div class="mink-diagram">
<svg viewBox="0 0 680 120" width="680" role="img" aria-label="Table tiering states: new, scheduled, pending, tiering, then tiered or failed. Failed and tiered return to scheduled after the freshness interval.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="30" width="110" height="44" class="box"/>
  <text x="75" y="57" text-anchor="middle" class="label">scheduled</text>
  <rect x="180" y="30" width="110" height="44" class="box"/>
  <text x="235" y="57" text-anchor="middle" class="label">pending</text>
  <rect x="340" y="30" width="110" height="44" class="box-accent"/>
  <text x="395" y="57" text-anchor="middle" class="label">tiering</text>
  <rect x="500" y="30" width="160" height="44" class="box"/>
  <text x="580" y="57" text-anchor="middle" class="label">tiered or failed</text>
  <path d="M130 52 L172 52" class="edge" marker-end="url(#arr)"/>
  <path d="M290 52 L332 52" class="edge" marker-end="url(#arr)"/>
  <path d="M450 52 L492 52" class="edge" marker-end="url(#arr)"/>
  <path d="M580 74 L580 96 L75 96 L75 82" class="edge-soft" marker-end="url(#arr)"/>
  <text x="151" y="24" text-anchor="middle" class="sub">due</text>
  <text x="311" y="24" text-anchor="middle" class="sub">worker</text>
  <text x="471" y="24" text-anchor="middle" class="sub">commit</text>
</svg>
</div>

| State | Transition |
| --- | --- |
| Scheduled | A table with `lake` set. Due at `last_tiered + lake_freshness`, default 3 minutes |
| Pending | Due. Waits for a worker |
| Tiering | A worker took it with a new epoch and heartbeats every 10 seconds. A silent worker is timed out and the table goes back to Pending |
| Tiered, Failed | Round finished or errored. Scheduled again after the freshness interval |

The coordinator keeps this state in memory on the lease holder and rebuilds it from the catalog after a lease change. `mink cluster stats` shows it per table.

## A round

1. Take the table, its descriptor and its bucket list. Abort if the table was dropped or recreated since it was scheduled.
2. Read the recorded lake snapshot: `snapshot_id` and `bucket_log_end_offset` per bucket.
3. Plan one split per bucket that has new data:

   | Recorded offset | Table | Split |
   | --- | --- | --- |
   | none, no lake snapshot yet | Primary-key | KV snapshot scan, ends at the snapshot's `log_offset` |
   | none | Log, or PK with a snapshot | Log from `log_start` to the high watermark |
   | at or past the high watermark | any | Skipped |
   | below `log_start` | any | Error. Retention is held at the lake offset, so this only follows a manual trim |
   | otherwise | any | Log from the recorded offset to the high watermark |

4. Write each split through the Iceberg writer, one writer per bucket. Complete returns data files and delete files.
5. Commit all files as one Iceberg snapshot with the `mink-offsets` property set to the new per-bucket end offsets. If nothing was written, only the offsets advance in the metadata log.
6. Propose `CommitLakeSnapshot { snapshot_id, bucket_log_end_offset }`. The [union reader](/docs/design/union-read) and [retention](/docs/design/retention) read from here.

If the lake has a snapshot newer than the recorded one, for example after a coordinator crash between step 5 and 6, the worker reads `mink-offsets` off that snapshot, records it and retries the round from there. The property makes the commit idempotent against the metadata log.

## Iceberg table layout

| Aspect | Value |
| --- | --- |
| Format version | 2 by default. `iceberg.format-version = 3` in the table properties selects v3 |
| Schema | The Mink schema with the same field ids. Primary key columns become identifier fields |
| Partition spec | Identity on each partition key, plus `{key}_bucket` with `bucket(n)` on the bucket key for tables with one bucket key |
| Sort order | Unsorted |
| Properties | Merge-on-read for delete, update and merge. `mink.*` copies of the table options. User `iceberg.*` options passed through |
| Data files | Parquet, rolled at the target file size |
| Delete files | Equality deletes on the identifier fields, primary-key tables only |
| Commit user | `__mink_lake_tiering` |

The bucket transform is why the bucketing rule follows the lake format: Iceberg's `bucket(n)` on the key column in the lake must equal the bucket Mink routed the row to, or the union reader could not pair a lake partition with a log bucket.

### Primary-key tables

A round's changelog range for one bucket is folded before it is written: only the last change per key survives. A key whose final change is `+I` or `+U` becomes one data row. A key whose final change is `-D` becomes an equality delete. A key that already existed in the lake and was updated gets both a delete for the old row and a data row for the new. A key first seen and deleted in the same round writes nothing.

### Log tables

Batches are written as they are, in offset order, into the partition's data files. Nothing is deduplicated.

## Compaction

With `lake_auto_compaction`, each round also rewrites small files in the partitions it touches. Groups of at least 3 files below the target size are read, merged and rewritten at `read.split.target-size` (default 128 MiB), and the rewrite is committed in the same snapshot as the new data. Without it, compaction is left to the lake.

## Attaching an existing table

`lake_attach = true` at create time points a Mink table at an Iceberg table that already exists. The catalog checks before accepting:

- format version, schema, types and nullability match the Mink descriptor.
- partition spec is identity on the partition keys.
- bucket key and count match the bucket transform, required for primary-key tables.
- the current snapshot becomes the baseline. Rows below it are the lake's, rows above come from the log.

## Catalog

The `[lake]` block in the node configuration selects the format and catalog: `format = "iceberg"`, `catalog = "rest"`, `uri`, `warehouse`, and `properties` for the object-store client. Table names are `database.table`. Databases map to namespaces.

## Freshness and cost

| Knob | Effect |
| --- | --- |
| `lake_freshness` | Interval between rounds per table. Shorter is fresher and produces more, smaller files |
| `tiering_poll_interval` | How often an idle worker asks for work, default 30 seconds |
| `lake_auto_compaction` | Trades write amplification in the worker for fewer files for readers |
| `log_ttl` | Independent of tiering. Retention never trims past the recorded lake offset, so a stalled worker holds the log rather than losing data |
