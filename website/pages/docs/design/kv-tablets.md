# KV tablets

A KV tablet is the materialized state of one bucket of a primary-key table: the latest row per key. It sits on top of the bucket's [log tablet](/docs/design/log-tablets), which holds the changelog and is the source of truth. The KV store on the node is a working copy that can be rebuilt from a snapshot in object storage plus a replay of the log.

## Put

<div class="mink-diagram">
<svg viewBox="0 0 680 230" width="680" role="img" aria-label="A put reads the old row from the prewrite buffer or the store, merges, encodes changelog records, appends them to the log, and on success flushes the prewrite buffer to the store.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="40" width="130" height="56" class="box"/>
  <text x="85" y="64" text-anchor="middle" class="label">read old</text>
  <text x="85" y="82" text-anchor="middle" class="sub">buffer, store</text>
  <rect x="190" y="40" width="130" height="56" class="box"/>
  <text x="255" y="64" text-anchor="middle" class="label">merge</text>
  <text x="255" y="82" text-anchor="middle" class="sub">engine, partial</text>
  <rect x="360" y="40" width="130" height="56" class="box-accent"/>
  <text x="425" y="64" text-anchor="middle" class="label">log append</text>
  <text x="425" y="82" text-anchor="middle" class="sub">changelog batch</text>
  <rect x="530" y="40" width="130" height="56" class="box"/>
  <text x="595" y="64" text-anchor="middle" class="label">flush</text>
  <text x="595" y="82" text-anchor="middle" class="sub">into the store</text>
  <rect x="190" y="150" width="130" height="50" class="box"/>
  <text x="255" y="180" text-anchor="middle" class="sub">prewrite buffer</text>
  <rect x="530" y="150" width="130" height="50" class="box"/>
  <text x="595" y="180" text-anchor="middle" class="sub">KV store</text>
  <path d="M150 68 L182 68" class="edge" marker-end="url(#arr)"/>
  <path d="M320 68 L352 68" class="edge" marker-end="url(#arr)"/>
  <path d="M490 68 L522 68" class="edge" marker-end="url(#arr)"/>
  <path d="M255 96 L255 142" class="edge" marker-end="url(#arr)"/>
  <path d="M595 96 L595 142" class="edge" marker-end="url(#arr)"/>
  <path d="M320 175 L522 175" class="edge-soft" marker-end="url(#arr)"/>
</svg>
</div>

1. **Read the old row.** From the prewrite buffer first, so a second write to the same key in one request sees the first, then from the KV store.
2. **Merge.** The merge engine decides the new row from old and new: default last-write, `first_row`, `versioned` or `aggregation`. A partial update copies only the target columns over the old row.
3. **Encode the changelog.** One batch of change records for the request, with the schema id of the table version the rows were encoded with.
4. **Append to the log.** The log tablet assigns offsets and waits for durability.
5. **Flush.** Only after the append succeeds do the staged rows move from the prewrite buffer into the KV store. A failed append leaves the store untouched.

The KV store is the local working copy, never the source of truth, so a crash between steps 4 and 5 loses nothing: recovery replays the log.

## Changelog

| Write | Old row | `changelog_image = full` | `changelog_image = wal` |
| --- | --- | --- | --- |
| Insert | none | `+I new` | `+I new` |
| Update | exists | `-U old`, `+U new` | `+U new` |
| Delete | exists | `-D old` | `-D old` |
| Delete | none | nothing | nothing |

`wal` with the default merge engine, no partial update and no auto-increment column skips the old-row read entirely and treats every write as an update. This is the fastest path and the changelog carries no before images.

Deletes follow `delete_behavior`: `allow` merges the delete, `ignore` drops it silently, `disable` rejects the request. A partial delete, a delete with target columns, nulls the target columns and removes the row only when every non-key column is already null.

## Merge engines

| Engine | Rule | Deletes | Partial update |
| --- | --- | --- | --- |
| Default | New replaces old | Removes the key | Yes |
| `first_row` | Old kept, new ignored | Rejected | No |
| `versioned` | Larger value in the version column wins | Rejected | No |
| `aggregation` | Each non-key column folded by its aggregate function | Removes the key | Yes |

Aggregate functions and the option constraints are listed under [Tables](/docs/design/tables#merge-engines).

## Partial update

A write with target columns must include every primary-key column and every non-nullable column, except an auto-increment column which the tablet fills. Columns outside the targets keep their old value. A write whose targets are only the key columns is a no-op that leaves the old row in place.

## Auto increment

An auto-increment column is filled on insert from a counter in the metadata log, allocated in ranges through the `Allocate` command. The counter is per table and column. Each tablet caches a segment of values, so values are unique across buckets, ascending within a bucket, and gaps appear after a leader change. Writes to such a table must name their target columns and must not include the auto-increment column.

## Value encoding

A stored value is `[schema_id u16][compacted row bytes]`. Reads decode with the schema the row was written under and remap to the current schema, so alter does not rewrite the store.

## Store engines

| Engine | Use |
| --- | --- |
| `surrealkv` | Default. Checkpoint is a directory of `.sst` files, shared between snapshots when unchanged, plus private WAL files |
| memory | Tests and the single-file checkpoint |

## Snapshots

Every `kv_snapshot_interval` (10 minutes) the bucket leader checkpoints the store, uploads the checkpoint files to object storage under `snap-{id}` and proposes `CommitKvSnapshot`.

| Snapshot field | Meaning |
| --- | --- |
| `snapshot_id`, `bucket`, `location` | Identity and object prefix |
| `shared`, `private` | Files reused from an earlier snapshot, files unique to this one |
| `log_offset` | Log high watermark at checkpoint time. Recovery replays from here |
| `row_count`, `auto_increment` | Row count and the counter position |
| `_METADATA` | The encoded snapshot record next to the files |

The coordinator keeps the newest `snapshots_retained` snapshots (default 1) and proposes `DropKvSnapshot` for the rest. A cleaner deletes files that no retained snapshot references.

## Recovery

When a node opens a KV tablet it downloads the latest snapshot, restores the store from it, then replays the log from `snapshot.log_offset`:

- up to the high watermark, into the store, skipping `-U` records since only the after image matters.
- from the high watermark to `log_end`, into the prewrite buffer, so unacknowledged rows in the s3stream WAL are not lost when they become durable.

With no snapshot, replay starts at `log_start`. Recovery time is bounded by the snapshot interval and the write rate.

## Reads

| Read | Path |
| --- | --- |
| Lookup | Encode the key, `get` from the store, decode |
| Multi lookup | One `get` per key |
| Prefix lookup | Range scan over the encoded prefix. The prefix is the partition keys plus the bucket keys, so the request lands on one bucket |
| Snapshot scan | Full store iteration in key order, optionally limited |

Lookups read the store, not the prewrite buffer, so a lookup returns rows whose changelog append has completed.
