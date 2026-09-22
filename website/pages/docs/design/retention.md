# Retention

Retention decides what leaves the log and when. The log is trimmed by time, never past what the lake, the latest KV snapshot or the stream itself still need. Everything else with a lifetime, KV snapshots, lake snapshots, partitions, producer and group offsets, has its own rule below.

## Log TTL

Each table has `log_ttl`, default 7 days, or unset for forever. Every `log_retention_interval` (5 minutes) the bucket leader computes a cutoff of `now - log_ttl` and trims the log to the first offset whose batch is newer than the cutoff, subject to a floor.

<div class="mink-diagram">
<svg viewBox="0 0 680 190" width="680" role="img" aria-label="The expired range ends at the TTL cutoff. The floor is the minimum of the committed stream end, the lake offset and the KV snapshot offset. The trim point is the lower of the two.">
  <rect x="20" y="40" width="200" height="40" class="box"/>
  <text x="120" y="65" text-anchor="middle" class="sub">older than log_ttl</text>
  <rect x="220" y="40" width="440" height="40" class="box-accent"/>
  <text x="440" y="65" text-anchor="middle" class="label">retained</text>
  <path d="M220 30 L220 90" class="edge"/>
  <text x="220" y="22" text-anchor="middle" class="sub">expired_to</text>
  <path d="M150 100 L150 160" class="edge-soft"/>
  <text x="150" y="176" text-anchor="middle" class="sub">lake offset</text>
  <path d="M330 100 L330 160" class="edge-soft"/>
  <text x="330" y="176" text-anchor="middle" class="sub">KV snapshot offset</text>
  <path d="M560 100 L560 160" class="edge-soft"/>
  <text x="560" y="176" text-anchor="middle" class="sub">stream end</text>
  <text x="20" y="120" class="sub">floor = min of these</text>
  <text x="20" y="140" class="sub">trim to min(expired_to, floor) = 150</text>
</svg>
</div>

| Floor input | Applies to | Why |
| --- | --- | --- |
| Stream end offset in the metadata log | Every table | Rows still only in the WAL, not yet committed to stream objects, cannot be trimmed |
| `bucket_log_end_offset` of the recorded lake snapshot | Tables with `lake` | The tiering worker reads the log from here. Trimming past it would lose rows the lake never got |
| `log_offset` of the latest KV snapshot | Primary-key tables | Recovery replays the changelog from here. Trimming past it would make the snapshot unrecoverable |

| Result | Meaning |
| --- | --- |
| `Kept` | Nothing expired, or no TTL |
| `Trimmed { new_start, expired_to }` | `log_start` moved forward. Stream objects below it become garbage |
| `Held { expired_to, held_at }` | Rows have expired but the floor is at or below `log_start`. Visible in `mink cluster stats` as a table whose tiering or snapshotting has stalled |

The cutoff offset is found with the binary search from [Log tablets](/docs/design/log-tablets#timestamp-lookup) and cached as a frontier `(offset, timestamp)`, so later passes only search forward from the last result.

A trim moves the stream's start offset in the metadata log. s3stream deletes stream objects that fall entirely below every stream's start and rewrites stream-set objects during compaction, so space comes back in the background rather than at trim time.

## What readers see

| Reader | Effect of a trim |
| --- | --- |
| `Scan` below `log_start` | Error. The client should start at `Earliest` |
| Kafka `Fetch` below `log_start` | `OFFSET_OUT_OF_RANGE`. The consumer's `auto.offset.reset` applies |
| Union read | Unaffected. The trimmed range is covered by the lake |
| Lakehouse engines | Unaffected |

`log_ttl` bounds how far back a Kafka consumer or a log scan can replay. History beyond it is in the lake for tiered tables and gone for untiered ones.

## KV snapshots

The coordinator keeps the newest `snapshots_retained` (default 1) snapshots per bucket and proposes `DropKvSnapshot` for older ones. The cleaner on each node then deletes files no retained snapshot references. Files shared between snapshots survive until the last reference goes. A snapshot is only dropped after a newer one has been committed, so there is always one to recover from.

## Lake snapshots

Mink never expires Iceberg snapshots. Snapshot expiry, orphan-file removal and any compaction beyond `lake_auto_compaction` are the lake's job through its catalog. The committer reports the earliest snapshot Mink still depends on so an external expiry can keep it.

## Partitions

With `auto_partition`, the coordinator creates `num_precreate` partitions ahead of the current time unit and drops partitions older than `num_retention` units (default 7). Dropping a partition destroys its buckets' streams and KV state. Its rows remain in the lake if they were tiered. Manual partitions are never dropped automatically.

## Offsets and writers

| Item | Lifetime |
| --- | --- |
| Idempotent writer window | Writer idle for 7 days is forgotten. The next batch starts a new window |
| Producer offsets, a sink's recorded start offsets per bucket | `producer_offsets_ttl`, default 24 hours, or the TTL given at registration. Swept every `producer_offsets_cleanup_interval` (1 hour) |
| Kafka group offsets | `group_offsets_ttl`, default 7 days, or the retention the client asked for |
| Prepared but uncommitted objects | Expired by the lease holder's lifecycle loop. A crashed upload leaves no data behind |

## Metadata log

The lease holder snapshots the state machine into `meta_snapshot` and deletes `meta_log` rows at or below the snapshot index. The SQL database therefore holds one snapshot plus the commands since it, not the full history.
