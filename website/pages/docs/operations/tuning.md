# Tuning

These settings trade one cost against another. Every setting is in [Configuration](/docs/operations/configuration).

## Freshness

Freshness needs no tuning. A load covers each partition up to its last stable offset, see [Snapshots](/docs/design/snapshots). The settings below decide how much of a load is a tail.

| Setting | Larger | Smaller |
| --- | --- | --- |
| `file_window` | Fewer, larger files. Longer tails. | More, smaller files. Shorter tails. |
| `snapshot_interval` | Fewer commits and metadata objects. Longer tails. | More commits. Shorter tails. |

## Files

A table's file count decides how much metadata engines read and how well filters prune. Small files prune well but cost more metadata. Assembly joins an ended time bucket's small files. Under `none`, files are never assembled.

| Setting | Effect |
| --- | --- |
| `file_bytes` | Ends a file on size. Busy partitions reach it before `file_window`. |
| `file_window` | Ends a file on time. Quiet partitions reach it first. |
| `granularity` | The time bucket. `day` or `month` gives fewer, larger buckets for tables queried over long ranges. |
| `assembly_delay` | How long after a bucket ends its files are assembled. A file that arrives later for that bucket is assembled on a later commit. |
| `assembled_bytes` | The largest assembled file |

A read of any range of a data file builds the whole file from Kafka on a cache miss. A larger `assembled_bytes` gives fewer files, and a cache miss on one of them reads more of Kafka.

## Cache

A cache miss on a data file reads its offset range from Kafka again. The cache decides how often that happens.

| Setting | Effect |
| --- | --- |
| `cache_bytes` | The memory tier |
| `cache_dir` | Turns on the disk tier, which keeps cached files across restarts |
| `cache_disk_bytes` | The disk tier's size |
| `cache_admission` | `reused` keeps one-off scans off the disk. `fetched` writes every block. `evicted` writes only what leaves memory. |
| `max_builds` | Builds at once. Each holds a whole file in memory. |

Adding `s3` nodes adds cache, see [Reads](/docs/design/reads). The hit ratio is in the `nestor_*` [metrics](/docs/operations/metrics#cache).

## Indexing

An indexer reads each committed record once. Adding nodes with the `indexer` role spreads partitions across them, see [Cluster](/docs/design/cluster).

| Setting | Effect |
| --- | --- |
| `max_partitions` | Caps partition leases per node |
| `history` | How far back a new table indexes. `none` or `{ since = ... }` skips old history. |
| `backfill_offsets` | Offsets per backfill chunk. Live indexing never waits for backfill. |

`mink_indexed_records_total` by `source` shows live and backfill throughput. Lag per partition is on the table's Partitions tab and in `GET /admin/tables/{name}/partitions`.

## Failover

`lease_ttl` decides how fast a crashed node's partitions and writers move. Leases renew every third of it.

| Shorter `lease_ttl` | Longer `lease_ttl` |
| --- | --- |
| A crashed node's partitions get a new indexer sooner | A pause or network blip shorter than `lease_ttl` keeps a node's leases |
| More lease writes to Postgres | A crashed node's partitions wait longer |

## Retention

| Setting | Effect |
| --- | --- |
| `snapshot_retention` | How far back time travel reaches. Longer keeps more metadata objects in the bucket and rows in Postgres. |
| `collection_grace` | How long unused metadata objects wait before deletion. It must outlast the longest query. |
| `statistics_interval` | How often a writer publishes distinct-value estimates. Each publish reads the table's sketches from Postgres and writes a statistics file to the bucket. |
