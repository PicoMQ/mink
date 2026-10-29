# Tuning

Most settings trade one cost for another: files in a table against records read at load time, cache size against Kafka reads, fast failover against stalls. All settings are in [Configuration](/docs/operations/configuration).

## Freshness

Freshness needs no tuning. Every table load covers each partition up to its last stable offset, as the latest snapshot plus a tail (see [Snapshots](/docs/design/snapshots)). The settings below only decide how much of a load is tail.

| Setting | Default | Larger | Smaller |
| --- | --- | --- | --- |
| `file_window` | `60s` | Fewer, larger files. Longer tails. | More, smaller files. Shorter tails. |
| `snapshot_interval` | `10s` | Fewer commits and metadata objects. Longer tails. | More commits. Shorter tails. |

A tail reads its records from Kafka to take their statistics. Loads that see the same offsets share one tail, so many engines loading one table pay for it once.

## Files

A table's file count decides how much metadata engines read and how well filters prune. Small files prune well but cost more metadata. Assembly joins an ended hour's small files, so file count stays bounded on busy and quiet tables alike.

| Setting | Default | Effect |
| --- | --- | --- |
| `file_bytes` | `128 MiB` | Ends a file on size. Busy partitions reach it before `file_window`. |
| `file_window` | `60s` | Ends a file on time. Quiet partitions reach it first. |
| `granularity` | `hour` | The time bucket. `day` or `month` gives fewer, larger buckets for tables queried over long ranges. |
| `assembly_delay` | `10m` | How long after a bucket ends its files are assembled. A file that arrives later for that bucket is assembled on a later commit. |
| `assembled_bytes` | `1 GiB` | The largest assembled file |

A read of any range of a data file builds the whole file from Kafka on a cache miss. A larger `assembled_bytes` gives fewer files, and a cache miss on one of them reads more of Kafka.

## Cache

A cache miss on a data file reads its offset range from Kafka again. The cache decides how often that happens.

| Setting | Default | Effect |
| --- | --- | --- |
| `cache_bytes` | `1 GiB` | The memory tier |
| `cache_dir` | none | Turns on the disk tier, which keeps cached files across restarts |
| `cache_disk_bytes` | `50 GiB` | The disk tier's size |
| `cache_admission` | `reused` | `reused` keeps one-off scans off the disk. `fetched` writes every block. `evicted` writes only what leaves memory. |
| `max_builds` | the number of CPUs | Builds at once. Each holds a whole file in memory. |

Each data file has one owner among the `s3` nodes, so adding `s3` nodes adds cache. A node joining or leaving moves only the files it owns or would own. The hit ratio is in the `nestor_*` [metrics](/docs/operations/metrics#cache).

## Indexing

An indexer reads each committed record once. Adding nodes with the `indexer` role spreads partitions across them, see [Cluster](/docs/design/cluster).

| Setting | Default | Effect |
| --- | --- | --- |
| `max_partitions` | unlimited | Caps partition leases per node, so no node takes more than it can index |
| `history` | `all` | How far back a new table indexes. `none` or `{ since = ... }` skips old history. |
| `backfill_offsets` | `100000` | Offsets per backfill chunk. Live indexing never waits for backfill. |

`mink_indexed_records_total` by `source` shows live and backfill throughput. Lag per partition is on the table's Partitions tab and in `GET /admin/tables/{name}/partitions`.

## Failover

`lease_ttl` decides how fast a crashed node's partitions and writers move. Leases renew every third of it.

| Shorter `lease_ttl` | Longer `lease_ttl` |
| --- | --- |
| A crashed node's partitions get a new indexer sooner | A pause or network blip shorter than `lease_ttl` keeps a node's leases |
| More lease writes to Postgres | A crashed node's partitions wait longer |

A clean stop is unaffected, since the node releases its leases as it leaves.

## Retention

| Setting | Default | Effect |
| --- | --- | --- |
| `snapshot_retention` | `24h` | How far back time travel reaches. Longer keeps more metadata objects in the bucket and rows in Postgres. |
| `collection_grace` | `1h` | How long unused metadata objects wait before deletion. It must outlast the longest query. |
| `statistics_interval` | `1h` | How often a writer publishes distinct-value estimates. Each publish reads the table's sketches from Postgres and writes a statistics file to the bucket. |

Kafka's own retention decides how far back a table reaches. The bucket holds only metadata, so its size follows snapshots and files, not record bytes.
