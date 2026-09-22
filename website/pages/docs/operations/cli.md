# CLI

One binary. `mink` runs a node, administers a cluster, reads and writes tables and benchmarks them. Data goes to stdout, logs and status to stderr, so output pipes cleanly.

## Connecting

| Flag | Environment | Default |
| --- | --- | --- |
| `-b`, `--bootstrap` | `MINK_BOOTSTRAP` | `grpc://127.0.0.1:9123`, comma separated for several nodes |
| `--json` | | Off. Prints the protocol messages as JSON instead of formatted text |

Any node answers metadata and admin commands. Reads and writes follow redirects to the bucket leader. Logging is `RUST_LOG`, default `info`.

## Databases and tables

| Command | Purpose |
| --- | --- |
| `db list` | Database names |
| `db create <db> [--comment] [--property k=v] [--if-not-exists]` | Create |
| `db drop <db> [--if-exists] [--cascade]` | Drop, `--cascade` drops its tables |
| `table list <db>` | Table names |
| `table describe <db.table>` | Descriptor, schemas, bucket leaders |
| `table create <db.table> ...` | Create, flags below |
| `table alter <db.table> [--add-column "name TYPE"] [--set k=v] [--reset k]` | Add columns and change options, all or nothing |
| `table drop <db.table> [--if-exists]` | Drop |
| `partition list <db.table>` | Partitions |
| `partition create <db.table> k=v ...` | One `k=v` per partition key |
| `partition drop <db.table> k=v ...` | Drop a partition and its buckets |

`table create` flags:

| Flag | Value |
| --- | --- |
| `-C`, `--column` | `name TYPE [NOT NULL]`, in order, repeatable |
| `--primary-key` | `k` or `k,region`. Makes it a primary-key table |
| `--partition-by` | Partition key columns |
| `--bucket-key` | Columns rows are hashed by. Primary key minus partition keys when omitted |
| `--bucket-count` | Buckets per partition. The cluster `default_bucket_count` when omitted |
| `--log-format` | `arrow` |
| `--kv-format` | `compacted` or `indexed`, primary-key tables |
| `--merge-engine` | `first_row`, `aggregation` or `versioned:<column>` |
| `--changelog-image` | `full` or `wal` |
| `--log-ttl` | `7d`, `12h` or `forever` |
| `--lake` | `iceberg` |
| `--lake-freshness` | How far the lake may lag, `3m` |
| `--lake-auto-compaction` | Let the tiering worker rewrite small files |
| `--lake-attach` | Attach to an existing lake table instead of creating one |
| `--property` | Free-form `k=v`, repeatable |
| `--descriptor <file>` | A whole descriptor as JSON, what `describe --json` prints. Excludes every other flag |
| `--comment`, `--if-not-exists` | |

Options and their defaults are in [Tables](/docs/design/tables#options).

## Offsets and snapshots

| Command | Purpose |
| --- | --- |
| `table offsets <db.table> [--bucket n] [--partition p] [--at earliest\|latest\|<ts>]` | Log offsets per bucket. `--at` takes a millisecond or RFC 3339 timestamp and resolves it to an offset |
| `table lake-snapshot <db.table>` | The Iceberg snapshot id and the offset each bucket was tiered to |
| `table kv-snapshot <db.table> --bucket n [--partition p]` | Newest completed KV snapshot of a primary-key bucket |
| `producer-offsets get <id>` | A sink's registered start offsets |
| `producer-offsets delete <id>` | Forget them |

## Read and write

```bash
mink write shop.orders < rows.jsonl
mink read shop.orders --mode snapshot
mink read shop.events --from latest -f
```

| Flag | `read` | `write` |
| --- | --- | --- |
| `--bucket n`, `--partition p` | One bucket or partition, all when omitted | |
| `--mode log\|snapshot\|union` | `log` is the log or changelog, `snapshot` the current rows of a primary-key table, `union` lake history then log tail | |
| `--from earliest\|latest\|<offset>` | Log start position | |
| `-f`, `--follow` | Keep reading as rows arrive, log mode | |
| `-n`, `--limit` | Stop after this many rows | |
| `--meta` | Add `__bucket`, `__offset` and `__change` to each row, log mode | |
| `--delete` | | Delete the keys in the rows instead of upserting |
| `--columns a,b` | | Partial update. Only these columns are in the input, must include the primary key |
| `--batch-rows n` | | Rows per request, `1024` |

Input and output are JSON lines. Partitions are created on first write.

## Admin

| Command | Prints |
| --- | --- |
| `cluster describe` | Coordinator, counts of databases, tables, partitions and buckets, and every node with address, liveness, buckets led, epoch and protocols |
| `cluster health [--node addr]` | A node's id, epoch, registration, coordinator flag, hosted buckets and uptime. Exit `1` when the node is not registered |
| `cluster config [--node addr]` | A node's effective configuration, secrets redacted |
| `cluster stats [--node addr \| --all]` | Hosted buckets with epoch, offsets, writers, KV rows and snapshot offsets. On the coordinator, the tiering schedule |
| `cluster rebalance` | Moves bucket leadership from nodes above 110% of the mean load to nodes below 90%, and prints the moves |

`--node` defaults to the bootstrap node. `health` is the liveness probe the compose stacks use.

## Bench

```bash
mink bench shop.load --create 8 --mode upsert --key-space 100000 --duration 30s
```

| Flag | Default | Purpose |
| --- | --- | --- |
| `--mode append\|upsert` | `append` | Log appends or primary-key upserts |
| `--rows` | `100000` | Rows in total |
| `--batch` | `1000` | Rows per write |
| `--concurrency` | `4` | Concurrent writers |
| `--payload` | `64` | Bytes per row |
| `--key-space` | `0` | Upsert: ids wrap modulo this so later writes update earlier rows |
| `--start` | `0` | First id |
| `--duration` | | Stop after this long even if rows remain |
| `--create <buckets>` | | Create the table first with the bench schema |
| `--lake` | | With `--create`, tier the table |
| `--leader-wait` | `60s` | How long a write retries while a bucket has no leader |
| `--pause-ms` | `0` | Sleep between writes per writer |

The report has rows and bytes per second, acknowledged rows and batches, errors, stalls and latency percentiles. It uses the same client path as `write`, so redirects and leader waits are in the numbers.

## mink-query

`mink-query` is a second binary from `query/mink-query-cli`.

| Command | Purpose |
| --- | --- |
| `serve [--listen 0.0.0.0:9130]` | Flight SQL |
| `exec -e <sql>`, `exec -f <file>`, `exec < stdin` | Run statements, `--format table\|json\|csv` |
| `shell` | Interactive |

| Flag | Environment | Default |
| --- | --- | --- |
| `-b`, `--bootstrap` | `MINK_BOOTSTRAP` | `grpc://127.0.0.1:9123` |
| `-c`, `--config` | `MINK_QUERY_CONFIG` | None. TOML with `[lake]` and engine settings |
| `-d`, `--database` | `MINK_QUERY_DATABASE` | `default` |

Planning, pushdown and the engine settings are in [SQL](/docs/design/query).

## Serve

`mink serve` runs a node. Its flags, the `MINK_*` environment and the TOML file are in [Configuration](/docs/operations/configuration), deployment layouts in [Docker](/docs/operations/deployment/docker).
