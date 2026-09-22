# Configuration

A node is configured in three layers. Defaults, then the TOML file given by `--config`, then `MINK_*` environment variables, then `mink serve` flags. A later layer wins.

```bash
mink serve \
    --config /etc/mink/mink.toml \
    --node-id 2 \
    --listen 0.0.0.0:9123 --advertise grpc://node2.internal:9123 \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise node2.internal:9092 \
    --meta-url postgres://user:pass@pg:5432/mink \
    --storage-uri "-2@s3://bucket?region=us-east-1"
```

Environment variables map to top-level keys: `MINK_NODE_ID`, `MINK_META_URL`, `MINK_STORAGE_URI`. The `[kafka]` and `[lake]` tables are set in the file, and the two Kafka listener keys also have flags. Unknown keys fail startup. `mink cluster config` prints the effective result with secrets redacted.

## Identity

| Key | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `node_id` | `--node-id` | `1` | Stable identity in the cluster. Unique per node |
| `cluster_id` | `--cluster-id` | `mink` | Prefix for WAL objects and the Kafka cluster id. The same on every node |
| `data_dir` | `--data-dir` | `./data` | Local working directory. KV tablet stores and caches, all rebuildable from object storage |

The node epoch is not configured. Every start takes a clock-derived epoch higher than the last, which fences whatever the previous process left in flight.

## Listeners

| Key | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `listen` | `--listen` | `127.0.0.1:9123` | Arrow Flight bind address |
| `advertise` | `--advertise` | `grpc://127.0.0.1:9123` | The address clients and other nodes use to reach this node |
| `kafka.listen` | `--kafka-listen` | `127.0.0.1:9092` | Kafka bind address. The listener runs only when `[kafka]` is present or a Kafka flag is given |
| `kafka.advertise` | `--kafka-advertise` | The bind address | `host:port` returned to Kafka clients in metadata |

`advertise` and `kafka.advertise` are registered in the metadata log and handed to clients as is, in Flight redirects and in Kafka metadata responses. They have to be reachable addresses, not bind addresses. The defaults work for a single node on one host.

::: info Note
There is no authentication. Listeners bound beyond `127.0.0.1` are expected on a private network.
:::

## Metadata

`meta_url` selects the SQL database that holds the metadata log.

```bash
--meta-url sqlite::memory:                     # tests, throwaway
--meta-url sqlite:./data/meta.db               # single node
--meta-url postgres://user:pass@pg:5432/mink   # cluster
```

SQLite is a file, so one node. A cluster needs Postgres, and every node points at the same database. That is the membership mechanism. See [Metadata](/docs/design/metadata).

## Storage

`storage_uri` is the data bucket as `id@uri`. The id is the s3stream bucket id, any stable integer.

```bash
--storage-uri "-2@file://./data/objects"
--storage-uri "-2@s3://bucket?region=us-east-1"
--storage-uri "-2@s3://mink?region=us-east-1&endpoint=http://rustfs:9000&pathStyle=true"
```

| URI parameter | Purpose |
| --- | --- |
| `region` | S3 region |
| `endpoint` | S3 compatible stores such as RustFS or MinIO |
| `pathStyle=true` | Path-style addressing for stores that need it |
| `s3Express=true` | Force S3 Express One Zone session auth. Detected automatically from the `--x-s3` bucket suffix |

Credentials come from the standard `AWS_*` environment variables.

`wal_uri` (`--wal-uri`) puts the WAL in its own bucket. Unset, the WAL is the data bucket under id minus one. The WAL URI takes the batching parameters covered in [Tuning](/docs/operations/tuning).

## Background work

| Key | Default | Purpose |
| --- | --- | --- |
| `wal_upload_interval` | `10s` | How often buffered WAL is packed into stream objects |
| `kv_snapshot_interval` | `10m` | How often a primary-key bucket checkpoints its KV store to object storage |
| `snapshots_retained` | `1` | KV snapshots kept per bucket |
| `log_retention_interval` | `5m` | How often bucket leaders trim logs past `log_ttl` |
| `lease_ttl` | `30s` | Heartbeat and coordinator lease TTL. Renewed every quarter of it |
| `coordinator_tick` | `5s` | Coordinator loop: re-lead orphans, prune snapshots, expire offsets, schedule tiering |
| `tiering_poll_interval` | `30s` | How often an idle tiering worker asks for work |
| `default_bucket_count` | `1` | Buckets for a table created without `--bucket-count` |
| `producer_offsets_ttl` | `24h` | How long a sink's registered offsets live |
| `producer_offsets_cleanup_interval` | `1h` | How often expired producer offsets are swept |

Durations are humantime: `500ms`, `10s`, `5m`, `2h`, `7d`.

## Kafka

```toml
[kafka]
listen = "0.0.0.0:9092"
advertise = "node2.internal:9092"
database = "kafka"
default_partitions = 3
```

| Key | Default | Purpose |
| --- | --- | --- |
| `database` | `kafka` | The database topics live in as log tables |
| `auto_create_topics` | `true` | Create a topic on first produce or fetch |
| `default_partitions` | `1` | Buckets for an auto-created topic |
| `max_request_bytes` | `100 MiB` | Largest request accepted |
| `max_fetch_bytes` | `50 MiB` | Cap on one fetch response |
| `max_in_flight` | `64` | Requests in flight per connection |
| `max_wait` | `30s` | Longest a fetch waits for data |
| `min_session_timeout` | `6s` | Consumer group session bounds |
| `max_session_timeout` | `30m` | |
| `group_offsets_ttl` | `7d` | How long committed group offsets outlive the group |

The topic to table mapping and request coverage are in [Protocols](/docs/design/protocols#kafka).

## Lake

```toml
[lake]
format = "iceberg"
catalog = "rest"
uri = "http://iceberg-rest:8181"
warehouse = "s3://mink-lake"

[lake.properties]
"s3.endpoint" = "http://rustfs:9000"
"s3.region" = "us-east-1"
"s3.path-style-access" = "true"
"s3.access-key-id" = "mink"
"s3.secret-access-key" = "minkminkmink"
```

| Key | Values | Purpose |
| --- | --- | --- |
| `format` | `iceberg` | Lake format |
| `catalog` | `rest`, `memory` | `rest` is the Iceberg REST catalog at `uri`. `memory` is for tests |
| `uri` | | Catalog endpoint |
| `warehouse` | | Root location for table data |
| `properties` | | Passed to `iceberg-rust` as catalog and file IO properties |

Without `[lake]` the node accepts no table with `--lake`. With it, only tables created with `--lake iceberg` are tiered. `mink-query` takes the same block in its own config file. See [Tiering](/docs/design/tiering).

## Example

The file the compose stacks mount at `/etc/mink/mink.toml`, with the intervals the harness shortens for tests:

```toml
wal_upload_interval = "1s"
kv_snapshot_interval = "2s"
log_retention_interval = "1m"
lease_ttl = "10s"
coordinator_tick = "1s"
tiering_poll_interval = "2s"
default_bucket_count = 3

[lake]
format = "iceberg"
catalog = "rest"
uri = "http://iceberg-rest:8181"
warehouse = "s3://mink-lake"

[lake.properties]
"s3.endpoint" = "http://rustfs:9000"
"s3.region" = "us-east-1"
"s3.path-style-access" = "true"
"s3.access-key-id" = "mink"
"s3.secret-access-key" = "minkminkmink"

[kafka]
listen = "0.0.0.0:9092"
database = "kafka"
default_partitions = 3
```

Identity, listeners, metadata and storage come from the environment in that stack, so one file serves every node.
