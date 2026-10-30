# Configuration

A node reads its settings from the defaults, then a TOML file given with `-c`, then `MINK_*` environment variables. An unknown setting fails startup.

```toml
postgres = "postgres://mink:mink@db:5432/mink"
warehouse = "s3://lake/mink"

[kafka]
bootstrap = ["kafka-1:9092", "kafka-2:9092"]

[storage]
region = "us-east-1"

[schema_registry]
url = "http://registry:8081"
```

Every setting has a variable: `MINK_` and its name in capitals, with `__` between a table and its keys. Lists take their TOML form.

```bash
MINK_POSTGRES=postgres://mink:mink@db:5432/mink
MINK_KAFKA__BOOTSTRAP='["kafka-1:9092", "kafka-2:9092"]'
MINK_KAFKA__SASL__PASSWORD=secret
MINK_SCHEMA_REGISTRY__URL=http://registry:8081
```

Durations are a number and a unit: `ms`, `s`, `m` or `h`. Sizes are bytes. Every duration and size must be positive.

## Roles and listeners

| Setting | Default | Purpose |
| --- | --- | --- |
| `roles` | `["catalog", "s3", "indexer", "admin"]` | Roles this node runs. See [Overview](/docs/design/overview). |
| `catalog_address` | `127.0.0.1:8181` | Where the REST catalog listens |
| `s3_address` | `127.0.0.1:9100` | Where the S3 endpoint listens |
| `admin_address` | `127.0.0.1:9180` | Where the admin API, the console and `/metrics` listen |
| `admin_remote` | `false` | Allow an `admin_address` that is not loopback. The admin API has no authentication. |

## Addresses others reach

| Setting | Default | Purpose |
| --- | --- | --- |
| `catalog_endpoint` | `http://127.0.0.1:8181` | The catalog URL as engines reach it, shown on the console's Connect page |
| `s3_endpoint` | `http://127.0.0.1:9100` | The S3 endpoint URL as engines reach it, handed out by the catalog |
| `advertised_address` | `http://{s3_address}` | The URL other nodes forward data file reads to |
| `advertised_admin_address` | `http://{admin_address}` | The URL other nodes forward admin requests to |

## Services

| Setting | Default | Purpose |
| --- | --- | --- |
| `postgres` | `postgres://mink:mink@localhost:5432/mink` | Postgres connection URL. TLS is set in the URL: `sslmode=verify-full` checks the server's certificate against `sslrootcert`, or the Mozilla CA certificates without it. `require` encrypts without checking the certificate. |
| `kafka.bootstrap` | `["localhost:9092"]` | Kafka brokers to bootstrap from |
| `kafka.tls` | none | TLS to the brokers, verified against the system's CA certificates. `{}` turns it on. |
| `kafka.tls.ca` | none | A PEM file of more CA certificates to trust |
| `kafka.sasl.mechanism` | none | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`, as Kafka names them |
| `kafka.sasl.username`, `kafka.sasl.password` | none | The SASL credentials |
| `warehouse` | `s3://mink/warehouse` | Where Iceberg metadata lives, and the root of every table location |
| `storage.endpoint` | none | An S3-compatible store such as RustFS or MinIO, addressed in path style. Without it, AWS S3 in `storage.region`. |
| `storage.region` | `us-east-1` | The bucket's region |
| `storage.credentials` | none | `{ access_key_id, secret_access_key }`. Without them, AWS's own sources: the `AWS_*` environment variables, web identity, the ECS task role, EKS Pod Identity, or the instance profile. |
| `schema_registry.url` | none | The Schema Registry. Without it, new records keep their raw bytes. |
| `schema_registry.username`, `schema_registry.password` | none | Basic authentication for the registry |
| `namespace` | `kafka` | The catalog's one namespace |

## Indexing

| Setting | Default | Purpose |
| --- | --- | --- |
| `history` | `all` | How far backfill goes: `all`, `none`, or `{ since = "<RFC 3339 time>" }` |
| `file_bytes` | `134217728` | A range of records becomes files when their key, value and header bytes reach this |
| `file_window` | `60s` | Or when their timestamps span this, or its first record was read this long ago |
| `backfill_offsets` | `100000` | Offsets per backfill chunk |
| `retention_check` | `60s` | How often indexers check Kafka's earliest offsets |
| `granularity` | `hour` | Time partitioning for tables added without one: `hour`, `day`, `month` or `none` |
| `max_partitions` | unlimited | The most partition leases one node holds |
| `lease_ttl` | `30s` | Lease lifetime. Leases renew every third of it. |

## Snapshots and retention

| Setting | Default | Purpose |
| --- | --- | --- |
| `snapshot_interval` | `10s` | How often a writer commits |
| `snapshot_retention` | `24h` | How long snapshots are kept |
| `collection_interval` | `10m` | How often a writer deletes unused metadata objects |
| `collection_grace` | `1h` | How long an unused object waits before deletion |
| `assembly_delay` | `10m` | How long after its end a time bucket is assembled |
| `assembled_bytes` | `1073741824` | The largest assembled file |
| `statistics_interval` | `1h` | How often a writer publishes a statistics file. Must be shorter than `snapshot_retention`. |

## Cache

| Setting | Default | Purpose |
| --- | --- | --- |
| `cache_bytes` | `1073741824` | Memory for built data files and metadata objects |
| `cache_dir` | none | Directory of the disk tier. Without it, the cache has no disk tier. |
| `cache_disk_bytes` | `53687091200` | Size of the disk tier |
| `cache_admission` | `reused` | Which blocks go to disk: `reused`, `fetched` or `evicted`. See [Reads](/docs/design/reads#cache). |
| `max_builds` | the number of CPUs | Data files the node builds at once |

## Console

| Setting | Default | Purpose |
| --- | --- | --- |
| `duckdb_engine` | none, jsDelivr | Base URL of the DuckDB-Wasm files the SQL page loads |
| `duckdb_extensions` | none, DuckDB's own | DuckDB extension repository for the SQL page. A mirror holds `iceberg`, `httpfs`, `parquet`, `avro` and `json`. |

The SQL page runs only DuckDB files whose hashes match the version the console was built with. See [Console](/docs/operations/console).
