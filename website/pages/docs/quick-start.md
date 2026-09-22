# Quick start

A node needs a metadata database and an object store. For a first run both can be local files, so nothing has to be installed besides the binary.

## Install

Build the `mink` binary from source with the Rust toolchain:

```bash
git clone https://github.com/PicoMQ/mink && cd mink
cargo install --path mink/mink-cli
```

This puts `mink` in `~/.cargo/bin`, which cargo adds to the `PATH`. To build without installing, use `cargo build --release -p mink-cli` and run `./target/release/mink`. The [Docker](#docker) section below skips the host install entirely.

## Run a node

```bash
# single node: SQLite metadata log, local object storage, Kafka listener on
mink serve --kafka-listen 127.0.0.1:9092
```

Defaults are `sqlite:./data/meta.db` for metadata and `-2@file://./data/objects` for the object store. Arrow Flight listens on `127.0.0.1:9123`. The Kafka listener is off unless `--kafka-listen` is given. Every flag has a `MINK_*` environment variable equivalent and a key in the optional `--config` TOML file. Flags override the environment, the environment overrides the file.

Against real infrastructure the same command points at Postgres and an S3 bucket:

```bash
mink serve \
    --node-id 2 \
    --listen 0.0.0.0:9123 --advertise grpc://node2.internal:9123 \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise node2.internal:9092 \
    --meta-url postgres://user:pass@pg:5432/mink \
    --storage-uri "-2@s3://bucket?region=us-east-1"
```

The `-2@` prefix is the s3stream bucket id. The WAL bucket defaults to id minus one on the same URI. `--wal-uri` sets it explicitly.

::: info Note
There is no authentication. Listeners bound beyond `127.0.0.1` are expected on a private network.
:::

<div class="mink-or">or</div>

## Docker

The `harness` compose files start everything in one command, including Postgres, RustFS as the object store and an Iceberg REST catalog:

```bash
cd harness

export MINK_IMAGE=ghcr.io/picomq/mink:latest MINK_QUERY_IMAGE=ghcr.io/picomq/mink-query:latest

docker compose up                                          # Postgres + RustFS + Iceberg REST, 1 node
docker compose -f compose.yml -f compose.cluster.yml up    # same stack, 3 nodes
docker compose -f compose.lite.yml up                      # SQLite + file://, no deps
```

The two variables select the published images. Without them `docker compose up --build` builds from source.

| Stack | Arrow Flight | Kafka | Advertised as |
| --- | --- | --- | --- |
| `compose.yml` | `:9123` | `:9092` | `mink1` |
| `compose.cluster.yml` | `:9124`, `:9125` | `:9093`, `:9094` | `mink2`, `mink3` |
| `compose.lite.yml` | `:9123` | `:9092` | `localhost` |

Clients follow the advertised name. From the host, `mink1` resolves through `/etc/hosts`, or the client runs inside the network with `docker compose exec mink1 mink ...`. `-f compose.query.yml` adds `mink-query` with Flight SQL on `:9130`. The full service list is in [Docker](/docs/operations/deployment/docker#compose).

The one-node and cluster stacks read `harness/mink.toml`, which points tiering at the Iceberg REST catalog and the `s3://mink-lake` warehouse. Tables opt in with `--lake iceberg`. The lite stack has no lakehouse.

## First table

The `mink` binary is also the client. Commands default to `grpc://127.0.0.1:9123` and take `-b <address>` or `MINK_BOOTSTRAP` for another node.

```bash
mink db create shop

mink table create shop.orders \
    -C "id BIGINT NOT NULL" \
    -C "region STRING NOT NULL" \
    -C "amount DECIMAL(10,2)" \
    --primary-key id,region \
    --partition-by region \
    --bucket-count 2

mink table create shop.events \
    -C "id BIGINT NOT NULL" \
    -C "note STRING" \
    --bucket-key id
```

`shop.orders` is a primary-key table partitioned by `region`. `shop.events` is a log table: no primary key, rows routed by the hash of `id`. Columns are `name TYPE [NOT NULL]`. Types are the [logical types](/docs/design/tables#types) such as `INT`, `BIGINT`, `STRING`, `DECIMAL(p,s)`, `TIMESTAMP(3)`, `ARRAY<INT>`.

### Write

`mink write` reads JSON lines from stdin. Partitions are created on first write.

```bash
mink write shop.orders <<'EOF'
{"id": 1, "region": "us", "amount": "10.50"}
{"id": 2, "region": "eu", "amount": "3.00"}
{"id": 1, "region": "us", "amount": "11.00"}
EOF

mink write shop.events <<'EOF'
{"id": 7, "note": "a"}
{"id": 8, "note": "b"}
EOF
```

The third order row upserts the first. `--delete` turns the input rows into deletes. `--columns id,region,amount` writes a partial update. The primary-key columns are always part of it.

### Read

```bash
mink read shop.orders --mode snapshot          # latest row per key
mink read shop.orders --meta --partition us    # changelog with __bucket, __offset, __change
mink read shop.events -n 2                     # first two log rows
mink read shop.events -f                       # tail
mink table offsets shop.events
```

`--mode log` (default) reads the bucket log, `snapshot` reads the KV state, `union` reads the lake snapshot plus the log tail once the table is tiered. `--from earliest|latest|<offset>` sets the log start position. `mink table offsets <path> --at <timestamp>` resolves a timestamp to an offset.

Output for the changelog read:

```text
{"id":1,"region":"us","amount":10.50,"__bucket":"p1/b0","__offset":0,"__change":"+I"}
{"id":1,"region":"us","amount":10.50,"__bucket":"p1/b0","__offset":1,"__change":"-U"}
{"id":1,"region":"us","amount":11.00,"__bucket":"p1/b0","__offset":2,"__change":"+U"}
```

## Kafka

Any Kafka client works against the Kafka listener. Topics live in the `kafka` database as log tables with `key`, `value`, `headers` and `timestamp` columns. A topic is created on first use with `default_partitions` buckets.

```bash
echo 'hello' | kcat -P -b mink1:9092 -t greetings
kcat -C -b mink1:9092 -t greetings -o beginning -e

mink read kafka.greetings
```

`value` is `BYTES` and prints hex encoded: `68656c6c6f` is `hello`. Consumer groups, offset commits and idempotent producers are supported. Transactions are not.

## SQL

`mink-query` runs DataFusion over the cluster and, when configured, the Iceberg lakehouse.

```bash
cargo install --path query/mink-query-cli

mink-query exec -e "SELECT region, sum(amount) FROM mink.shop.orders GROUP BY region"
mink-query shell
mink-query serve          # Flight SQL on 0.0.0.0:9130
```

In compose the engine is the `query` service: `docker compose exec query mink-query exec -e "..."`.

Tables appear under the `mink` catalog as `mink.<database>.<table>`. A query over a tiered table reads the lake snapshot plus the log tail. Over an untiered table it reads the log or the KV snapshot. [SQL](/docs/design/query) covers planning and pushdown.

## Lakehouse

`--lake iceberg` tiers a table into the `[lake]` warehouse. `--lake-freshness` bounds how far the lake lags, `3m` by default. Iceberg bucketing hashes one key column, so a tiered primary-key table has a single-column primary key.

```bash
mink table create shop.bills \
    -C "id BIGINT NOT NULL" \
    -C "region STRING" \
    -C "amount DECIMAL(10,2)" \
    --primary-key id \
    --lake iceberg \
    --lake-freshness 5s

mink write shop.bills <<'EOF'
{"id": 1, "region": "us", "amount": "10.50"}
{"id": 1, "region": "us", "amount": "11.00"}
EOF

mink table lake-snapshot shop.bills
mink read shop.bills --mode union
mink-query exec -e "SELECT id, region, amount FROM mink.shop.bills"
```

`lake-snapshot` prints the Iceberg snapshot id and the offset each bucket was tiered to. The union read and the query return the lake snapshot merged with the changelog after that offset: one row, `amount` `11.00`.

## Next

- [Design overview](/docs/design/overview) for how tables, tablets and tiering fit together.
- [Why not Apache Fluss](/docs/faq/fluss) for a comparison with the project Mink is modelled on.
