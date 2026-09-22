# Mink

Mink is lakehouse-native streaming storage on object storage. Log tables and primary-key tables over Kafka and Arrow Flight, history tiered into Iceberg and served with the live tail as one table.

[Documentation](website/pages/docs/index.md) · [Quick start](website/pages/docs/quick-start.md) · [Contribute](website/pages/docs/contribute.md)

## Install

```bash
cargo install --path mink/mink-cli
```

This puts the `mink` binary in `~/.cargo/bin`. Or run it in place with `cargo run -p mink-cli -- <args>`.

## Run a node

```bash
# single node: SQLite metadata log, local object storage, Kafka listener on
mink serve --kafka-listen 127.0.0.1:9092
```

Arrow Flight listens on `127.0.0.1:9123`. Every flag has a `MINK_*` env equivalent and a key in the optional `--config` TOML file. There is no authentication, listeners bound beyond loopback are expected on a private network.

## Docker

Skips the install, everything runs in compose:

```bash
cd harness

export MINK_IMAGE=ghcr.io/picomq/mink:latest MINK_QUERY_IMAGE=ghcr.io/picomq/mink-query:latest

docker compose up                                          # Postgres + RustFS + Iceberg REST, 1 node
docker compose -f compose.yml -f compose.cluster.yml up    # same stack, 3 nodes
docker compose -f compose.lite.yml up                      # SQLite + file://, no deps
docker compose -f compose.yml -f compose.query.yml up      # + mink-query, Flight SQL on :9130
```

Without the two variables `--build` builds the images from source.

```bash
```

Arrow Flight: `localhost:9123` (cluster also `:9124`, `:9125`). Kafka: `:9092` (`:9093`, `:9094`). Nodes advertise as `mink1`, `mink2`, `mink3`, the lite stack as `localhost`. RustFS console: `:9001`. Iceberg REST: `:8181`.

## Use it

```bash
mink db create shop
mink table create shop.orders -C "id BIGINT NOT NULL" -C "amount DECIMAL(10,2)" --primary-key id
echo '{"id": 1, "amount": "10.50"}' | mink write shop.orders
mink read shop.orders --mode snapshot
mink read shop.orders -f

echo 'hello' | kcat -P -b mink1:9092 -t greetings
kcat -C -b mink1:9092 -t greetings -o beginning -e

mink-query exec -e "SELECT sum(amount) FROM mink.shop.orders"
```

## Test

```bash
cargo test --workspace

# Postgres-backed tests, env-gated
MINK_PG_URL=postgres://user:pass@localhost:5432/mink \
    cargo test -p mink-sql --test pg

# end-to-end, in compose
scripts/e2e.sh lite
```
