# Mink

Mink serves Kafka topics as Iceberg tables, through a read-only Iceberg REST catalog and an S3 endpoint. The records stay in Kafka.

[Documentation](https://minkdb.com/docs/) · [Quick start](https://minkdb.com/docs/quick-start) · [Contribute](https://minkdb.com/docs/contribute)

- **`mink/`** the node: Kafka client, schemas, Postgres store, Iceberg files and metadata, metrics, and the `mink` CLI
- **`console/`** the web console, embedded in the node
- **`specification/`** the files and metadata Mink writes

## Install

```bash
(cd console && npm ci && npm run build)
cargo install --path mink/mink-cli
```

The console is embedded in the binary, so it builds first.

## Run a node

```bash
# Kafka, Schema Registry, Postgres and RustFS
docker compose -f harness/compose.yml up -d postgres kafka rustfs schema-registry createbucket

mink serve -c mink.toml
```

```toml
# mink.toml
[kafka]
bootstrap = ["localhost:29092"]

[storage]
endpoint = "http://localhost:9000"
access_key_id = "mink"
secret_access_key = "minkminkmink"

[schema_registry]
url = "http://localhost:8081"
```

Catalog on `:8181`, S3 on `:9100`, admin API and console on `:9180`. Every setting has a `MINK_*` env equivalent.

## Docker

```bash
docker build -t mink .
```

Running it against the compose stack, and as a cluster: [Deployment](https://minkdb.com/docs/operations/deployment).

## Use it

```bash
mink table add payments -c mink.toml
mink table list -c mink.toml
mink granularity payments day -c mink.toml
```

```sql
-- DuckDB
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1',
  ENDPOINT '127.0.0.1:9100', URL_STYLE 'path', USE_SSL false);
ATTACH 'warehouse' AS mink (TYPE iceberg, ENDPOINT 'http://127.0.0.1:8181', AUTHORIZATION_TYPE 'none');

SELECT * FROM mink.kafka.payments;
```

## Test

```bash
cargo test

# service-backed tests, env-gated
TEST_POSTGRES_URL=postgres://mink:mink@localhost:5432/mink \
TEST_KAFKA_BOOTSTRAP=localhost:29092 \
TEST_S3_ENDPOINT=http://localhost:9000 \
TEST_SCHEMA_REGISTRY=http://localhost:8081 \
    cargo test

# end to end in Docker: DuckDB, PyIceberg, Spark, Trino and the console
scripts/e2e.sh
```
