# Mink

Mink serves Kafka topics as Iceberg tables, through a read-only Iceberg REST catalog and an S3 endpoint, with older records tiered into Iceberg and read as one table.

[Documentation](https://minkdb.com/docs/) · [Quick start](https://minkdb.com/docs/quick-start) · [Contribute](https://minkdb.com/docs/contribute)

- **`mink/`** the node: Kafka client, schemas, Postgres store, Iceberg files and metadata, metrics, and the `mink` CLI
- **`console/`** the web console, embedded in the node
- **`conformance/`** the fixtures that pin the [file formats](https://minkdb.com/docs/formats)

## Run

```bash
# Kafka, Schema Registry, Postgres and RustFS
docker compose -f harness/compose.yml up -d postgres kafka rustfs schema-registry createbucket

docker run -d --name mink --network harness_default \
  -p 8181:8181 -p 9100:9100 -p 9180:9180 \
  -e MINK_POSTGRES=postgres://mink:mink@postgres:5432/mink \
  -e MINK_KAFKA__BOOTSTRAP='["kafka:9092"]' \
  -e MINK_STORAGE__ENDPOINT=http://rustfs:9000 \
  -e MINK_STORAGE__CREDENTIALS__ACCESS_KEY_ID=mink \
  -e MINK_STORAGE__CREDENTIALS__SECRET_ACCESS_KEY=minkminkmink \
  -e MINK_SCHEMA_REGISTRY__TYPE=confluent \
  -e MINK_SCHEMA_REGISTRY__URL=http://schema-registry:8081 \
  -e MINK_CATALOG_ADDRESS=0.0.0.0:8181 \
  -e MINK_S3_ADDRESS=0.0.0.0:9100 \
  -e MINK_ADMIN_ADDRESS=0.0.0.0:9180 \
  -e MINK_ADMIN_REMOTE=true \
  ghcr.io/picomq/mink
```

Catalog on `:8181`, S3 on `:9100`, admin API and console on `:9180`. Running a cluster: [Deployment](https://minkdb.com/docs/operations/deployment).

## Use it

```bash
docker exec mink mink table add payments
docker exec mink mink table list
docker exec mink mink granularity payments day
```

```sql
-- DuckDB
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1',
  ENDPOINT '127.0.0.1:9100', URL_STYLE 'path', USE_SSL false);
ATTACH 'warehouse' AS mink (TYPE iceberg, URI 'http://127.0.0.1:8181', AUTHORIZATION_TYPE 'none');

SELECT * FROM mink.merged.payments;
```

## Build from source

```bash
(cd console && npm ci && npm run build)
cargo install --path mink/mink-cli

docker build -t mink .
```

The console is embedded in the binary, so it builds first.

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
