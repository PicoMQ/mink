# Quick start

Run Mink against a local Kafka, add a topic as a table, and query it from DuckDB. The compose stack in the repository has everything Mink needs: Kafka, Schema Registry, Postgres and an S3-compatible bucket.

## Start the services

```bash
git clone https://github.com/PicoMQ/mink && cd mink
docker compose -f harness/compose.yml up -d postgres kafka rustfs schema-registry createbucket
```

| Service | Address |
| --- | --- |
| Kafka | `localhost:29092` |
| Schema Registry | `http://localhost:8081` |
| Postgres | `localhost:5432`, user, password and database `mink` |
| RustFS | `http://localhost:9000`, bucket `mink` |

## Run a node

```bash
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

| Listener | Address |
| --- | --- |
| REST catalog | `http://127.0.0.1:8181` |
| S3 endpoint | `http://127.0.0.1:9100` |
| Admin API and console | `http://127.0.0.1:9180` |

Every setting is a `MINK_*` variable, see [Configuration](/docs/operations/configuration). To build the binary instead, see [Contribute](/docs/contribute#build-and-test).

## Produce records

Create a topic with three partitions, and write three records with a JSON Schema.

```bash
docker exec mink-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic payments --partitions 3

docker exec -i mink-schema-registry kafka-json-schema-console-producer \
  --bootstrap-server kafka:9092 --topic payments \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema='{"type":"object","properties":{"id":{"type":"integer"},"item":{"type":"string"},"amount":{"type":"number"}}}' <<'EOF'
{"id":1,"item":"tea","amount":4.5}
{"id":2,"item":"bread","amount":3.2}
{"id":3,"item":"milk","amount":1.9}
EOF
```

## Add the topic as a table

```bash
docker exec mink mink table add payments
```

The table shows up in the catalog with its first snapshot, about 10 seconds later.

## Query it

In DuckDB 1.4 or later:

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1',
  ENDPOINT '127.0.0.1:9100', URL_STYLE 'path', USE_SSL false);
ATTACH 'warehouse' AS mink (TYPE iceberg, ENDPOINT 'http://127.0.0.1:8181', AUTHORIZATION_TYPE 'none');

SELECT _kafka_offset, id, item, amount FROM mink.merged.payments ORDER BY id;
```

| `_kafka_offset` | `id` | `item` | `amount` |
| --- | --- | --- | --- |
| 0 | 1 | tea | 4.5 |
| 1 | 2 | bread | 3.2 |
| 2 | 3 | milk | 1.9 |

Produce more records and run the query again. New records show up without waiting for a commit.

## Open the console

`http://127.0.0.1:9180` shows the table, its partitions, files, snapshots, schema and records. The SQL page runs the same queries in the browser, with the table named directly:

```sql
SELECT id, item, amount FROM payments WHERE amount > 2;
```
