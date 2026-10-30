# Admin API

Every node with the `admin` role serves the admin API, the [console](/docs/operations/console) and `/metrics` on `admin_address`. The API has no authentication, so it listens on loopback unless `admin_remote` is `true`. Put authentication in front of it before opening it up.

## Conventions

- Bodies are JSON with camelCase names. Times are milliseconds since the epoch, in fields ending in `AtMs`. Sizes are bytes.
- An error is `{"error": "<message>"}`.
- A list is `{"items": [...], "next": <cursor or null>}`. `limit` is 1 to 1000, 100 by default. Pass `next` as `cursor` for the following page.
- Any node answers for any other. A route that names another node is forwarded to it.

| Status | When |
| --- | --- |
| `400` | A parameter or body that does not parse |
| `404` | No such table, partition, file, snapshot, node or schema |
| `409` | A change that conflicts with the table's state |
| `502` | Kafka, the Schema Registry or another node did not answer |
| `503` | Postgres cannot be reached |

## Probes

| Route | Returns |
| --- | --- |
| `GET /health` | `200 ok` while the process runs |
| `GET /ready` | `200` when Postgres answers, `503` otherwise, with `{ready, nodeId, roles}` |
| `GET /metrics` | Prometheus text. See [Metrics](/docs/operations/metrics). |

## Cluster

| Route | Returns |
| --- | --- |
| `GET /admin/cluster` | This node's id, roles and version, the namespace, catalog URL, S3 endpoint and warehouse, the registry and whether it answers |
| `GET /admin/overview` | Table, node, record and byte counts, and the tables with the most lag, the most bytes and the oldest snapshot. Computed at most once a minute. |
| `GET /admin/nodes` | Live nodes with their roles, addresses and lease counts |
| `GET /admin/nodes/{id}` | One node, with its settings, secrets redacted, and its cache usage |
| `GET /admin/nodes/{id}/metrics` | That node's `/metrics` |
| `GET /admin/leases?table=&node=` | Leases with holder, epoch and expiry, filtered by table and node |

## Tables

| Route | Does |
| --- | --- |
| `GET /admin/tables?prefix=` | Lists tables with partitions, granularity, state, latest snapshot, records and bytes |
| `GET /admin/tables/{name}` | One table, with its location, partition specs, writer lease and lag |
| `POST /admin/tables` | Adds a table: `{"topic": "orders", "granularity": "day"}`. `granularity` is optional. Returns `201`. |
| `PATCH /admin/tables/{name}` | Changes the granularity: `{"granularity": "hour"}` |
| `DELETE /admin/tables/{name}` | Removes the table. Returns `202`. Its objects and rows are deleted once its leases expire. |
| `GET /admin/tables/{name}/partitions` | Each partition's earliest, indexed and last stable offsets, lag, backfill left and lease. Reads Kafka. |
| `GET /admin/tables/{name}/files?partition=&bucket=` | Live data files with offsets, bucket, spec, schema, parts, records and bytes |
| `GET /admin/tables/{name}/files/{file}` | One file, with its path and per-column statistics |
| `GET /admin/tables/{name}/snapshots` | Snapshots, newest first |
| `GET /admin/tables/{name}/snapshots/{id}` | One snapshot, with each partition's offsets and its metadata location |
| `GET /admin/tables/{name}/schemas` | Every table schema, and every schema reference records named, merged or rejected |

## Records

`GET /admin/tables/{name}/partitions/{index}/records?offset=&timestamp=&limit=` returns committed records from `offset`, or from the first offset at or after `timestamp`. `limit` is 1 to 500, 50 by default.

| Field | Value |
| --- | --- |
| `offset`, `timestampAtMs` | The record's offset and timestamp |
| `key`, `value` | `{data, encoding, bytes, truncated}`: UTF-8 text, or base64 for other bytes, at most 4 KiB |
| `headers` | `{key, value}` for each header |
| `row` | The record as the table reads it, field name to value |

Listing records reads Kafka and changes nothing.

## Registry

| Route | Returns |
| --- | --- |
| `GET /admin/registry` | Stored Schema Registry schemas with their key, format, references and the tables that use them |
| `GET /admin/registry/{key}` | One schema with its definition. A key names the registry and the schema, such as `confluent:42`. |

## Catalog and S3 for the console

A node with the `catalog` and `s3` roles also serves the REST catalog under `/catalog` and the S3 endpoint under `/s3` on the admin listener, for the console's SQL page. With `admin_remote`, this makes table data reachable through `admin_address`.
