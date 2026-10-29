# Deployment

A Mink cluster is a set of nodes that share one Postgres, one bucket and one Kafka cluster. There is no join step and no seed list. A node that starts with the same `postgres` setting is part of the cluster.

| A node needs | For |
| --- | --- |
| Postgres | All state: tables, indexed ranges, files, snapshots, leases and members. Mink creates its tables at startup. |
| A bucket on an S3-compatible store | Iceberg metadata only, under `warehouse`. Data files are never written to it. The bucket must exist. |
| Kafka | The topics it serves, read as a `read_committed` consumer |
| A Schema Registry | Optional. Typed columns for Avro, Protobuf and JSON Schema records. |

## Image

One image is published to GitHub Container Registry for every release, for `amd64` and `arm64`.

| Image | Tags | Contents |
| --- | --- | --- |
| `ghcr.io/picomq/mink` | `latest`, version, major.minor, commit SHA | The `mink` binary with the console embedded. Runs `mink serve`. |

The Dockerfile at the repository root builds the same image from source:

```bash
docker build -t mink .
```

A node takes its settings from `MINK_*` environment variables or a config file mounted with `-c`, see [Configuration](/docs/operations/configuration).

## Single node

With the [Quick start](/docs/quick-start) services running:

```bash
docker run -d --name mink --network harness_default \
  -p 8181:8181 -p 9100:9100 -p 9180:9180 \
  -e MINK_POSTGRES=postgres://mink:mink@postgres:5432/mink \
  -e MINK_KAFKA_BOOTSTRAP='["kafka:9092"]' \
  -e MINK_STORAGE='{endpoint = "http://rustfs:9000"}' \
  -e MINK_SCHEMA_REGISTRY='{url = "http://schema-registry:8081"}' \
  -e MINK_CATALOG_ADDRESS=0.0.0.0:8181 \
  -e MINK_S3_ADDRESS=0.0.0.0:9100 \
  -e MINK_ADMIN_ADDRESS=0.0.0.0:9180 \
  -e MINK_ADMIN_REMOTE=true \
  ghcr.io/picomq/mink
```

The listeners default to loopback, so a container binds `0.0.0.0`. The admin API has no authentication, so `admin_remote` must be set to bind it beyond loopback. Engines on the host reach the catalog at `http://127.0.0.1:8181`, the defaults for `catalog_endpoint` and `s3_endpoint`.

## Cluster

Each node sets the addresses other nodes and engines reach it at.

| Setting | Value |
| --- | --- |
| `advertised_address` | This node's S3 endpoint as other nodes reach it. Reads of data files this node owns are forwarded here. |
| `advertised_admin_address` | This node's admin listener as other nodes reach it |
| `catalog_endpoint` | The catalog URL as engines reach it |
| `s3_endpoint` | The S3 endpoint URL as engines reach it. The catalog hands it to engines with every table. |

Two nodes against the Quick start services, saved as `mink.yml`:

```yaml
x-mink: &mink
  image: ghcr.io/picomq/mink
  environment: &env
    MINK_POSTGRES: postgres://mink:mink@postgres:5432/mink
    MINK_KAFKA_BOOTSTRAP: '["kafka:9092"]'
    MINK_STORAGE: '{endpoint = "http://rustfs:9000"}'
    MINK_SCHEMA_REGISTRY: '{url = "http://schema-registry:8081"}'
    MINK_CATALOG_ADDRESS: 0.0.0.0:8181
    MINK_S3_ADDRESS: 0.0.0.0:9100
    MINK_ADMIN_ADDRESS: 0.0.0.0:9180
    MINK_ADMIN_REMOTE: "true"
    MINK_CACHE_DIR: /cache

services:
  mink-1:
    <<: *mink
    environment:
      <<: *env
      MINK_ADVERTISED_ADDRESS: http://mink-1:9100
      MINK_ADVERTISED_ADMIN_ADDRESS: http://mink-1:9180
    volumes: [mink-1-cache:/cache]
    ports: ["8181:8181", "9100:9100", "9180:9180"]

  mink-2:
    <<: *mink
    environment:
      <<: *env
      MINK_ADVERTISED_ADDRESS: http://mink-2:9100
      MINK_ADVERTISED_ADMIN_ADDRESS: http://mink-2:9180
      MINK_CATALOG_ENDPOINT: http://127.0.0.1:8182
      MINK_S3_ENDPOINT: http://127.0.0.1:9101
    volumes: [mink-2-cache:/cache]
    ports: ["8182:8181", "9101:9100", "9181:9180"]

volumes:
  mink-1-cache:
  mink-2-cache:
```

```bash
docker compose -f harness/compose.yml -f mink.yml up -d mink-1 mink-2
```

The nodes split the partitions and writers between them within a few lease rounds, see [Cluster](/docs/design/cluster). An engine can use either node. A data file read on a node that does not own it is forwarded to its owner.

### Roles

Every node runs every role by default. `roles` splits them.

| Nodes | Roles | Reached by |
| --- | --- | --- |
| Serving | `catalog`, `s3`, `admin` | Engines, through a load balancer for `catalog_endpoint` and `s3_endpoint` |
| Indexing | `indexer`, `admin` | Prometheus only, on the admin listener |

Any `catalog` node answers any table load, and any `s3` node answers any read, so a load balancer needs no affinity. The console's SQL page needs `catalog` and `s3` on the node that serves it.

## Health and readiness

| Probe | Use |
| --- | --- |
| `GET /health` | Liveness. `200` while the process runs. |
| `GET /ready` | Readiness. `200` while Postgres answers, `503` otherwise. |

Both are on the admin listener. Scrape `/metrics` on every node, see [Metrics](/docs/operations/metrics).

## Restarts and upgrades

Restarting a node is safe at any moment. Mink keeps no record bytes, so nothing is lost when a node stops.

| The node | Its leases |
| --- | --- |
| Gets `SIGTERM` or `SIGINT` | Released at once. Other nodes take them in their next round. |
| Crashes or loses its network | Taken by other nodes once they expire, after `lease_ttl` |

With a disk cache on a volume, a restarted node serves the files it cached without building them again.
