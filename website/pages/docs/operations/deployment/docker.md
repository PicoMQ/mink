# Docker

A cluster is defined by two shared resources. Every node points at the same Postgres database and the same object storage bucket. There is no join procedure, no seed list and no quorum to size. A node started with the right `meta_url` and `storage_uri` registers itself and is part of the cluster.

<div class="mink-diagram">
<svg viewBox="0 0 680 250" width="680" role="img" aria-label="Three mink nodes share one Postgres metadata log and one object storage bucket. Clients reach the nodes over Arrow Flight and Kafka. mink-query reads the nodes and the Iceberg catalog. Tiering writes from the coordinator node to the catalog.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="10" y="20" width="120" height="50" class="box"/>
  <text x="70" y="41" text-anchor="middle" class="label">clients</text>
  <text x="70" y="58" text-anchor="middle" class="sub">Flight, Kafka</text>
  <rect x="10" y="180" width="120" height="50" class="box"/>
  <text x="70" y="201" text-anchor="middle" class="label">mink-query</text>
  <text x="70" y="218" text-anchor="middle" class="sub">Flight SQL</text>
  <rect x="190" y="20" width="130" height="50" class="box-accent"/>
  <text x="255" y="41" text-anchor="middle" class="label">mink1</text>
  <text x="255" y="58" text-anchor="middle" class="sub">:9123 :9092</text>
  <rect x="190" y="100" width="130" height="50" class="box-accent"/>
  <text x="255" y="121" text-anchor="middle" class="label">mink2</text>
  <text x="255" y="138" text-anchor="middle" class="sub">:9123 :9092</text>
  <rect x="190" y="180" width="130" height="50" class="box-accent"/>
  <text x="255" y="201" text-anchor="middle" class="label">mink3</text>
  <text x="255" y="218" text-anchor="middle" class="sub">:9123 :9092</text>
  <rect x="440" y="20" width="150" height="50" class="box"/>
  <text x="515" y="41" text-anchor="middle" class="label">Postgres</text>
  <text x="515" y="58" text-anchor="middle" class="sub">metadata log</text>
  <rect x="440" y="100" width="150" height="50" class="box"/>
  <text x="515" y="121" text-anchor="middle" class="label">object storage</text>
  <text x="515" y="138" text-anchor="middle" class="sub">WAL, objects</text>
  <rect x="440" y="180" width="150" height="50" class="box"/>
  <text x="515" y="201" text-anchor="middle" class="label">Iceberg catalog</text>
  <text x="515" y="218" text-anchor="middle" class="sub">tiered history</text>
  <path d="M130 45 L182 45" class="edge" marker-end="url(#arr)"/>
  <path d="M130 55 L182 120" class="edge" marker-end="url(#arr)"/>
  <path d="M130 62 L182 198" class="edge" marker-end="url(#arr)"/>
  <path d="M320 40 L432 40" class="edge" marker-end="url(#arr)"/>
  <path d="M320 115 L432 45" class="edge" marker-end="url(#arr)"/>
  <path d="M320 195 L432 50" class="edge" marker-end="url(#arr)"/>
  <path d="M320 55 L432 120" class="edge" marker-end="url(#arr)"/>
  <path d="M320 125 L432 125" class="edge" marker-end="url(#arr)"/>
  <path d="M320 200 L432 130" class="edge" marker-end="url(#arr)"/>
  <path d="M320 60 L432 200" class="edge-soft" marker-end="url(#arr)"/>
  <text x="380" y="140" text-anchor="middle" class="sub">tiering</text>
  <path d="M130 205 L182 210" class="edge-soft" marker-end="url(#arr)"/>
  <path d="M130 212 L432 218" class="edge-soft" marker-end="url(#arr)"/>
</svg>
</div>

## Single node

One node with SQLite and a local directory is a complete deployment.

```bash
mink serve --meta-url sqlite:./data/meta.db --storage-uri "-2@file://./data/objects"
```

The same node from the image, state in a named volume:

```bash
docker run -d --name mink -p 9123:9123 -p 9092:9092 -v mink-data:/data \
    ghcr.io/picomq/mink serve \
    --data-dir /data --meta-url sqlite:/data/meta.db --storage-uri "-2@file:///data/objects" \
    --listen 0.0.0.0:9123 --advertise grpc://localhost:9123 \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise localhost:9092
```

Durability follows the storage. With `file://` the data is as durable as that disk. The same single node against S3 has object store durability without Postgres, since SQLite only limits how many nodes share the metadata log.

## Cluster

Each node needs a unique `node_id`, the shared Postgres URL, the shared bucket, and `advertise` and `kafka.advertise` addresses that clients and the other nodes can reach.

```bash
mink serve --node-id 1 \
    --listen 0.0.0.0:9123 --advertise grpc://node1.internal:9123 \
    --kafka-listen 0.0.0.0:9092 --kafka-advertise node1.internal:9092 \
    --meta-url postgres://user:pass@pg:5432/mink \
    --storage-uri "-2@s3://mink?region=us-east-1"
```

Clients are redirected to a bucket's leader at its advertised address, and Kafka clients are sent the advertised broker list. Every node has to be directly reachable. A load balancer can be the bootstrap address, it cannot be the only reachable one.

## Images

Images are published to GitHub Container Registry on version tags, tagged `latest`, by version and by commit SHA. Both are multi-arch, `linux/amd64` and `linux/arm64`.

| Image | Binary | Ports |
| --- | --- | --- |
| `ghcr.io/picomq/mink` | `mink`, entrypoint `mink serve` | `9123` Flight, `9092` Kafka |
| `ghcr.io/picomq/mink-query` | `mink-query`, entrypoint `mink-query serve` | `9130` Flight SQL |

The node image holds only the binary and CA certificates. Configuration is the `MINK_*` environment or a mounted TOML file.

## Compose

The repository ships compose files under `harness/`. `MINK_IMAGE` and `MINK_QUERY_IMAGE` select the published images. Unset, the services build from source with `--build`.

```bash
cd harness
export MINK_IMAGE=ghcr.io/picomq/mink:latest MINK_QUERY_IMAGE=ghcr.io/picomq/mink-query:latest

docker compose up                                          # Postgres, RustFS, Iceberg REST, 1 node
docker compose -f compose.yml -f compose.cluster.yml up    # same, 3 nodes
docker compose -f compose.yml -f compose.query.yml up      # + mink-query
docker compose -f compose.lite.yml up                      # SQLite, file://, no dependencies
```

| Service | Host port | Advertised as |
| --- | --- | --- |
| `mink1` | `9123`, `9092` | `mink1` |
| `mink2`, `mink3` | `9124`, `9093` and `9125`, `9094` | `mink2`, `mink3` |
| `mink` (lite) | `9123`, `9092` | `localhost` |
| `query` | `9130` | |
| `rustfs` | `9000`, console `9001` | |
| `iceberg-rest` | `8181` | |

Host clients follow the advertised names, so `mink1` through `mink3` resolve through `/etc/hosts`, or the client runs inside the network with `docker compose exec mink1 mink ...`. Nodes read `harness/mink.toml` for the lake and the shortened intervals, see [Configuration](/docs/operations/configuration#example).

## Health

A node is live while its heartbeat row is younger than `lease_ttl`. `mink cluster health` reports registration and exits `1` when the node is not registered, which is the probe the compose files use:

```yaml
healthcheck:
  test: ["CMD", "mink", "-b", "grpc://127.0.0.1:9123", "cluster", "health"]
  interval: 2s
  timeout: 5s
  retries: 30
  start_period: 5s
```

`mink cluster describe` shows every node's liveness and load from any node, since the metadata view is the same everywhere.

## Restarts and upgrades

Restarting a node is safe at any moment. Acknowledged writes are in the WAL on object storage, and the new process registers at a higher epoch, which fences what the old one left in flight. Buckets it led come back in one of two ways: the restarted node re-leads them, or if it stays down past `lease_ttl`, the coordinator re-leads them to live nodes on its next tick and the new leader replays the WAL. Timeline in [Ownership and failover](/docs/design/ownership#failover).

Upgrades are rolling restarts, one node at a time, waiting for `cluster health` between nodes. After the last one, `mink cluster rebalance` evens leadership back out.

A node stopping cleanly releases its lease and expires its heartbeat on the way out, so failover starts on the next tick rather than after the TTL.

## Growing the cluster

Adding a node is starting one with a new id against the same database and bucket. It registers itself and takes new buckets round-robin from then on. Existing buckets stay where they are until `mink cluster rebalance` moves them. Removing a node is stopping it. Its buckets are re-led on the next tick, and its metadata row remains without ever leading again.
