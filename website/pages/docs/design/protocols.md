# Protocols

Two listeners front the same node service. Arrow Flight is the native protocol: typed Arrow batches, lookups and admin over gRPC. The Kafka listener maps the Kafka wire protocol onto log tables in one database. Flight SQL is served separately by `mink-query`.

<div class="mink-diagram">
<svg viewBox="0 0 680 200" width="680" role="img" aria-label="Arrow Flight and Kafka listeners on a node share the node service. mink-query serves Flight SQL and talks to nodes over Arrow Flight.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="10" y="20" width="150" height="50" class="box"/>
  <text x="85" y="41" text-anchor="middle" class="label">Flight client</text>
  <text x="85" y="58" text-anchor="middle" class="sub">mink CLI, Rust</text>
  <rect x="10" y="130" width="150" height="50" class="box"/>
  <text x="85" y="151" text-anchor="middle" class="label">Kafka client</text>
  <text x="85" y="168" text-anchor="middle" class="sub">any</text>
  <rect x="10" y="75" width="150" height="50" class="box"/>
  <text x="85" y="96" text-anchor="middle" class="label">SQL client</text>
  <text x="85" y="113" text-anchor="middle" class="sub">Flight SQL, JDBC</text>
  <rect x="220" y="75" width="150" height="50" class="box"/>
  <text x="295" y="96" text-anchor="middle" class="label">mink-query</text>
  <text x="295" y="113" text-anchor="middle" class="sub">Flight SQL :9130</text>
  <rect x="450" y="20" width="210" height="50" class="box"/>
  <text x="555" y="41" text-anchor="middle" class="label">Arrow Flight :9123</text>
  <text x="555" y="58" text-anchor="middle" class="sub">DoPut, DoGet, actions</text>
  <rect x="450" y="130" width="210" height="50" class="box"/>
  <text x="555" y="151" text-anchor="middle" class="label">Kafka :9092</text>
  <text x="555" y="168" text-anchor="middle" class="sub">produce, fetch, groups</text>
  <path d="M160 45 L442 45" class="edge" marker-end="url(#arr)"/>
  <path d="M160 155 L442 155" class="edge" marker-end="url(#arr)"/>
  <path d="M160 100 L212 100" class="edge" marker-end="url(#arr)"/>
  <path d="M370 100 L442 60" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

## Arrow Flight

Every message body is JSON in the Flight ticket, descriptor or action body. Every row payload is Arrow IPC. Bucket-addressed calls must land on the bucket's leader. The rest are served by any node.

| Flight method | Used for |
| --- | --- |
| `DoPut` | Writes. Descriptor is `Append`, `Put`, `AppendTable` or `PutTable`. Each batch carries `WriteBatch { batch_sequence, changes }` and gets `Written` or `Routed` back |
| `DoGet` | Reads. Ticket is `Scan`, `LimitScan`, `Snapshot`, `Union` or `Lake`. Each batch carries `ScanBatch` or `SnapshotBatch` metadata |
| `DoExchange` | `Tail`, the long poll |
| `DoAction` | Everything else, listed below |
| `ListFlights`, `GetFlightInfo`, `GetSchema`, `ListActions` | Discovery over the catalog |
| `Handshake`, `PollFlightInfo` | Not served |

### Actions

| Group | Actions |
| --- | --- |
| Writers and offsets | `init_writer`, `list_offsets`, `register_producer_offsets`, `get_producer_offsets`, `delete_producer_offsets` |
| Lookups | `lookup`, `prefix_lookup` |
| Catalog | `create_database`, `drop_database`, `list_databases`, `database_exists`, `create_table`, `drop_table`, `list_tables`, `table_exists`, `get_table`, `alter_table`, `create_partition`, `drop_partition`, `list_partitions` |
| Snapshots | `latest_kv_snapshot`, `lake_snapshot` |
| Cluster | `metadata`, `describe_cluster`, `get_config`, `node_stats`, `rebalance`, `health` |

Catalog and cluster actions are proposals to the metadata log and can be sent to any node. `rebalance` is forwarded to the lease holder. `metadata` returns the table, its buckets and their leaders, which is how a client builds its routing table.

### Headers

| Header | Direction | Meaning |
| --- | --- | --- |
| `mink-applied-index` | Response | The metadata index the serving node had applied |
| `mink-min-applied-index` | Request | Wait until this node has applied at least this index before serving. Read-your-writes across nodes |

### Errors

gRPC status codes carry the error. `NotLeader` adds a `Redirect { bucket, to }` body with the leader's advertised address. `Unavailable` means the bucket is being opened. Validation errors, unknown tables and schema mismatches are `InvalidArgument` or `NotFound`.

## Kafka

Topics are log tables in the Kafka database (`kafka` by default). Partitions are buckets. Brokers are nodes with a Kafka listener. The group coordinator lives on the lease holder.

| Kafka | Mink |
| --- | --- |
| Topic `t` | Table `kafka.t`, columns `key BYTES`, `value BYTES`, `headers BYTES`, `timestamp TIMESTAMP_LTZ(3) NOT NULL` |
| Partition `p` | Bucket `p` |
| Offset | Log offset |
| Leader, replicas, ISR | The bucket leader, alone |
| `retention.ms` | `log_ttl` |
| Producer id | Writer id from the shared allocator |
| Group offsets | `CommitGroupOffsets` in the metadata log |

Topics are created on first use when `auto_create_topics` is on, with `default_partitions` buckets, or through `CreateTopics`. Table names must be valid Mink names. A topic created through the Flight API with the fixed four-column schema is a topic too.

### Requests

| API | Versions | Notes |
| --- | --- | --- |
| `ApiVersions` | 0-3 | |
| `Metadata` | 1-12 | Leaders from the metadata view |
| `DescribeCluster` | 0-1 | |
| `Produce` | 3-9 | One `Append` per topic partition. Every produce is durable on object storage before the response. `acks=0` only suppresses the response |
| `Fetch` | 4-12 | `Tail` with `max_wait_ms` and `min_bytes`. Below `log_start` is `OFFSET_OUT_OF_RANGE` |
| `ListOffsets` | 1-7 | `EARLIEST`, `LATEST`, timestamp. `MAX_TIMESTAMP` and `LATEST_TIERED` rejected |
| `InitProducerId` | 0-4 | Idempotent producers. Transactional ids rejected |
| `FindCoordinator` | 0-4 | The lease holder |
| `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup` | | Classic consumer group protocol. States Empty, PreparingRebalance, CompletingRebalance, Stable |
| `OffsetCommit` | 2-8 | Group offsets with `group_offsets_ttl` or the requested retention |
| `OffsetFetch` | 1-8 | |
| `DescribeGroups`, `ListGroups`, `DeleteGroups` | | |
| `CreateTopics` | 2-7 | `retention.ms` becomes `log_ttl` |
| `DeleteTopics` | 1-6 | |
| `DescribeConfigs` | 0-4 | |

Not implemented: transactions (`AddPartitionsToTxn`, `EndTxn`, transactional produce), the KIP-848 consumer protocol, SASL, ACLs, quotas, `IncrementalAlterConfigs`.

### Limits

| Setting | Default |
| --- | --- |
| `max_request_bytes` | 100 MiB |
| `max_fetch_bytes` | 50 MiB |
| `max_in_flight` per connection | 64 |
| `max_wait` cap on fetch | 30 s |
| Session timeout range | 6 s to 30 min |
| `group_offsets_ttl` | 7 days |

Group state is held in memory on the lease holder and rebuilt through rebalances when the lease moves. Committed offsets survive in the metadata log.

## Flight SQL

Served by `mink-query` on `:9130`, not by the node. See [SQL](/docs/design/query).

## Authentication

Not implemented. Neither listener authenticates or authorizes clients. Both are expected on a private network. The Flight `Handshake` is not served.
