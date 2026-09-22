# Reads

Reads are served by the bucket's leader from the fastest tier that holds the data. A reader following the tail is answered from the s3stream log cache and rarely touches the object store. A reader replaying history streams out of stream objects through a readahead cache. Every log read is bounded by the high watermark, so a reader cannot observe a row that a crash could take back.

## Request types

| Read | Addressed to | Returns |
| --- | --- | --- |
| `Scan { bucket, offset, max_bytes, columns }` | Bucket | Log batches from `offset` up to the high watermark, bounded by `max_bytes` |
| `Tail { bucket, offset, columns, max_wait_ms, min_bytes }` | Bucket | Like `Scan`, but waits for the watermark to pass `offset`. The long poll behind consumers |
| `LimitScan { bucket, limit, columns }` | Bucket | The first `limit` rows of a primary-key table's KV store |
| `Snapshot { bucket, columns, batch_rows }` | Bucket | Every row of the KV store in key order |
| `Union { bucket, columns }` | Bucket | Lake snapshot plus log tail, merged. See [Union read](/docs/design/union-read) |
| `Lake { path, partition, snapshot_id, columns }` | Table | The Iceberg snapshot only, read by the node |
| `lookup`, `prefix_lookup` | Bucket | Rows by primary key or by partition plus bucket-key prefix |
| `list_offsets { bucket, Earliest \| Latest \| Timestamp }` | Bucket | One offset |

`Scan`, `Tail`, `Snapshot` and `Union` are DoGet tickets, `Tail` a DoExchange, lookups and `list_offsets` are actions. All bucket-addressed reads require the receiving node to lead the bucket. Otherwise the answer is `NotLeader` with the leader's address.

## Log scan

<div class="mink-diagram">
<svg viewBox="0 0 640 170" width="640" role="img" aria-label="A scan at offset n is answered by the log cache for the tail, the block cache for recent history, or the object store for the rest.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="57" width="120" height="56" class="box-accent"/>
  <text x="80" y="81" text-anchor="middle" class="label">scan at n</text>
  <text x="80" y="99" text-anchor="middle" class="sub">leader</text>
  <rect x="200" y="57" width="120" height="56" class="box"/>
  <text x="260" y="81" text-anchor="middle" class="label">log cache</text>
  <text x="260" y="99" text-anchor="middle" class="sub">tail, memory</text>
  <rect x="360" y="57" width="120" height="56" class="box"/>
  <text x="420" y="81" text-anchor="middle" class="label">block cache</text>
  <text x="420" y="99" text-anchor="middle" class="sub">recent objects</text>
  <rect x="520" y="57" width="100" height="56" class="box"/>
  <text x="570" y="81" text-anchor="middle" class="label">objects</text>
  <text x="570" y="99" text-anchor="middle" class="sub">readahead</text>
  <path d="M140 85 L192 85" class="edge" marker-end="url(#arr)"/>
  <path d="M320 85 L352 85" class="edge-soft" marker-end="url(#arr)"/>
  <path d="M480 85 L512 85" class="edge-soft" marker-end="url(#arr)"/>
  <text x="336" y="40" text-anchor="middle" class="sub">miss</text>
  <text x="496" y="40" text-anchor="middle" class="sub">miss</text>
</svg>
</div>

Each batch comes back with `ScanBatch { base_offset, last_offset, commit_timestamp, schema_id, changes, high_watermark }` in the Flight app metadata. `changes` is the change-type vector for primary-key changelogs and absent for append-only batches. `high_watermark` lets a consumer measure its lag without a second request.

Projection: `columns` is a list of field indexes. The tablet rewrites each batch to those columns before sending, so bytes on the wire and Arrow decode on the client scale with the projection, not the table.

Schema: batches written under an older schema version are remapped to the current version by the client from `schema_id`, with added columns null and dropped columns removed.

## Tail

`Tail` is `Scan` with a wait. If the high watermark is at or below `offset`, the tablet subscribes to the watermark watch and returns when it moves or `max_wait_ms` expires. `min_bytes` keeps it waiting for a fuller response. A Kafka `Fetch` with `max_wait_ms` and `min_bytes` maps onto it directly. An empty response after the wait is a normal result, not an error.

## Snapshot and lookup

Primary-key reads go to the KV tablet.

| Read | Path |
| --- | --- |
| `Snapshot` | Iterates the store in key order, `batch_rows` per Arrow batch. `SnapshotBatch { log_offset }` on each batch names the changelog offset the store had reached, so a client can continue with `Scan` from there and miss nothing |
| `LimitScan` | The first `limit` rows, for previews |
| `lookup` | Encode the primary key, one store `get`. Multi-key lookups are one `get` per key |
| `prefix_lookup` | Range scan over partition keys plus bucket keys |

A `Snapshot` followed by a `Scan` from its `log_offset` is a consistent bootstrap of a primary-key table: current state, then every change since.

## Offsets by position and time

| Spec | Result |
| --- | --- |
| `Earliest` | `log_start` |
| `Latest` | High watermark |
| `Timestamp` | First offset with `commit_timestamp >= ts`, binary search over batch headers |

Kafka `ListOffsets` uses the same three. `MAX_TIMESTAMP` and `LATEST_TIERED` are not supported.

## Kafka fetch

A `Fetch` maps each topic partition to a bucket and runs `Tail`. Rows come back as Kafka record batches built from the `key`, `value`, `headers` and `timestamp` columns. A fetch at an offset below `log_start` returns `OFFSET_OUT_OF_RANGE`. The log is the only source for Kafka reads, and the retained window is the table's `log_ttl`. History older than that is read from the lakehouse through Flight, Flight SQL or any Iceberg engine.

## Lake and union

`Lake` reads one Iceberg snapshot of a table or partition on the node, with column projection and a predicate pushed into the Iceberg scan. `Union` combines that with the log tail for one bucket. Both are covered in [Union read](/docs/design/union-read). How `mink-query` chooses between these reads for a statement is covered in [SQL](/docs/design/query).
