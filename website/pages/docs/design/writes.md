# Writes

A write is an Arrow record batch addressed to a bucket or to a table. Bucket-addressed writes go straight to the leader. Table-addressed writes are split by the receiving node. Either way the row ends up in one bucket's log, acknowledged only once the s3stream WAL upload on object storage has completed.

## Routing

<div class="mink-diagram">
<svg viewBox="0 0 680 230" width="680" role="img" aria-label="A client batch is split by partition and bucket, then each sub-batch is sent to that bucket's leader with a per-bucket sequence.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="87" width="130" height="56" class="box"/>
  <text x="85" y="111" text-anchor="middle" class="label">rows</text>
  <text x="85" y="129" text-anchor="middle" class="sub">one Arrow batch</text>
  <rect x="210" y="87" width="150" height="56" class="box-accent"/>
  <text x="285" y="111" text-anchor="middle" class="label">route</text>
  <text x="285" y="129" text-anchor="middle" class="sub">partition, bucket</text>
  <rect x="420" y="20" width="240" height="50" class="box"/>
  <text x="540" y="41" text-anchor="middle" class="label">p=us / b0, seq 17</text>
  <text x="540" y="58" text-anchor="middle" class="sub">leader node 1</text>
  <rect x="420" y="90" width="240" height="50" class="box"/>
  <text x="540" y="111" text-anchor="middle" class="label">p=us / b1, seq 9</text>
  <text x="540" y="128" text-anchor="middle" class="sub">leader node 2</text>
  <rect x="420" y="160" width="240" height="50" class="box"/>
  <text x="540" y="181" text-anchor="middle" class="label">p=eu / b0, seq 3</text>
  <text x="540" y="198" text-anchor="middle" class="sub">leader node 1</text>
  <path d="M150 115 L202 115" class="edge" marker-end="url(#arr)"/>
  <path d="M360 100 L412 48" class="edge" marker-end="url(#arr)"/>
  <path d="M360 115 L412 115" class="edge" marker-end="url(#arr)"/>
  <path d="M360 130 L412 182" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

| Step | Rule |
| --- | --- |
| Partition | The partition key columns of each row name the partition. Missing partitions are created on demand |
| Bucket, with bucket keys | Hash of the encoded bucket-key columns with the table's [bucketing rule](/docs/design/tables#bucketing), modulo `bucket_count`. Null in a key column is rejected |
| Bucket, without keys | The whole batch goes to one bucket. The next batch goes to the next bucket, round-robin |
| Leader | The bucket's leader from the metadata view. A stale view gets a redirect and retries |

The Rust client routes on the client side and keeps one idempotent sequence per bucket. The `mink write` command, the Kafka listener and any client that sends `AppendTable` or `PutTable` let the receiving node route instead: it splits the batch, appends buckets it leads locally and forwards the rest to their leaders, then returns a `Routed` list with the offsets per bucket.

## Requests

DoPut descriptors, one per Flight stream:

| Descriptor | Fields | Table type |
| --- | --- | --- |
| `Append` | `bucket`, `schema_id`, `writer_id` | Log table |
| `Put` | `bucket`, `schema_id`, `writer_id`, `target_columns` | Primary-key table |
| `AppendTable` | `path`, `schema_id` | Log table, routed by the node |
| `PutTable` | `path`, `schema_id`, `target_columns` | Primary-key table, routed by the node |

Each Arrow batch on the stream carries `batch_sequence` for idempotence and, for `Put`, an optional `changes` vector marking rows as upserts or deletes. `target_columns` turns the batch into a partial update. The batch schema is then a subset of the table schema. `schema_id` names the table version the rows were encoded with. It is written into the batch header and travels with the rows.

Response per batch: `Written { first_offset, last_offset, duplicated }` for bucket writes, `Routed { buckets[] }` for table writes.

## On the leader

<div class="mink-diagram">
<svg viewBox="0 0 680 120" width="680" role="img" aria-label="On the leader: check, assign offsets, submit to the stream, WAL upload on object storage, acknowledge, then KV flush for primary-key tables.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="30" width="110" height="56" class="box"/>
  <text x="75" y="54" text-anchor="middle" class="label">check</text>
  <text x="75" y="72" text-anchor="middle" class="sub">epoch, seq</text>
  <rect x="160" y="30" width="110" height="56" class="box"/>
  <text x="215" y="54" text-anchor="middle" class="label">offsets</text>
  <text x="215" y="72" text-anchor="middle" class="sub">base, ts</text>
  <rect x="300" y="30" width="110" height="56" class="box-accent"/>
  <text x="355" y="54" text-anchor="middle" class="label">WAL put</text>
  <text x="355" y="72" text-anchor="middle" class="sub">object store</text>
  <rect x="440" y="30" width="110" height="56" class="box"/>
  <text x="495" y="54" text-anchor="middle" class="label">ack</text>
  <text x="495" y="72" text-anchor="middle" class="sub">HW advances</text>
  <rect x="580" y="30" width="90" height="56" class="box"/>
  <text x="625" y="54" text-anchor="middle" class="label">KV flush</text>
  <text x="625" y="72" text-anchor="middle" class="sub">PK only</text>
  <path d="M130 58 L152 58" class="edge" marker-end="url(#arr)"/>
  <path d="M270 58 L292 58" class="edge" marker-end="url(#arr)"/>
  <path d="M410 58 L432 58" class="edge" marker-end="url(#arr)"/>
  <path d="M550 58 L572 58" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

1. **Ownership.** The node must lead the bucket in its current view, or it answers `NotLeader` with a redirect. The tablet's stream is open under the bucket's leader epoch. S3stream rejects appends under an older epoch.
2. **Sequence.** With a `writer_id`, the batch sequence is checked against the writer window. Duplicates return the original offsets with `duplicated = true`.
3. **Primary-key tables.** The KV tablet reads old rows, applies the merge engine and partial-update targets, and encodes the changelog batch. See [KV tablets](/docs/design/kv-tablets).
4. **Offsets.** The log tablet assigns `base_offset` and `commit_timestamp`.
5. **Durability.** The batch is submitted to the stream. s3stream groups concurrent batches into one WAL object `PUT`. The append resolves when that upload and every earlier one have completed. The high watermark advances and tailing readers are woken.
6. **Flush.** For primary-key tables, staged rows move from the prewrite buffer into the KV store.

Latency is one object-store `PUT` on an idle bucket. Under load, group commit spreads that `PUT` across every batch that arrived while the previous upload was in flight.

## Kafka produce

A Kafka `Produce` request is a batch of records per topic partition. The listener maps the topic to a log table in the Kafka database, the partition to a bucket, and each record to a row of `key`, `value`, `headers`, `timestamp`. Records are appended as one Arrow batch through the same path as `Append`. Idempotent producers use the writer window: `producer_id` is a writer id from the shared allocator and `base_sequence` is the batch sequence. Transactional produce requests are rejected.

## Failure cases

| Failure | Outcome |
| --- | --- |
| Object store `PUT` fails or times out | Append fails. Nothing is acknowledged. The client retries with the same sequence |
| Leader changes mid-request | The old leader's append is rejected by the stream epoch. The client gets `NotLeader` with the new leader and retries |
| Node crashes after ack, before KV flush | The changelog is durable. Recovery replays it into the store |
| Duplicate after retry | Recognized by the writer window, original offsets returned |
| Partition missing | Created on demand, then written |
