# Tuning

Write latency is bounded below by one object store `PUT`, and cost is roughly proportional to how many `PUT`s are made. Every knob on this page moves along that line: latency against request count, freshness against request count, or failover speed against false positives.

<div class="mink-diagram">
<svg viewBox="0 0 720 150" width="720" role="img" aria-label="A write is acknowledged after the WAL batch is uploaded, bounded by batchInterval. WAL is packed into stream objects every wal_upload_interval. KV state is snapshotted every kv_snapshot_interval. History is tiered every lake_freshness.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="0" y="30" width="162" height="72" class="box-accent"/>
  <text x="81" y="53" text-anchor="middle" class="label">WAL batch</text>
  <text x="81" y="71" text-anchor="middle" class="sub">batchInterval</text>
  <text x="81" y="88" text-anchor="middle" class="sub">250 ms</text>
  <rect x="186" y="30" width="162" height="72" class="box"/>
  <text x="267" y="53" text-anchor="middle" class="label">stream objects</text>
  <text x="267" y="71" text-anchor="middle" class="sub">wal_upload_interval</text>
  <text x="267" y="88" text-anchor="middle" class="sub">10s</text>
  <rect x="372" y="30" width="162" height="72" class="box"/>
  <text x="453" y="53" text-anchor="middle" class="label">KV snapshot</text>
  <text x="453" y="71" text-anchor="middle" class="sub">kv_snapshot_interval</text>
  <text x="453" y="88" text-anchor="middle" class="sub">10m</text>
  <rect x="558" y="30" width="162" height="72" class="box"/>
  <text x="639" y="53" text-anchor="middle" class="label">lake snapshot</text>
  <text x="639" y="71" text-anchor="middle" class="sub">lake_freshness</text>
  <text x="639" y="88" text-anchor="middle" class="sub">3m</text>
  <path d="M162 66 L180 66" class="edge" marker-end="url(#arr)"/>
  <path d="M348 66 L366 66" class="edge" marker-end="url(#arr)"/>
  <path d="M534 66 L552 66" class="edge" marker-end="url(#arr)"/>
  <text x="81" y="128" text-anchor="middle" class="sub">acknowledged</text>
  <text x="267" y="128" text-anchor="middle" class="sub">read from objects</text>
  <text x="453" y="128" text-anchor="middle" class="sub">recovery start</text>
  <text x="639" y="128" text-anchor="middle" class="sub">read from the lake</text>
</svg>
</div>

## Write latency

A write is acknowledged when its WAL batch is on object storage. The WAL seals a batch at `maxBytesInBatch` (8 MiB) or when `batchInterval` (250 ms) lapses, so a lone write on a quiet bucket pays up to the interval on top of the upload. The interval is a parameter on the WAL URI.

```bash
--wal-uri "0@s3://mink?region=us-east-1&batchInterval=5"
```

| Parameter | Default | Purpose |
| --- | --- | --- |
| `batchInterval` | `250` ms | Longest a batch waits before upload |
| `maxBytesInBatch` | `8 MiB` | Batch size that seals early |
| `maxUnflushedBytes` | `1 GiB` | Backpressure. Writes wait once this much WAL is not yet uploaded |
| `maxInflightUploadCount` | `50` | Concurrent WAL uploads |
| `readaheadDataSize` | `100 MiB` | Readahead when replaying the WAL on failover |

`5` ms gives near-floor latency at one `PUT` per flush. Busy buckets are insensitive to the interval because size seals the batch first. The compose stacks use `batchInterval=5`.

An S3 Express One Zone directory bucket as the WAL cuts the `PUT` itself to single-digit milliseconds while the data bucket stays on standard S3. Detected from the `--x-s3` name suffix, or forced with `s3Express=true`.

## Throughput

Throughput comes from parallelism, not from faster individual writes.

| Lever | Effect |
| --- | --- |
| Rows per write | One durability wait per batch. `mink write --batch-rows`, `mink bench --batch` |
| Bucket count | Each bucket is an independent log with its own leader and WAL pipeline. `--bucket-count` at create, `default_bucket_count` for the cluster |
| Concurrent writers | Writes to different buckets run on different nodes. Writes to one bucket are serialized by the leader |
| Idempotent writers | A writer id and sequence per bucket. Retries after a timeout do not duplicate |

`mink bench` reports what a combination achieves against a real cluster, with redirects and leader waits in the numbers.

## Recovery time

| Key | Default | Trade |
| --- | --- | --- |
| `wal_upload_interval` | `10s` | Shorter keeps less WAL to replay on failover, at more object writes |
| `kv_snapshot_interval` | `10m` | A primary-key bucket recovers from the newest snapshot plus the changelog since. Shorter is a faster open at more snapshot uploads |
| `snapshots_retained` | `1` | More retained snapshots cost object storage and buy nothing for recovery, since the newest is used |

Failover of a primary-key bucket is snapshot download plus changelog replay from the snapshot's offset. The interval bounds the replay.

## Failover speed

| Key | Default | Trade |
| --- | --- | --- |
| `lease_ttl` | `30s` | A node is dead after this long without a heartbeat. Shorter detects failure sooner and turns a GC pause or network blip into a failover |
| `coordinator_tick` | `5s` | Re-lead happens on the tick after detection. Shorter is faster reassignment at more metadata reads |

Heartbeats renew every `lease_ttl / 4`, so the TTL has to cover several missed renewals. Detection to serving is `lease_ttl + coordinator_tick + WAL replay`. The compose stacks use `10s` and `1s`.

## Freshness and cost

| Setting | Default | Trade |
| --- | --- | --- |
| `lake_freshness` per table | `3m` | How stale the lake may be. Each round is at least one Parquet file per bucket plus a catalog commit, so shorter freshness is more small files |
| `--lake-auto-compaction` | Off | The tiering worker rewrites small files. Fewer files for lake readers at extra writes |
| `log_ttl` per table | `7d` | How long the log serves history. Reads past it come from the lake through union read, not from Kafka |
| `log_retention_interval` | `5m` | How often trims run. Coarser is fewer metadata proposals |

Request count dominates the bill on most object stores. The three levers that matter are the WAL batch interval, the WAL upload interval and lake freshness. Compaction and snapshot pruning add a background of requests proportional to churn.

## Reads

| Setting | Default | Purpose |
| --- | --- | --- |
| Scan bytes per response | `16 MiB` | Server default for a `Scan` without an explicit limit |
| Snapshot rows per batch | `4096` | Server default for `Snapshot` and lookups |
| Leader wait | `30s` | How long a request waits for a bucket that has no leader before `Unavailable` |
| `kafka.max_fetch_bytes` | `50 MiB` | Cap on one Kafka fetch |
| `kafka.max_wait` | `30s` | Longest a Kafka fetch parks waiting for data |

Tail readers are woken by the write path, so delivery latency for a live consumer is the write acknowledgement latency. Tightening `batchInterval` improves end-to-end delivery as a side effect.
