# Metrics

Every node serves Prometheus text on `GET /metrics` of its admin listener. Counters start at zero when the node starts. A node keeps no history: Prometheus or the console turns successive readings into rates.

```yaml
scrape_configs:
  - job_name: mink
    static_configs:
      - targets: ['mink-1:9180', 'mink-2:9180']
```

Scrape every node. Each counts only its own work. Label values come from fixed sets, so the number of series never grows with tables, partitions or files.

## Indexing and commits

| Metric | Type | Labels | Counts |
| --- | --- | --- | --- |
| `mink_indexed_records_total` | counter | `source`: `live`, `backfill` | Records written as files |
| `mink_indexed_bytes_total` | counter | `source` | Bytes of those files |
| `mink_files_written_total` | counter | `source` | Files written by indexers |
| `mink_kafka_requests_total` | counter | `request`: `metadata`, `offsets`, `fetch`. `result`: `ok`, `error` | Kafka requests, retries included |
| `mink_kafka_fetched_bytes_total` | counter | | Record batch bytes fetched |
| `mink_commits_total` | counter | `result`: `ok`, `error` | Snapshot commits |
| `mink_commit_duration_seconds` | histogram | | Time of one commit |
| `mink_assemblies_total` | counter | | Assembled files committed |
| `mink_snapshots_expired_total` | counter | | Snapshots expired |
| `mink_objects_collected_total` | counter | | Metadata objects deleted |
| `mink_worker_failures_total` | counter | `worker`: `indexer`, `writer` | Indexers and writers that ended with an error |
| `mink_leases` | gauge | `scope`: `partition`, `topic` | Leases this node holds |

## Reads

| Metric | Type | Labels | Counts |
| --- | --- | --- | --- |
| `mink_catalog_requests_total` | counter | `route`: `config`, `namespaces`, `tables`, `load`, `write`. `status`: `2xx`, `4xx`, `5xx` | Catalog requests |
| `mink_tails_total` | counter | | Load tails built |
| `mink_tail_duration_seconds` | histogram | | Time to build one tail |
| `mink_s3_requests_total` | counter | `method`: `get`, `head`. `object`: `data`, `metadata`. `status` | S3 requests answered or forwarded |
| `mink_s3_sent_bytes_total` | counter | | Response bytes this node sent |
| `mink_s3_forwarded_total` | counter | `result`: `ok`, `unreachable` | Reads forwarded to a file's owner |
| `mink_builds_total` | counter | `result`: `ok`, `error` | Data files built from Kafka |
| `mink_build_duration_seconds` | histogram | | Time to build one data file |
| `mink_registry_requests_total` | counter | `result`: `ok`, `missing`, `error` | Schema Registry requests |
| `mink_admin_requests_total` | counter | `status` | Admin API requests |
| `mink_cache_bytes` | gauge | | Bytes in the cache's memory tier |

Histograms have buckets from 0.5 ms to 10 s.

## Cache

The cache adds its own `nestor_*` metrics, labelled by `namespace`: `files` for data files, `metadata` for metadata objects.

| Metric | Counts |
| --- | --- |
| `nestor_blocks_hit_total`, `nestor_blocks_miss_total` | Blocks served from the cache, and blocks fetched |
| `nestor_bytes_served_total` | Bytes served |
| `nestor_origin_requests_total`, `nestor_origin_bytes_total` | Fetches from the origin, Kafka builds for `files` and the bucket for `metadata` |
| `nestor_origin_errors_total`, `nestor_origin_timeouts_total` | Failed and timed out fetches |

The cache hit ratio for data files is `nestor_blocks_hit_total{namespace="files"}` over hits plus misses.
