# Reads

Engines read a table in two steps: load it from the REST catalog, then read its objects from the S3 endpoint. Metadata objects come from the bucket. A data file's footer comes from Postgres. Its row groups are built from Kafka on the node that owns them, and cached there.

<div class="mink-diagram">
<svg viewBox="0 0 660 340" width="660" role="img" aria-label="An engine loads the table from the catalog, which reads the snapshot from Postgres, then reads metadata and data files from the S3 endpoint. The S3 endpoint reads metadata objects from the bucket and forwards data file reads to the owner node, which serves them from its cache or builds them from Kafka.">
  <defs>
    <marker id="reads-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="10" y="40" width="140" height="162" class="box"/>
  <text x="80" y="116" text-anchor="middle" class="label">engine</text>
  <text x="80" y="134" text-anchor="middle" class="sub">plans and runs</text>
  <rect x="220" y="40" width="180" height="56" class="box"/>
  <text x="310" y="64" text-anchor="middle" class="label">REST catalog</text>
  <text x="310" y="82" text-anchor="middle" class="sub">any node</text>
  <rect x="220" y="146" width="180" height="56" class="box"/>
  <text x="310" y="170" text-anchor="middle" class="label">S3 endpoint</text>
  <text x="310" y="188" text-anchor="middle" class="sub">any node</text>
  <rect x="470" y="40" width="180" height="56" class="box"/>
  <text x="560" y="64" text-anchor="middle" class="label">Postgres</text>
  <text x="560" y="82" text-anchor="middle" class="sub">snapshots, files</text>
  <rect x="470" y="146" width="180" height="56" class="box-accent"/>
  <text x="560" y="170" text-anchor="middle" class="label">owner's cache</text>
  <text x="560" y="188" text-anchor="middle" class="sub">memory, optional disk</text>
  <rect x="220" y="264" width="180" height="56" class="box"/>
  <text x="310" y="288" text-anchor="middle" class="label">bucket</text>
  <text x="310" y="306" text-anchor="middle" class="sub">metadata objects</text>
  <rect x="470" y="264" width="180" height="56" class="box"/>
  <text x="560" y="288" text-anchor="middle" class="label">Kafka</text>
  <text x="560" y="306" text-anchor="middle" class="sub">records</text>
  <path d="M150 68 L216 68" class="edge" marker-end="url(#reads-arrow)"/>
  <path d="M400 68 L466 68" class="edge" marker-end="url(#reads-arrow)"/>
  <path d="M150 174 L216 174" class="edge" marker-end="url(#reads-arrow)"/>
  <path d="M400 174 L466 174" class="edge" marker-end="url(#reads-arrow)"/>
  <path d="M310 202 L310 260" class="edge-soft" marker-end="url(#reads-arrow)"/>
  <path d="M560 202 L560 260" class="edge" marker-end="url(#reads-arrow)"/>
  <text x="183" y="60" text-anchor="middle" class="sub">1. load</text>
  <text x="183" y="166" text-anchor="middle" class="sub">2. read</text>
  <text x="433" y="60" text-anchor="middle" class="sub">snapshot</text>
  <text x="433" y="166" text-anchor="middle" class="sub">forward</text>
  <text x="318" y="236" class="sub">metadata</text>
  <text x="552" y="236" text-anchor="end" class="sub">build on a miss</text>
</svg>
</div>

## Loading a table

The catalog returns the table metadata, see [Snapshots](/docs/design/snapshots). Its `config` tells the engine where the S3 endpoint is: `s3.endpoint` from the `s3_endpoint` setting, path-style access, and the storage region.

## Reading objects

| Object | Source |
| --- | --- |
| Table metadata, manifest lists, manifests, statistics files | The bucket, through the cache. Checked against the bucket's ETag after a minute. |
| Data file, everything but its row groups | Postgres, through the cache. No build. |
| Data file, its row groups | Built from Kafka on the file's owner node, only the extents the read overlaps, through the owner's cache |
| A tail's data file ([Snapshots](/docs/design/snapshots)) | Built whole from Kafka on its owner node, through the owner's cache. A tail is never committed, so Postgres has no row for it. |

The endpoint answers `GET` with an optional `Range` header, and `HEAD`. A data file's `HEAD` comes from its stored statistics, without a build. Writes are refused with `403 AccessDenied`.

## Footers

Engines open a Parquet file by reading from its end: 64 KiB with PyIceberg, 48 KiB with Trino, and with DuckDB the power of two at or above a 1,024th of the file, at least 16 KiB. Every data file has zero bytes before its footer, at least 68 KiB and about 0.2% of a large file, so that read never reaches a row group. The padding, the footer and its length are known from Postgres, so the first read of a file needs no build, and an engine that skips all of a file's row groups by their statistics never causes a Kafka read.

## Parts and extents

A file the indexer wrote is one part. An assembled file is the parts it joined, each still a file of its own. Mink builds an assembled file in extents: runs of consecutive parts up to 1 MiB, or one larger part on its own. A read of an assembled file's row groups builds only the extents those row groups fall in, each from one Kafka read.

| Parts | Extents |
| --- | --- |
| Larger than 1 MiB, as on a busy topic | One per part, so a read builds exactly the parts it needs |
| Smaller, as on a quiet topic | Packed together, so a read of many small parts is a few builds, not one per part |

Below about 1 MiB, the fixed cost of a build, a Kafka read and its lookups, outweighs building a little more.

Measured on AWS with DuckDB 2.0, two nodes, each query on a cold cache:

| Table | Query | Built from Kafka | Kafka read |
| --- | --- | --- | --- |
| `lineitem`: 3.0 M rows in 168 assembled files of about 1 MB, parts of about 28 KB | 500 offsets of one partition | 1 extent | 8.8 MB |
| | One month by `_kafka_timestamp` | 2 extents | 14.4 MB |
| | One week by `l_shipdate` | 2 extents | 19.5 MB |
| | Full scan of `l_quantity` | 168 extents, one per file | 1,381 MB |
| `large_parts`: 15,000 records of 10 KB in one file of 150 MB, 3 parts of 50 MB | 500 offsets inside the middle part | 1 extent of 3 | 57.6 MB |

## Owners

Each data file has one owner among the nodes with the `s3` role, chosen by hashing each node's id with the file's path. The highest hash wins. A read that needs a build is forwarded to the owner, and the answer relayed. A read of stored bytes only, such as a footer read, is answered by whichever node receives it.

| Case | Result |
| --- | --- |
| Owner answers | The owner serves from its cache, or builds the extents the read needs and caches them |
| Owner unreachable within 5 seconds, or silent for 60 | The receiving node builds the extents itself, without caching them |
| A node joins or leaves | Only the files it owns or would own move |

## Builds

A build reads committed records from Kafka and writes the Parquet with the file's schema. The bytes are the same every time, on any node. It builds an extent, or a whole tail file.

- A build answers with the whole extent, so a read of any range of an extent caches all of it.
- Concurrent reads of one extent wait for one build. The build runs as a task of its own, so it finishes and is cached even when the read that started it goes away.
- A node runs at most `max_builds` builds at once, by default the number of CPUs.
- A built part whose length differs from its span is never served. The endpoint answers `500` and logs it.

PyIceberg reads each row group with a request of its own, through PyArrow's pool of 8 IO threads. On a cold cache, the requests of one file wait on one build and can hold all 8 threads, so builds run one at a time. A larger pool lets them overlap:

```python
import pyarrow

pyarrow.set_io_thread_count(64)
```

| Cold scan of `lineitem`, 3.0 M rows in 168 files, 2 nodes | Time |
| --- | --- |
| PyIceberg 0.12 with PyArrow 26, 8 IO threads, the default | 47.4 s |
| PyIceberg 0.12 with PyArrow 26, 64 IO threads | 23.8 s |
| DuckDB 2.0 | 23.5 s |

## Cache

| Tier | Setting |
| --- | --- |
| Memory | `cache_bytes` |
| Disk | `cache_disk_bytes`, under `cache_dir` when set |

`cache_admission` chooses which blocks go to disk:

| Value | Blocks written to disk |
| --- | --- |
| `reused`, the default | On their second read |
| `fetched` | All, as soon as they are fetched |
| `evicted` | On eviction from memory |

The cache holds what Postgres does not: built extents, and whole tail files. A stored file's padding and footer are never cached as data. They come from its row in Postgres, cached once per file.

A file on disk survives a restart. Files are cached under their table's uuid. A table added again under the same name never reads the old table's files.
