# Table metadata

Each committed snapshot `N`, under lease epoch `E`, writes a table metadata file, a manifest list and the manifests it changes, all under `{warehouse}/{topic}/metadata/`. A load can also render a tail that is never committed. Tables are Iceberg format version 2.

<div class="mink-diagram">
<svg viewBox="0 0 700 270" width="700" role="img" aria-label="Snapshot N's table metadata names its manifest list, which names the manifests the commit wrote, the earlier manifests it lists again, and the external table's entries. A tail's table metadata names its own manifest list, which names the tail's manifests and then snapshot N's manifests.">
  <defs>
    <marker id="metadata-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="140" y="14" text-anchor="middle" class="sub">snapshot N</text>
  <text x="560" y="14" text-anchor="middle" class="sub">tail, never committed</text>
  <rect x="20" y="24" width="240" height="50" class="box-accent"/>
  <text x="140" y="54" text-anchor="middle" class="sub">v{N}-e{E}.metadata.json</text>
  <rect x="440" y="24" width="240" height="50" class="box-accent"/>
  <text x="560" y="54" text-anchor="middle" class="sub">v{N}-e{E}-t{K}.metadata.json</text>
  <rect x="20" y="110" width="240" height="50" class="box"/>
  <text x="140" y="140" text-anchor="middle" class="sub">snap-{N}-e{E}.avro</text>
  <rect x="440" y="110" width="240" height="50" class="box"/>
  <text x="560" y="140" text-anchor="middle" class="sub">snap-{N}-e{E}-t{K}.avro</text>
  <rect x="20" y="200" width="155" height="56" class="box"/>
  <text x="97" y="223" text-anchor="middle" class="label">written</text>
  <text x="97" y="241" text-anchor="middle" class="sub">{N}-e{E}-m{k}.avro</text>
  <rect x="190" y="200" width="155" height="56" class="box"/>
  <text x="267" y="223" text-anchor="middle" class="label">listed again</text>
  <text x="267" y="241" text-anchor="middle" class="sub">earlier manifests</text>
  <rect x="360" y="200" width="155" height="56" class="edge-soft"/>
  <text x="437" y="223" text-anchor="middle" class="label">external</text>
  <text x="437" y="241" text-anchor="middle" class="sub">entries unchanged</text>
  <rect x="530" y="200" width="155" height="56" class="box"/>
  <text x="607" y="223" text-anchor="middle" class="label">tail files</text>
  <text x="607" y="241" text-anchor="middle" class="sub">…-t{K}-m{k}.avro</text>
  <path d="M140 74 L140 106" class="edge" marker-end="url(#metadata-arrow)"/>
  <path d="M560 74 L560 106" class="edge" marker-end="url(#metadata-arrow)"/>
  <path d="M140 160 L97 196" class="edge" marker-end="url(#metadata-arrow)"/>
  <path d="M140 160 L267 196" class="edge" marker-end="url(#metadata-arrow)"/>
  <path d="M140 160 L437 196" class="edge" marker-end="url(#metadata-arrow)"/>
  <path d="M607 160 L607 196" class="edge" marker-end="url(#metadata-arrow)"/>
  <path d="M440 135 L264 135" class="edge-soft" marker-end="url(#metadata-arrow)"/>
  <text x="352" y="128" text-anchor="middle" class="sub">then N's manifests</text>
</svg>
</div>

## Table

| Field | Value |
| --- | --- |
| `format-version` | 2 |
| `table-uuid` | The topic's uuid |
| `location` | `{warehouse}/{topic}` |
| `schemas`, `current-schema-id` | Every [schema](/docs/formats/schema) of the topic, the latest current |
| `last-column-id` | The highest field id among them |
| `partition-specs`, `default-spec-id` | As [Partition specs](#partition-specs) says |
| `last-partition-id` | The highest partition field id among the specs |
| `sort-orders` | Unsorted, id 0 and the default, then the external table's orders with their own ids |
| `statistics` | The latest [statistics file](/docs/formats/statistics), once one is published |
| `properties` | None |
| `current-snapshot-id`, `refs.main` | The latest snapshot |
| `last-sequence-number`, `last-updated-ms` | The latest snapshot's |
| `metadata-log` | The table metadata files of the earlier snapshots it keeps |

Keys are sorted and arrays keep the orders above, so a snapshot renders to the same bytes on any node.

## Views

Each snapshot is rendered once per [namespace](/docs/design/tables#namespaces). A tail renders `merged` and `stream`.

| | `merged` | `stream` | `lake` |
| --- | --- | --- | --- |
| Table metadata | `v{N}-e{E}.metadata.json` | `v{N}-e{E}-stream.metadata.json` | `v{N}-e{E}-lake.metadata.json` |
| Manifest list | `snap-{N}-e{E}.avro` | `snap-{N}-e{E}-stream.avro` | `snap-{N}-e{E}-lake.avro` |
| Lists | Mink's manifests, then the external entries | Mink's manifests | The external entries |
| `table-uuid` | The topic's uuid | The name-based uuid (version 5) of the topic's uuid and `stream` | The same, with `lake` |
| `snapshots`, `snapshot-log`, `metadata-log` | Every retained snapshot | The current snapshot | The current snapshot |
| Summary totals | Both parts | Mink's files | The external entries |
| `statistics` | The latest file | None | None |

Schemas, specs, sort orders, snapshot ids and sequence numbers are the same in all three. Manifests are written once and listed by each view that holds them.

## Partition specs

| Spec | Id | Fields |
| --- | --- | --- |
| Time spec `n`, the topic's `n`th granularity | `2^30 + n` | `_kafka_partition`, field 1000, identity of field 1. With a time granularity, `_kafka_timestamp_{granularity}`, the transform of field 3, field 1001 for the topic's first time granularity, 1002 for the next different one, and so on. |
| The external table's | Its own, below `2^30` | As the external table has them |

`default-spec-id` is the current time spec. Granularities are in [Time partitioning](/docs/design/layout#time-partitioning).

## Snapshots

| Field | Value |
| --- | --- |
| `snapshot-id`, `parent-snapshot-id` | `N` and `N - 1`, with no parent for the first |
| `sequence-number` | One above the previous snapshot's, or above the external table's highest entry when that is higher |
| `timestamp-ms`, `schema-id` | The commit time and the current schema |
| `manifest-list` | `snap-{N}-e{E}.avro` |
| `summary.operation` | `append` when the commit drops no files, `delete` when it only drops, `overwrite` when it does both |
| `summary` counters | `added-data-files`, `added-records`, `added-files-size`, `deleted-data-files`, `deleted-records`, `removed-files-size` for what the commit changed. `total-equality-deletes` is 0. |
| `summary` totals | `total-data-files`, `total-records`, `total-files-size` of Mink's files, plus the external entries' file and row counts. `total-delete-files` and `total-position-deletes` count the external delete entries. With an external table, `total-files-size` is left out. |
| `mink.partition.{p}.start-offset`, `.end-offset` | The offsets partition `p` covers |
| `mink.external.generation`, `mink.external.snapshot-id` | The listed external table's generation and snapshot |
| `mink.external.partition.{p}.end-offset` | Where the external entries end for partition `p` |

## Manifests

Each of Mink's manifests holds the files of one spec and one time bucket, see [Manifests](/docs/design/layout#manifests).

- A commit writes one manifest per spec and bucket it adds files to, as `ADDED`.
- A bucket it assembles, or one that would pass 16 manifests, is rewritten as one manifest: live files `EXISTING` or `ADDED`, replaced and dropped files `DELETED`.
- Any other manifest that holds a dropped file is rewritten the same way.
- A manifest of only `DELETED` entries is listed only by the snapshot that wrote it. Every other earlier manifest is listed again unchanged.
- The manifest list names the written manifests, then the rewritten, then the reused, then the external table's entries unchanged. Each of Mink's entries summarizes `_kafka_partition` and the bucket.

| Data file entry | Value |
| --- | --- |
| `file_path` | The [data file](/docs/formats/data) path |
| `file_format` | `PARQUET` |
| `partition` | The partition index and, with a time spec, the bucket |
| `record_count`, `file_size_in_bytes` | The file's records and bytes |
| `lower_bounds`, `upper_bounds`, `null_value_counts`, `nan_value_counts` | As [Statistics](/docs/design/layout#statistics) lists, keyed by field id. Bounds use Iceberg's single-value encoding. |

## Tail

A tail extends snapshot `N` to the last stable offsets a load sees.

| Part | Value |
| --- | --- |
| `K` | The first 16 hex digits of the SHA-256 of the tail's offsets, one `partition:start:end` line per partition |
| `snapshot-id` | `2^62` plus the low 62 bits of `K`. Committed ids stay below `2^62`. |
| `sequence-number`, `parent-snapshot-id` | One above `N`'s, and `N` |
| Manifests | One per spec and bucket of the records after `N`, as `ADDED`, then `N`'s manifests |
| Table metadata | `N`'s, with the tail current and `N`'s file added to `metadata-log` |

## External table

Each tier commit adds one snapshot to the [external table](/docs/design/tiering):

| Object | Value |
| --- | --- |
| Data files | `{write.data.path or location/data}/{partition path}/{p}-{start}-{end}-{n}-{uuid}.parquet`, `n` a five-digit counter. Rows are partitioned by the default spec and sorted by the default sort order. |
| Parquet settings | `write.target-file-size-bytes`, `write.parquet.row-group-size-bytes`, `page-size-bytes`, `dict-size-bytes`, `compression-codec`, `compression-level`, `bloom-filter-enabled.column.*`, `bloom-filter-fpp.column.*`, and `write.metadata.metrics.*` |
| Snapshot | `append`, with one new manifest of the files as `ADDED` |
| `mink.partition.{p}.start-offset`, `.end-offset` | In the snapshot summary: the offsets of partition `p` the commit appends |
| `mink.partition.{p}.end-offset` | A table property set in the same commit: the first offset of partition `p` the table does not hold |
