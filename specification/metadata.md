# Metadata

Each committed snapshot of a topic becomes a snapshot of an Iceberg table, written as a table metadata file, a manifest list and manifests. This document pins what Mink writes, where, and how each follows from the snapshot and its files. Conformance fixtures in `../conformance/fixtures/metadata/` pin it.

## Format version

`FormatVersion` names the Iceberg table format Mink writes. Only the versions below exist. A new version is a new entry here, new fixtures, and a new variant. A table's format version is fixed when the table is created.

| Version | Status |
| --- | --- |
| 2 | Written for every table |

Format version 3 is not supported. Supporting it means defining row lineage for Mink tables and adding it here.

## Table

| Field | Value |
| --- | --- |
| `format-version` | 2 |
| `table-uuid` | The topic's uuid |
| `location` | `{warehouse}/{topic}` |
| Schemas | The binary schema, id 0, and every typed schema of the topic (`schema.md`), in id order, as Postgres holds them when the snapshot is rendered |
| `current-schema-id` | The latest of them when the snapshot was rendered |
| `last-column-id` | The highest field id of any of them |
| Partition specs | Every time spec the topic has had (`layout.md` Time partitioning), as Postgres holds them when the snapshot is rendered, in id order, then the specs of each external table a retained snapshot lists, in id order: spec `e` of the external table listed as generation `g` has id `2^30 + g × 2^16 + e`, for `0 ≤ e < 2^16` and `1 ≤ g ≤ 16383`. Different external tables listed by one table have different generations. `default-spec-id` is the current time spec, and `last-partition-id` the highest partition field id among them all |
| Statistics | The latest statistics file (`statistics.md`), when the writer has published one |
| Sort orders | Unsorted, order id 0, then the other sort orders of the newest generation a retained snapshot lists, in id order. `default-sort-order-id` is 0 |
| Properties | None |
| `current-snapshot-id` and the `main` branch | The latest snapshot |
| `last-sequence-number` | The latest snapshot's sequence number |
| `last-updated-ms` | The latest snapshot's `timestamp_ms` |

## Snapshots

A Mink snapshot is the state of one topic's table at one moment: for each partition, the offsets `[start_offset, end_offset)` the table covers, with an id `N`, a `timestamp_ms` and the epoch `E` of the lease it was committed under. Ids start at 1, and each commit takes the one after the latest. A snapshot's files are those whose ranges lie inside its offsets, less the files an assembly replaced at or before it and the assembled files added after it (`layout.md` Assembly).

With an external table, each partition starts at the larger of its indexed start and the external table's end offset.

Iceberg snapshot `N` is Mink snapshot `N`. Its sequence number `S` is one more than the previous snapshot's, or than the highest sequence number among the external table's manifest list entries when that is higher.

| Field | Value |
| --- | --- |
| `snapshot-id` | `N` |
| `sequence-number` | `S` |
| `parent-snapshot-id` | `N - 1`, absent for the first snapshot |
| `timestamp-ms` | The snapshot's `timestamp_ms` |
| `schema-id` | The table's `current-schema-id` when the snapshot was rendered |
| `manifest-list` | `{location}/metadata/snap-{N}-e{E}.avro` |
| `summary.operation` | `append` when the commit drops no files, `delete` when it drops files and adds none, `overwrite` when it does both |
| `summary` counters | `added-data-files`, `added-records`, `added-files-size` when the commit adds files. `deleted-data-files`, `deleted-records`, `removed-files-size` when it drops files. Always `total-equality-deletes` as 0 |
| `summary` totals, no external table | `total-data-files`, `total-records`, `total-files-size` of Mink's files, and `total-delete-files`, `total-position-deletes` as 0 |
| `summary` totals, with an external table | `total-data-files` and `total-records` of Mink's files plus the external data entries' file and row counts. `total-delete-files` and `total-position-deletes` are the external delete entries' file and row counts. A total is left out when an entry lacks its count. `total-files-size` is left out |
| `summary.mink.partition.{p}.start-offset`, `.end-offset` | The snapshot's offsets for partition `p` |
| `summary.mink.external.generation` | The generation of the listed external table |
| `summary.mink.external.snapshot-id` | The id of the external table snapshot whose entries are listed |
| `summary.mink.external.partition.{p}.end-offset` | The external table's end offset of partition `p` that its listed entries reach |

## Manifests

Every manifest holds the files of one partition spec and one time bucket (`layout.md` Manifests). The manifests a commit writes are `{location}/metadata/{N}-e{E}-m{k}.avro`, numbered from 0 in the order the manifest list names them.

- A commit that adds files writes one manifest per spec and bucket among them, holding them with status `ADDED`, data sequence number `S` and snapshot id `N`.
- A commit that assembles any of a bucket's files, or after which a bucket would have more than 16 manifests, writes that bucket's manifests as one: its live files as `EXISTING`, or `ADDED` when the commit adds them, and the files the commit replaces or drops as `DELETED`. The bucket's earlier manifests are no longer listed.
- A commit whose snapshot drops files of any other bucket writes one more manifest per earlier manifest that holds any of them, holding the dropped files as `DELETED` and the earlier manifest's other files as `EXISTING`. The earlier manifest itself is no longer listed.
- An earlier manifest that holds only `DELETED` entries is listed only by the snapshot that wrote it.
- Every other earlier manifest is listed again unchanged.
- Snapshot `N`'s manifest list lists its manifests in this order: the added-files manifests, then the rewritten buckets' manifests, then the manifests that replaced earlier ones, each group by spec and then bucket, then the reused earlier manifests in their earlier order. Each entry carries its manifest's partition summary: the bounds of `_kafka_partition` and of the bucket over its files.
- A table with an external table lists the external table's current manifest list entries after its own, each with the id its spec is listed under.

Each data file entry carries:

| Field | Value |
| --- | --- |
| `file_path` | The file's path (`file.md`) |
| `file_format` | `PARQUET` |
| `partition` | `{_kafka_partition: index}` |
| `record_count`, `file_size_in_bytes` | `records`, `bytes` from the file's stats |
| `lower_bounds`, `upper_bounds`, `null_value_counts`, `nan_value_counts` | The file's stats, keyed by field id |

## Table metadata file

`{location}/metadata/v{N}-e{E}.metadata.json` holds the table as of snapshot `N`. Its `metadata-log` lists the table metadata files of the earlier snapshots it still keeps.

Keys are in sorted order and arrays in the orders above.

## Object paths

In every path above, `N` is the snapshot's id and `E` its epoch, the epoch of the topic lease it is committed under. Every path follows from those two, and no path is stored separately.

## Tail

A tail extends the latest committed snapshot `N`, of epoch `E`, to the last stable offsets a load sees. It is never committed.

| Part | Value |
| --- | --- |
| Offsets | `N`'s, with each `end_offset` raised to the partition's last stable offset where that is higher |
| Files | `N`'s files, plus, per extended partition, one file per time bucket among the committed records of `[N's end_offset, last stable offset)` |
| Key `K` | The first 16 hex digits of the SHA-256 of the tail's offsets, one `partition:start_offset:end_offset` line per partition in partition order |
| `snapshot-id` | `2^62` plus the low 62 bits of `K`. Committed snapshot ids stay below `2^62`. |
| `sequence-number`, `parent-snapshot-id` | `S + 1`, `N` |
| Objects | `{location}/metadata/v{N}-e{E}-t{K}.metadata.json`, `snap-{N}-e{E}-t{K}.avro`, and one manifest `{N}-e{E}-t{K}-m{k}.avro` per spec and bucket of the tail's files, holding them as `ADDED` |
| Manifest list | The tail's manifests first, by spec and then bucket, then `N`'s manifests unchanged |
| Table metadata | `N`'s kept snapshots and the tail, the tail current. `metadata-log` adds `N`'s table metadata file |

A tail's paths are a function of `N`, `E` and its offsets. The same tail renders to the same table metadata bytes on any node.

## External table

A table's external table is an Iceberg table in an external catalog. Each tier commit adds one snapshot to it:

| Object | Value |
| --- | --- |
| Data file path | `{data}/{partition path}/{p}-{start}-{end}-{n}-{uuid}.parquet`. `{data}` is `write.data.path`, or `{location}/data`. `{p}`, `{start}` and `{end}` are the partition and offsets of the read that wrote the file, `{n}` a five-digit counter. |
| Data file rows | The records of the tiered offsets, partitioned by the table's default spec and sorted by its default sort order |
| Data file Parquet | `write.target-file-size-bytes`, `write.parquet.row-group-size-bytes`, `write.parquet.page-size-bytes`, `write.parquet.dict-size-bytes`, `write.parquet.compression-codec`, `write.parquet.compression-level`, `write.parquet.bloom-filter-enabled.column.*`, `write.parquet.bloom-filter-fpp.column.*` and `write.metadata.metrics.*` apply. `write.parquet.bloom-filter-max-bytes` does not. |
| Snapshot | `operation` `append`. A new manifest holds the commit's files as `ADDED`, and the manifest list keeps every earlier manifest. |
| `summary.mink.partition.{p}.start-offset`, `.end-offset` | The offsets of partition `p` the commit appends |
| `mink.partition.{p}.end-offset` | Table property set in the same commit: the first offset of partition `p` the external table does not hold |

## Expiry

When snapshots expire, they are removed from the table metadata, from `snapshot-log` and from `metadata-log`. The latest snapshot is never expired. Expiry writes no objects of its own: the next table metadata file reflects it.

## Determinism

The table metadata file is a function of the snapshots, their files, the topic's table schemas, the listed external table and the specs of every generation a retained snapshot lists. Avro manifests and manifest lists are not byte for byte: the Avro writer picks a random sync marker. Their contents and lengths are. A snapshot's objects are written before it is committed.

## Errors

| Error | When |
| --- | --- |
| `topic` | An applied snapshot belongs to another topic |
| `conflict` | A snapshot's id is not the one after the table's latest, or a tail extends a snapshot that is not the latest |
| `file_outside_snapshot` | An applied snapshot adds a file outside its offsets, or a tail adds one that is not past the latest snapshot's end |
| `latest_snapshot` | Expiry would remove the latest snapshot |
| `stats_field` | A file's stats name a field that is not a primitive field of the schema |
| `format_version` | Table metadata of a format version Mink does not write |
| `tail_offsets` | A tail whose offsets do not start where the latest snapshot's start, or end before them |
| `external_schema` | The external table's current schema is not the topic's latest table schema |
| `external_partition_field` | An external spec's partition field has an id from 1000 to 1003, the ids time specs take, and is not the time spec field with that id |
| `equality_deletes` | An external delete manifest holds equality deletes |
| `external_generation` | A generation outside 1 to 16383 |
| `external_spec_id` | An external spec id outside 0 to 2^16 - 1 |
| `external_offsets` | An applied snapshot starts a partition before the external table's end offset |
| `external_sequence_number` | An applied snapshot's sequence number is not above every external entry's |

## Invariants

1. Every object path above, other than the external table's, is under `{location}/metadata/` and is named by the snapshot and the lease epoch that wrote it.
2. Snapshot `N`'s own manifests list exactly the files of Mink snapshot `N`, each once, with that file's stats. With an external table, its manifest list entries follow them, unchanged but for their spec ids.
3. Every manifest holds one spec and one bucket. A commit writes one manifest per bucket it adds files to, one per bucket it assembles or merges, and one per other earlier manifest it changes, and reuses every other earlier manifest unchanged.
4. The table metadata file is the same bytes whenever it is rendered from the same snapshots and files.
5. Only format version 2 is written.
6. An external commit's `summary.mink.partition.{p}.start-offset` equals the previous commit's end offset, or is above it where Kafka deleted offsets before they were tiered. Its end offset equals the table property `mink.partition.{p}.end-offset`.

## Conformance

- `conformance/fixtures/metadata/`: a scripted topic's snapshots, each with its offsets, the files it adds, its lease epoch and the snapshots expired before it, and the objects it writes and releases, together with the table metadata file of every snapshot (`v{N}-e{E}.metadata.json`, byte for byte) and the parsed contents of every manifest list and manifest (`snap-{N}-e{E}.json`, `{N}-e{E}-m{k}.json`). `cases.json` records the format version, and files by their paths (`file.md`). Written by `cargo run -p mink-conformance -- generate metadata --bless`.
- `mink-iceberg` `table::tests::metadata_matches_the_fixtures`: invariants 1, 2, 3 and 4.
- `mink-core` `snapshot::tests::a_snapshot_covers_exactly_the_files_inside_its_offsets`: a snapshot's files.
- `mink-iceberg` `table::manifests::tests::dropping_files_rewrites_only_the_manifests_that_hold_them`: the Manifests rules.
- `mink-iceberg` `table::manifests::tests::manifests_with_no_live_files_are_not_listed_again`: the rule for manifests that hold only deleted files.
- `mink-iceberg` `table::tests::expiry_keeps_the_latest_snapshot`: the Expiry rules.
- `mink-iceberg` `table::tests::writers_with_different_epochs_never_share_a_path`: the Object paths rule.
- `mink-iceberg` `table::tests::the_metadata_loads_in_iceberg_rust`: iceberg-rust parses every table metadata file, manifest list and manifest Mink writes, and the live files it reads are exactly the files every step added that the snapshot covers (invariant 2), computed without the table's own state.
- `mink-iceberg` `table::tests::added_files_must_lie_inside_the_snapshot`: the `file_outside_snapshot` error.
- `mink-iceberg` `table::tests::a_tail_extends_the_latest_snapshot_without_committing`: the Tail rules, and that the tail's metadata loads in iceberg-rust.
- `mink-server` `writer::tiering::tests::a_tier_commit_sets_the_end_offsets_with_its_files`: the External table objects, read back through the external catalog.
- `mink-server` `tier::tests::tiering_continues_after_offsets_kafka_deleted_and_counts_the_gap`: the start offset that marks a gap.
- `mink-iceberg` `table::tests::a_snapshot_lists_the_external_table_after_its_own_manifests`: the external table's entries, specs, sort orders, sequence number, totals and end offsets, and the `external_offsets` and `external_sequence_number` errors.
- `mink-iceberg` `table::tests::an_unlisted_external_table_keeps_its_specs_until_its_snapshots_expire`: the specs and sort orders of an external table that is no longer listed.
- `mink-iceberg` `table::tests::a_new_generation_lists_beside_the_old_one_without_sharing_spec_ids`: the generation in listed spec ids.
- `mink-iceberg` `table::tests::the_external_files_plan_in_iceberg_rust`: iceberg-rust plans Mink's files and the external table's files from one snapshot.
- `mink-iceberg` `table::external::tests`: the `format_version`, `external_schema`, `external_partition_field` and `equality_deletes` errors.
- `mink-server` `writer::tiering::tests::the_merged_table_lists_each_record_once_after_a_writer_restart`: invariant 2 against the REST catalog fixture, and the partition starts after a restart.
