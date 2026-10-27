# Metadata

Each committed snapshot of a topic becomes a snapshot of an Iceberg table, written as a table metadata file, a manifest list and manifests. This document pins what Mink writes, where, and how each follows from the snapshot and its files. Conformance fixtures in `../conformance/fixtures/metadata/` pin it.

## Format version

`FormatVersion` names the Iceberg table format Mink writes. Only the versions below exist, and every version-specific choice is made by matching on the version, so a new version is a new entry here, new fixtures, and a new variant. A table's format version is fixed when the table is created.

| Version | Status |
| --- | --- |
| 2 | Written for every table |

Format version 3 is not supported yet. Supporting it means defining row lineage for Mink tables and adding it here.

## Table

| Field | Value |
| --- | --- |
| `format-version` | 2 |
| `table-uuid` | The topic's uuid |
| `location` | `{warehouse}/{topic}` |
| Schemas | The binary schema, id 0, and every typed schema of the topic (`schema.md`), in id order, as Postgres holds them when the snapshot is rendered |
| `current-schema-id` | The latest of them when the snapshot was rendered |
| `last-column-id` | The highest field id of any of them |
| Partition specs | Every time spec the topic has had (`layout.md` Time partitioning), as Postgres holds them when the snapshot is rendered, in id order; `default-spec-id` the current one; `last-partition-id` the highest partition field id among them |
| Statistics | The latest statistics file (`statistics.md`), when the writer has published one |
| Sort order | Unsorted, order id 0 |
| Properties | None |
| `current-snapshot-id` and the `main` branch | The latest snapshot |
| `last-sequence-number` | The latest snapshot's id |
| `last-updated-ms` | The latest snapshot's `timestamp_ms` |

## Snapshots

A Mink snapshot is the state of one topic's table at one moment: for each partition, the offsets `[start_offset, end_offset)` the table covers, with an id `N`, a `timestamp_ms` and the epoch `E` of the lease it was committed under. Ids start at 1, and each commit takes the one after the latest. A snapshot's files are those whose ranges lie inside its offsets, less the files an assembly replaced at or before it and the assembled files added after it (`layout.md` Assembly).

Iceberg snapshot `N` is Mink snapshot `N`:

| Field | Value |
| --- | --- |
| `snapshot-id`, `sequence-number` | `N` |
| `parent-snapshot-id` | `N - 1`, absent for the first snapshot |
| `timestamp-ms` | The snapshot's `timestamp_ms` |
| `schema-id` | The table's `current-schema-id` when the snapshot was rendered |
| `manifest-list` | `{location}/metadata/snap-{N}-e{E}.avro` |
| `summary.operation` | `append` when the commit drops no files, `delete` when it drops files and adds none, `overwrite` when it does both |
| `summary` counters | `added-data-files`, `added-records`, `added-files-size` when the commit adds files; `deleted-data-files`, `deleted-records`, `removed-files-size` when it drops files; always `total-data-files`, `total-records`, `total-files-size`, and `total-delete-files`, `total-position-deletes`, `total-equality-deletes` as 0 |
| `summary.mink.partition.{p}.start-offset`, `.end-offset` | The snapshot's offsets for partition `p` |

## Manifests

Every manifest holds the files of one partition spec and one time bucket (`layout.md` Manifests). The manifests a commit writes are `{location}/metadata/{N}-e{E}-m{k}.avro`, numbered from 0 in the order the manifest list names them.

- A commit that adds files writes one manifest per spec and bucket among them, holding them with status `ADDED`, data sequence number `N` and snapshot id `N`.
- A commit that assembles any of a bucket's files, or after which a bucket would have more than 16 manifests, writes that bucket's manifests as one: its live files as `EXISTING`, or `ADDED` when the commit adds them, and the files the commit replaces or drops as `DELETED`. The bucket's earlier manifests are no longer listed.
- A commit whose snapshot drops files of any other bucket writes one more manifest per earlier manifest that holds any of them, holding the dropped files as `DELETED` and the earlier manifest's other files as `EXISTING`. The earlier manifest itself is no longer listed.
- An earlier manifest with no live files, one that holds only `DELETED` entries, is not listed again: the snapshot that wrote it lists it, so readers see the deletes, and later snapshots release it.
- Every other earlier manifest is listed again unchanged.
- `Table::apply` returns, with the objects it wrote, the objects the new snapshot releases: the previous table metadata file, the previous manifest list, the previous statistics file when the commit writes a new one, and every earlier manifest it no longer lists.
- Snapshot `N`'s manifest list lists its manifests in this order: the added-files manifests, then the rewritten buckets' manifests, then the manifests that replaced earlier ones, each group by spec and then bucket; then the reused earlier manifests in their earlier order. Each entry carries its manifest's partition summary: the bounds of `_kafka_partition` and of the bucket over its files.

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

Mink renders this file itself rather than through iceberg-rust's table metadata builder, whose output orders snapshots by hash and stamps the wall clock. Keys are in sorted order and arrays in the orders above, so the bytes depend only on the snapshots and their files.

## Loading

A writer that takes over a topic loads the table from the objects of its latest snapshot: the table metadata file at the path its id and epoch name, its manifest list and its manifests. The loaded table applies the next snapshot exactly as the table that wrote those objects would have. Loading reads no schemas or specs: every commit and tail is given the topic's time specs and table schemas from Postgres (`Table::apply` and `Table::tail`). Loading fails with `corrupt` if the metadata names another table, does not end at that snapshot, or lists a data file that is not a file of the topic (`file.md`).

## Object paths

In every path above, `N` is the snapshot's id and `E` its epoch, the epoch of the topic lease it is committed under. `Table` derives every path from those two; nothing stores a path separately. Two writers therefore never write the same path: a writer that lost its lease can still write objects, but only under its own epoch, and it cannot commit them. A writer retrying its own commit rewrites only its own objects.

## Tail

A tail extends the latest committed snapshot `N`, of epoch `E`, to the last stable offsets a load sees. It is rendered by `Table::tail(committed, offsets, added, specs, schemas)`, where `added` holds the tail's files, and never committed.

| Part | Value |
| --- | --- |
| Offsets | `N`'s, with each `end_offset` raised to the partition's last stable offset where that is higher |
| Files | `N`'s files, plus, per extended partition, one file per time bucket among the committed records of `[N's end_offset, last stable offset)` |
| Key `K` | The first 16 hex digits of the SHA-256 of the tail's offsets, one `partition:start_offset:end_offset` line per partition in partition order |
| `snapshot-id` | `2^62` plus the low 62 bits of `K`; committed snapshot ids stay below `2^62`, so the two never collide |
| `sequence-number`, `parent-snapshot-id` | `N + 1`, `N` |
| Objects | `{location}/metadata/v{N}-e{E}-t{K}.metadata.json`, `snap-{N}-e{E}-t{K}.avro`, and one manifest `{N}-e{E}-t{K}-m{k}.avro` per spec and bucket of the tail's files, holding them as `ADDED` |
| Manifest list | The tail's manifests first, by spec and then bucket, then `N`'s manifests unchanged |
| Table metadata | `N`'s kept snapshots and the tail, the tail current; `metadata-log` adds `N`'s table metadata file |

A tail releases nothing. Its objects are recorded as unreferenced when they are written, and its paths are a function of `N`, `E` and its offsets, so the same tail renders to the same table metadata bytes on any node.

## Expiry

When snapshots expire, they are removed from the table metadata, from `snapshot-log` and from `metadata-log`. The latest snapshot is never expired. Expiry writes no objects of its own: the next table metadata file reflects it. The objects of expired snapshots are deleted by collection.

## Determinism

The table metadata file is a function of the snapshots, their files and the topic's table schemas, so writing it twice gives the same bytes. Avro manifests and manifest lists are not: the Avro writer picks a random sync marker. So the Avro objects are not rebuilt to check or repair them: each is written at a path named by the snapshot and the epoch that wrote it. A writer retrying its own commit writes the same contents again; only the Avro sync markers differ, and the manifest lengths do not change. The objects of a snapshot are written before the snapshot is committed, so a committed snapshot's metadata location always names an existing file.

## Errors

| Error | When |
| --- | --- |
| `topic` | An applied snapshot belongs to another topic |
| `conflict` | A snapshot's id is not the one after the table's latest, or a tail extends a snapshot that is not the latest |
| `file_outside_snapshot` | An applied snapshot adds a file outside its offsets, or a tail adds one that is not past the latest snapshot's end |
| `latest_snapshot` | Expiry would remove the latest snapshot |
| `stats_field` | A file's stats name a field that is not a primitive field of the schema |
| `format_version` | Loading table metadata of a format version Mink does not write |
| `corrupt` | Loading fails as the Loading section says |
| `tail_offsets` | A tail whose offsets do not start where the latest snapshot's start, or end before them |
| `object_read` | Loading cannot read an object from the warehouse |

## Invariants

1. Every object path above is under `{location}/metadata/` and is named by the snapshot and the lease epoch that wrote it.
2. Snapshot `N`'s manifests list exactly the files of Mink snapshot `N`, each once, with that file's stats.
3. Every manifest holds one spec and one bucket. A commit writes one manifest per bucket it adds files to, one per bucket it assembles or merges, and one per other earlier manifest it changes, and reuses every other earlier manifest unchanged.
4. The table metadata file is the same bytes whenever it is rendered from the same snapshots and files.
5. Only format version 2 is written.

## Conformance

- `conformance/fixtures/metadata/`: a scripted topic's snapshots, each with its offsets, the files it adds, its lease epoch and the snapshots expired before it, and the objects it writes and releases, together with the table metadata file of every snapshot (`v{N}-e{E}.metadata.json`, byte for byte) and the parsed contents of every manifest list and manifest (`snap-{N}-e{E}.json`, `{N}-e{E}-m{k}.json`). `cases.json` records the format version, and files by their paths (`file.md`). Written by `cargo run -p mink-conformance -- generate metadata --bless`.
- `mink-iceberg` `table::tests::metadata_matches_the_fixtures`: invariants 1, 2, 3 and 4.
- `mink-core` `snapshot::tests::a_snapshot_covers_exactly_the_files_inside_its_offsets`: a snapshot's files.
- `mink-iceberg` `table::manifests::tests::dropping_files_rewrites_only_the_manifests_that_hold_them`: the Manifests rules.
- `mink-iceberg` `table::manifests::tests::manifests_with_no_live_files_are_not_listed_again`: the rule for manifests that hold only deleted files.
- `mink-iceberg` `table::tests::expiry_keeps_the_latest_snapshot`: the Expiry rules.
- `mink-iceberg` `table::tests::writers_with_different_epochs_never_share_a_path`: the Object paths rule.
- `mink-iceberg` `table::tests::the_metadata_loads_in_iceberg_rust`: iceberg-rust parses every table metadata file, manifest list and manifest Mink writes, and the live files it reads are exactly the files every step added that the snapshot covers (invariant 2), computed without the table's own state.
- `mink-iceberg` `table::tests::a_loaded_table_applies_like_the_table_it_was_loaded_from`: the Loading rule.
- `mink-iceberg` `table::tests::loading_rejects_metadata_that_is_not_the_snapshot_of_the_topic`: the Loading failures.
- `mink-iceberg` `table::tests::added_files_must_lie_inside_the_snapshot`: the `file_outside_snapshot` error.
- `mink-iceberg` `table::tests::a_tail_extends_the_latest_snapshot_without_committing`: the Tail rules, and that the tail's metadata loads in iceberg-rust.
