# Layout

A topic's files are laid out by time, so that an engine reading one hour reads one hour's files and opens one hour's manifests. This document pins a topic's time partitioning, how an ended time bucket's small files are assembled into coarser ones without reading Kafka, and how the manifests are grouped.

## Time partitioning

A topic's time partitioning is one of `hour`, `day`, `month` or `none`. Each of the first three is the Iceberg transform of the same name applied to `_kafka_timestamp`; its value for a record is the record's bucket: hours, days or months since the epoch, in UTC, rounded down, so a timestamp before the epoch has a negative bucket. A bucket beyond the range of a 32-bit integer is held at its nearest end, so timestamps hundreds of millennia away share the first or the last bucket.

| Granularity | Partition fields |
| --- | --- |
| `hour`, `day`, `month` | `_kafka_partition` (field id 1000), identity of source field 1; `_kafka_timestamp_{granularity}`, the transform of source field 3 (`_kafka_timestamp`), with field id 1001 for the first time granularity the topic has had, 1002 for the next different one, and so on: a partition field id names one field across all of a table's specs |
| `none` | `_kafka_partition` (field id 1000), identity of source field 1 |

A table's granularity is recorded when the table is added, as given or else from the `granularity` setting. The recorded value is the only one any node uses.

Each granularity a topic has had is one of its time specs, numbered from 0 in the order they were recorded, and each time spec is one partition spec of the Iceberg table. The latest is the topic's current spec, and every file is written with the spec that was current when it was written. Changing a table's granularity records a new spec when the granularity differs from the current one; files already written keep theirs, and the table lists every spec (`metadata.md` Table). Assembly never mixes specs.

## Files of a range

An offset range ends when its records' estimated size, the sum of their key, value and header bytes, reaches `file_bytes`, when their timestamps span `file_window`, or at the first record whose bucket is later than the bucket of every record before it in the range. A range's records then normally share one bucket. A record whose timestamp is earlier than its neighbours' (Kafka keeps producer timestamps as they were written) can still fall in an earlier bucket.

A range becomes one file per bucket among its committed records: the file of bucket `b` holds the records of the range whose bucket is `b` (`file.md` Identity). The files of one range share its offsets, and the range is indexed once all of them are added. Under `none`, a range is one file with no bucket.

## Assembly

A bucket of a partition has ended once its end is at least `assembly_delay` before now (`TimeSpecs::bucket_ended`). Files with no bucket, under `none`, are never assembled. The writer assembles the files of each ended bucket: for every partition, spec, bucket and schema, it groups the live files in offset order into runs whose summed bytes stay within `assembled_bytes`, and replaces each run of two or more files with one assembled file. A run may hold an assembled file: records that reach a bucket after it was assembled land in new small files, and the next assembly adds them to it.

An assembled file is a file (`file.md` Identity) whose parts are the ranges of the indexer's files it holds, in offset order: the files it replaces, with an assembled one among them standing for its own parts. Its bytes are those files' row groups, byte for byte, under one footer (`file.md` Assembled bytes). The writer reads the replaced files' footers, parts and table schema from Postgres, and `mink_iceberg::assemble` computes the file's length, stats and footer from them, without reading Kafka:

| Field | Value |
| --- | --- |
| Length | `4 + Σ (length − footer length − 12) + merged footer length + 8`, summed over the replaced files |
| `records` | The sum of the replaced files' records |
| `lower`, `upper` | The smallest lower and the largest upper bound of each field over the replaced files |
| `nulls`, `nans` | The sums of the replaced files' counts |
| Footer | The replaced files' footers merged (`file.md` Assembled bytes), stored like any file's footer |

The writer adds the assembled files and replaces their parts in the commit that drops the parts from the table. A replaced file stays buildable for as long as a kept snapshot lists it (`metadata.md` Snapshots), and its row is removed with the last such snapshot.

## Manifests

A manifest holds the files of one spec and one bucket, so its partition summary in the manifest list names one bucket and an engine skips every manifest outside its time range. PyIceberg 0.12 and DuckDB 2.0 skip manifests by their partition summaries; DuckDB 1.3 opens every manifest and skips only data files, by partition value and column bounds. Without a time field every manifest's summary spans all time, so neither skips any.

1. The files a commit adds are written to one new manifest per spec and bucket among them.
2. A commit that assembles any of a bucket's files rewrites all of that bucket's manifests into one, holding its live files as `EXISTING` or `ADDED` and the files the commit replaces as `DELETED`.
3. A bucket with more than 16 manifests after a commit has them merged into one in that commit, in the same way.
4. Files with no bucket (`none`) are one bucket for these rules.

A table with `n` buckets in Kafka's retention therefore lists at most one manifest per ended, assembled bucket and at most 16 per bucket that has not ended.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `granularity` | `hour` | The granularity a table is added with when none is given |
| `assembly_delay` | `10m` | How long after its end a bucket counts as ended |
| `assembled_bytes` | 1073741824 | The most bytes one assembled file holds |

## Errors

| Error | When |
| --- | --- |
| `unknown_time_spec` | A file names a time spec its topic does not have |
| `assembly_files` | An assembly whose replaced files differ from it in partition, spec, bucket, schema or file version, or that replaces fewer than two files |
| `parts` | An assembled file's parts overlap, are out of offset order, are empty, are fewer than two, or do not match the file's assembly hash |
| `missing_footer` | An assembly part has no stored footer |

## Invariants

1. Every row of a file with a bucket has that bucket, so the file's partition value holds for all of it.
2. Assembly never reads Kafka; an assembled file's length, stats and footer follow from the stored rows of the files it replaces.
3. An assembled file's rows are its parts' rows, in part order, and its bytes rebuild to exactly the length the writer computed.
4. A query over one bucket opens only that bucket's manifests and reads only that bucket's files.

## Conformance

- `mink-iceberg` `assembly::tests::an_assembly_matches_the_bytes_of_its_parts`: invariants 2 and 3, over generated runs of parts, including merging an assembled file's footer with more parts, which must equal merging every part at once.
- `mink-iceberg` `assembly::tests::assembled_files_read_back_as_their_parts`: an assembled file read back by the `parquet` crate's Arrow reader holds its parts' rows in order.
- `mink-iceberg` `file::tests::a_range_splits_into_one_file_per_bucket`: Files of a range and invariant 1, with an out-of-order record.
- `mink-iceberg` `ranges::tests::a_range_becomes_one_file_per_bucket_with_its_offsets`: Files of a range, as the indexer and the tail build them: one file per bucket, each with the range's offsets.
- `mink-iceberg` `table::manifests::tests::manifests_hold_one_bucket_each`: Manifests rules 1 to 3, and the partition summaries iceberg-rust reads.
- `mink-core` `granularity::tests::buckets_follow_the_iceberg_transforms` and `granularity::tests::a_bucket_ends_where_the_next_begins`: buckets and their ends.
- `mink-server` `indexer::tests::ranges_end_where_a_later_bucket_begins`: Files of a range, the indexer's rule.
- `mink-server` `indexer::retention::tests::trims_never_split_an_assembled_file`: the retention trim around assembled files.
- `mink-server` `writer::assembly::tests::ended_buckets_are_assembled_within_their_size`: Assembly, with a late file merged in by a later assembly and the parts kept flat.
- `mink-server` `writer::assembly::tests::runs_stay_within_the_assembled_size`: runs within `assembled_bytes`.
- `mink-store` `files::tests::assembled_files_replace_their_parts_from_their_snapshot_on`: the files of snapshots around an assembly.
- `mink-conformance` `engines_read_only_the_queried_hour`: invariant 4 end to end, with DuckDB 2.0 or later. Records stamped across three hours are produced and assembled; DuckDB and PyIceberg each query one hour, and the node's S3 endpoint serves manifests and data files of that hour only.
