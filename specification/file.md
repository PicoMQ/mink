# File

A file holds the records of an immutable range of offsets in one Kafka partition that fall in one time bucket, or, assembled, the records of several such files (`layout.md`). Its identity determines its path and, together with the records Kafka holds for the range and what Postgres holds for its schema and parts, its bytes. Conformance fixtures in `../conformance/fixtures/path/` pin the path format, and fixtures in `../conformance/fixtures/data/` pin the bytes.

## Identity

| Field | Type | Rule |
| --- | --- | --- |
| `partition.topic` | string | A legal Kafka topic name: 1 to 249 characters from `[A-Za-z0-9._-]`, and not `.` or `..` |
| `partition.index` | i32 | `>= 0` |
| `start_offset` | i64 | `>= 0`, the first offset in the range |
| `end_offset` | i64 | `> start_offset`, the first offset after the range |
| `spec_id` | i32 | `>= 0`, the topic's time spec the file is written with (`layout.md` Time partitioning) |
| `bucket` | optional i32 | The time bucket of every record the file holds; absent exactly when the spec has no time field |
| `schema_id` | i32 | `>= 0`, the Iceberg schema the file is written with: 0, the binary schema, or one of the topic's typed schemas (`schema.md`) |
| `assembly` | optional 16 hex digits | For an assembled file, the first 16 hex digits of the SHA-256 of its parts, one `start_offset:end_offset` line per part in offset order. Its parts are the ranges of the files the indexer wrote whose records it holds: at least two, the first starting at `start_offset` and the last ending at `end_offset` (`layout.md` Assembly). They are stored with the file and read when it is built. Absent for a file the indexer wrote |
| `version` | `FileVersion` | One of the file versions defined under Bytes. Rendered as its number. |

The range is half-open: `[start_offset, end_offset)`. A file holds the committed records of its range whose bucket is `bucket`, or every committed record of its range when it has no bucket. An assembled file holds the records of its parts' ranges whose bucket is `bucket`.

## Path

```
{warehouse}/{topic}/data/p={index}/{start_offset}-{end_offset}.t{spec_id}[_{bucket}].s{schema_id}[.a{assembly}].v{version}.parquet
```

- `_{bucket}` is present exactly when the file has a bucket, rendered in base 10 with a leading `-` when negative.
- `.a{assembly}` is present exactly for an assembled file, in lowercase hex.

- `warehouse` is rendered without a trailing `/`.
- Numbers are rendered in base 10 with no sign, no leading zeros and no padding.
- `v{version}` is the number of a file version defined under Bytes. A path naming any other number does not parse.
- A path parses to a file only if that file renders back to exactly the same path. Every file therefore has exactly one path, and every path names at most one file.

Example: partition 3 of `orders.v2`, offsets `[1200000, 1400000)`, spec 0 by hour, bucket 499260, schema 2, file version 1, under `s3://mink/kafka`:

```
s3://mink/kafka/orders.v2/data/p=3/1200000-1400000.t0_499260.s2.v1.parquet
```

## Bytes

A file's bytes are a Parquet file. They are a function of the file's identity, the committed records of its range (`records.md`), and, for a typed schema, the stored table schema and registry schemas, which never change. Building the same file twice gives the same bytes. A file holds at least one record: an offset range with no committed records has no file.

`FileVersion` names the rules that turn records into bytes. Only the versions below exist, and every version-specific choice is made by matching on the version, so a new version is a new entry here, new fixtures, and a new variant. A file version never changes once released, because files written with it must rebuild to the same bytes for as long as a snapshot references them.

### File version 1

| Setting | Value |
| --- | --- |
| Columns | The file's schema (`schema.md`), in schema order, each Parquet field carrying its Iceberg field id |
| Rows | One per committed record, in offset order, built as `records.md` Rows says for the binary schema and `decoding.md` Rows for a typed schema |
| Compression | ZSTD, level 3 |
| Row groups | At most 131,072 rows each |
| Statistics | Parquet column chunk statistics |
| Page index | None: no column index and no offset index, so a file's bytes are `PAR1`, its row groups, its footer, the footer's length and `PAR1` |
| Dictionary encoding | On |
| Writer version | Parquet 1.0 |
| `created_by` | `mink file version 1` |
| Key-value metadata | None, including no embedded Arrow schema |
| Everything else | The defaults of the `parquet` crate version in `Cargo.lock` |

A `parquet` crate upgrade that changes any byte of a version 1 file is caught by the data fixtures. Such an upgrade needs a new file version, not new fixtures.

### Assembled bytes

An assembled file's bytes are `PAR1`; then each part's bytes from offset 4 up to its footer, its row groups, in part order; then the merged footer, its length as a 4-byte little-endian integer, and `PAR1`. Each part is built as the file of its range, bucket, spec, schema and version, from the same records.

The merged footer is the parts' Parquet `FileMetaData` with their row groups in part order; each row group's `file_offset` and every column chunk's `data_page_offset` and `dictionary_page_offset` moved by where the part's row groups now start (a column chunk's own `file_offset` is always 0); each row group's `ordinal` its position in the file; and `num_rows` the sum of the parts'. Its schema and `created_by` are the parts', which all parts share. The footer is serialized as the `parquet` crate serializes any footer, so its length follows from the parts' stored footers alone.

### Stats

Writing a file also yields its `Stats`, computed from the rows, not read back from the Parquet footer:

| Stat | Value |
| --- | --- |
| `records` | The number of rows |
| `bytes` | The length of the file |
| `first_timestamp_ms` | The earliest record timestamp of the file, in milliseconds; for an assembled file, the earliest of its parts |
| `lower`, `upper` | For `_kafka_partition` (1), `_kafka_offset` (2), `_kafka_timestamp` (3), and every primitive column of a typed schema that is not inside a list or a map, other than `binary` and `fixed`: the smallest and largest non-null value, in Iceberg's single-value binary encoding. A `string` bound longer than 16 characters is truncated to its first 16, and the upper bound's last character is then raised by one; a column with no non-null value has no bound. `float` and `double` bounds leave out NaN |
| `nulls` | For every leaf field: the number of null values |
| `nans` | For every `float` and `double` leaf field: the number of NaN values |

Stats have one JSON form, used by the store and the fixtures: an object with `records`, `bytes` and `first_timestamp_ms` as numbers; `lower` and `upper` as objects from field id to the hex of the value's Iceberg single-value encoding; `nulls` and `nans` as objects from field id to a count. Anything else is a corrupt row.

### Errors

| Error | When |
| --- | --- |
| `no_records` | Writing a file from no records |
| `record_outside_file` | A record's offset is outside the file's range |
| `record_out_of_order` | A record's offset is not after the one before it |
| `file_schema` | A file is built with a schema whose id is not the file's `schema_id` |

## Invariants

1. `parse(warehouse, render(warehouse, file)) == file` for every file whose fields satisfy the identity rules.
2. `parse` returns nothing for any path that is not the rendering of a valid file. That includes other warehouses, other directories, signs, leading zeros, out-of-range numbers, empty or reversed ranges, and illegal topic names.
3. A topic name never contains `/`, so the topic is always exactly one path segment.
4. Building the same file twice gives the same bytes and the same stats, whether or not the registry is reachable.
5. A file is never empty.

## Conformance

- `mink-iceberg` `file::tests::typed_files_carry_bounds_for_their_primitive_columns`: Stats for a typed schema: string bounds truncated to 16 characters, NaN counts, and nulls of optional and raw columns.
- `conformance/fixtures/path/cases.json`: `files` lists identities with the path each must render to. `rejected` lists paths that must not parse, each named for the reason it is rejected, including file versions that do not exist.
- `conformance/fixtures/data/cases.json`: for each record fixture with committed records, the file's path in warehouse `s3://warehouse` (topic `conformance`, partition 0, from the first committed offset to one past the last, spec 0 with no time field, schema 0, version 1), the expected stats, and `<case>.parquet`, the version 1 bytes. `cargo run -p mink-conformance -- generate data --bless` writes them from the record fixtures. They pin the bytes; Mink's own output is not proof of correctness, so independent readers check them (below).
- `mink-core` `location::tests::files_render_and_parse_as_the_fixtures_say` covers invariant 1 against the fixtures.
- `mink-core` `location::tests::rejected_paths_in_the_fixtures_do_not_parse` covers invariant 2.
- `mink-core` `location::tests::any_valid_file_round_trips_through_its_path` covers invariant 1 as a property over all valid identities.
- `mink-core` `topic::tests::names_follow_kafka_topic_rules` and `topic::tests::a_topic_needs_at_least_one_partition` cover the topic rules.
- `mink-core` `topic::tests::partitions_are_rejected_with_an_illegal_topic_or_a_negative_index` and `file::tests::files_are_rejected_with_an_empty_range_or_a_negative_id` cover the identity rules for every file that can be constructed, not only for parsed ones.
- `mink-iceberg` `file::tests::files_match_the_data_fixtures`: invariant 4 and the version 1 settings, byte for byte.
- `mink-iceberg` `file::tests::stats_follow_the_rows`: the Stats table.
- `mink-iceberg` `file::tests::files_read_back_to_their_rows`: the Parquet reader of the `parquet` crate returns the rows that were written, and the footer shows the version 1 settings: field ids, ZSTD, row groups of at most 131,072 rows, writer version 1, `created_by` and no key-value metadata.
- `mink-iceberg` `file::tests::records_must_lie_inside_their_file_in_offset_order`: the Rows setting; records outside the file's range or out of offset order are rejected.
- `mink-iceberg` `file::tests::a_file_needs_at_least_one_record`: invariant 5.
- `mink-store` `stats::tests::stats_round_trip_through_their_json_form`: the stats JSON form.
- `mink-core` `offsets::tests::offset_ranges_are_non_negative_and_ordered`: the offset range of every file and snapshot.
- `mink-core` `file::tests::assembled_files_need_ordered_parts_and_name_them`: the `assembly` identity field.
- `mink-iceberg` `file::tests::a_range_splits_into_one_file_per_bucket`: a file holds the records of its bucket only.
- `mink-iceberg` `assembly::tests::an_assembly_matches_the_bytes_of_its_parts` and `assembly::tests::assembled_files_read_back_as_their_parts`: Assembled bytes.
- `mink-conformance` `data_fixtures_read_in_duckdb`: DuckDB reads every data fixture and returns its records and field ids. It runs the DuckDB CLI in `TEST_DUCKDB`.
