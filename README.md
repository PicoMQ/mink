# Mink

Mink is a read-only Iceberg catalog over Kafka. Iceberg engines query topics as tables, and the records stay in Kafka. It is tested against Apache Kafka 3.9.1.

[Documentation](https://minkdb.com/docs/) · [Contribute](https://minkdb.com/docs/contribute)

## Status

Built and tested:

- A Kafka client that reads the committed records of any offset range, verified against librdkafka's `read_committed` consumer and Apache Kafka 3.9.1.
- Kafka records as rows of the binary table schema.
- Parquet data files built from offset ranges, read back by DuckDB, and the Iceberg table metadata over them, read back by iceberg-rust.
- Mink's state in Postgres: each partition's indexed range and files, snapshots, leases, and the metadata objects retention collects.
- A node with three roles: per-partition indexers and a writer per topic that commits snapshots, a read-only Iceberg REST catalog whose loads reach each partition's last stable offset, and an S3 endpoint that builds data files from Kafka on read.
- DuckDB and PyIceberg reading Mink tables end to end, including records produced a moment before the query.
- Keys and values written with Schema Registry schemas, in Avro, Protobuf or JSON Schema, read as typed columns. Field ids stay stable as schemas evolve. A part Mink cannot decode keeps its raw bytes. This is checked against fixtures written by Confluent's own serializers.
- Tables partitioned by hour, day or month of the record timestamp, set per topic. A closed hour's small files are assembled into one file per partition without reading Kafka again, and each hour has one manifest. A one-hour query in DuckDB 2.0 or PyIceberg opens only that hour's manifests and files.
- Distinct-value sketches per column, published as an Iceberg statistics file for cost-based planners.
- Many nodes over one Postgres. Partitions and writers spread evenly over the nodes indexing a topic, and move by themselves when nodes join, leave or crash. Each data file is built and cached on one owner node, and other nodes forward its reads there. Nodes ride out a Postgres restart without restarting.

Next: a disk cache.

## Design

- **Metadata from the log.** One indexer per partition reads each record once and turns closed offset ranges into Iceberg data files with statistics. A snapshot is one offset range per partition.
- **Bytes on demand.** A data file is a fixed offset range. When an engine reads it, Mink builds the Parquet from Kafka, the same bytes every time.
- **Not a copy.** Kafka holds the only copy of the records. Mink stores metadata, never data files.
- **Real time.** A table includes every record committed in Kafka when an engine loads it.
- **Stateless nodes.** Postgres holds what Mink knows. Every cache can be rebuilt from Kafka or Postgres.

The contracts are in [`specification/`](specification/).

## Build and test

```bash
cargo build --workspace
cargo test
```

`cargo test` runs every crate except `mink-conformance`. Tests that need a service run when its variable is set: `TEST_POSTGRES_URL`, `TEST_KAFKA_BOOTSTRAP`, `TEST_S3_ENDPOINT` and `TEST_SCHEMA_REGISTRY`. Otherwise they are skipped, except when `CI` is set, where a missing variable fails them. The compose stack in `harness/` provides the services:

```bash
docker compose -f harness/compose.yml up -d postgres kafka rustfs schema-registry createbucket
TEST_POSTGRES_URL=postgres://mink:mink@localhost:5432/mink \
TEST_KAFKA_BOOTSTRAP=localhost:29092 \
TEST_S3_ENDPOINT=http://localhost:9000 \
TEST_SCHEMA_REGISTRY=http://localhost:8081 \
cargo test
```

`scripts/e2e.sh` runs the end-to-end tests in Docker: `mink-conformance` against DuckDB, PyIceberg, Spark and Trino, and the console's browser tests. CI runs it on demand.

## License

Apache 2.0. See [NOTICE](NOTICE) for derived work.
