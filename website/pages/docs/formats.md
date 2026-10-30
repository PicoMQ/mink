# File formats

Every file Mink serves is pinned by a specification in the repository, with conformance fixtures that tests check byte for byte.

| Specification | Pins |
| --- | --- |
| [`file.md`](https://github.com/PicoMQ/mink/blob/main/specification/file.md) | A data file's path, Parquet bytes and statistics |
| [`records.md`](https://github.com/PicoMQ/mink/blob/main/specification/records.md) | How Kafka record batches become a file's rows |
| [`decoding.md`](https://github.com/PicoMQ/mink/blob/main/specification/decoding.md) | How Avro, Protobuf and JSON Schema payloads become typed columns |
| [`schema.md`](https://github.com/PicoMQ/mink/blob/main/specification/schema.md) | Table schemas, type mappings, field ids and evolution |
| [`layout.md`](https://github.com/PicoMQ/mink/blob/main/specification/layout.md) | Time partitioning, assembled files and manifest grouping |
| [`metadata.md`](https://github.com/PicoMQ/mink/blob/main/specification/metadata.md) | Snapshots, table metadata, manifest lists and manifests |
| [`statistics.md`](https://github.com/PicoMQ/mink/blob/main/specification/statistics.md) | The Puffin statistics file with per-column distinct-value sketches |

Fixtures live in [`conformance/fixtures/`](https://github.com/PicoMQ/mink/tree/main/conformance/fixtures). Independent readers check them: DuckDB, PyIceberg, Apache DataSketches and a real Kafka broker.
