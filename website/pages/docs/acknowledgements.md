# Acknowledgements

Mink stands on open source projects that solved hard problems well. This page names the ones whose design or code is part of what Mink is.

| Project | Contribution |
| --- | --- |
| [Apache Fluss](https://fluss.apache.org) | The data model. Log and primary-key tables, buckets, changelogs, merge engines, lakehouse tiering and union read are Fluss concepts. Mink follows its design and its documentation is the reference for how the two compare, see [Why not Apache Fluss](/docs/faq/fluss) |
| [AutoMQ](https://www.automq.com) | The stream engine design. [S3Stream](https://github.com/AutoMQ/automq/tree/main/s3stream) is AutoMQ's stream storage over object storage: WAL on S3, stream objects, the object layout and the caching and compaction that make a diskless broker work. Every log tablet in Mink is an S3Stream stream |
| [PicoMQ](https://picomq.com) | The Rust port of that engine, [s3stream](https://github.com/PicoMQ/s3stream), derived from AutoMQ S3Stream and used by Mink as a dependency. The SQL command log as a metadata plane, node leases and epoch fencing follow PicoMQ's host design. This website is built on the PicoMQ theme |
| [SurrealKV](https://github.com/surrealdb/surrealkv) by SurrealDB | The default KV store engine behind primary-key tables. Pure Rust, an LSM with a checkpoint layout of shared `.sst` files that makes tablet snapshots cheap to take and cheap to restore |
| [Apache DataFusion](https://datafusion.apache.org) | The SQL engine in `mink-query`. Parsing, planning, optimization and vectorized execution over Arrow. Mink supplies the catalog, the table provider and the physical scan, see [SQL](/docs/design/query) |

Mink is also built with [Apache Arrow](https://arrow.apache.org) and Arrow Flight, [Apache Iceberg](https://iceberg.apache.org) through `iceberg-rust`, and [Tokio](https://tokio.rs). All of the above are Apache 2.0 or MIT licensed. Mink is Apache 2.0.
