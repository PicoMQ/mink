# Why not table topics

Kafka platforms write topics as Iceberg tables themselves: Confluent Tableflow, Redpanda Iceberg Topics, AutoMQ Table Topic, Aiven Iceberg Topics and StreamNative Ursa. It is the copy a connector makes, run by the platform. The table lags the topic, and each needs that platform. Mink runs next to any Kafka cluster and serves a topic as a table, with every record committed when the table loads.

## How they work

| Product | Platform | Freshness |
| --- | --- | --- |
| Confluent Tableflow | Confluent Cloud only | About 5 minutes |
| Redpanda Iceberg Topics | Redpanda Enterprise. On Redpanda Cloud, BYOC and BYOVPC only. | `iceberg_target_lag_ms`, 1 minute by default, 10 seconds at least |
| AutoMQ Table Topic | AutoMQ 1.5 or later, enabled when the cluster is deployed | `automq.table.topic.commit.interval.ms`, up to 15 minutes |
| Aiven Iceberg Topics | Apache Kafka with Aiven's tiered storage plugin. Alpha. | Once a segment rolls and is tiered |
| StreamNative Ursa for Kafka | A fork of Apache Kafka. Limited public preview. | Seconds to a minute after the write |

| Product | Writes the table | Limits |
| --- | --- | --- |
| Tableflow | Parquet in managed storage or your bucket, beside the topic | Schema Registry required for keys and values. Topics without a schema are not supported. |
| Redpanda | The broker writes Parquet, beside the tiered log segments | Turning it on does not backfill an existing topic |
| AutoMQ | Table workers in the broker, beside the stream storage | |
| Aiven | Parquet when a segment is tiered, as the only remote copy. Tiered reads are rebuilt from it. | Avro values and a Schema Registry only. The first schema sets the table schema, with no evolution. No transactional batches. |
| StreamNative | The write-ahead log in S3 is compacted into Parquet, as the only remote copy of a diskless topic | Diskless topics have no transactions and no compaction |

## Differences

| | Table topics | Mink |
| --- | --- | --- |
| Kafka | That vendor's platform, or Apache Kafka with Aiven's plugin | Any Kafka cluster, unchanged |
| Freshness | The table lags by the commit interval or the segment roll | Every record committed when the table loads |
| Copies | Most keep the log and Parquet. Aiven and Ursa keep one remote copy. | Kafka only. The bucket holds metadata. |
| Schemas | Required by Tableflow and Aiven | Optional. Without one, the key and value stay raw bytes. |
| Transactions | Not supported by Aiven or by Ursa's diskless topics | Committed records only, aborted ones skipped |
| Updates | Tableflow's upsert mode | None. One row per record. |
| History | The table can outlive the log | As far back as Kafka's retention |
| Engines | Any that reads the catalog and the storage | Any with an Iceberg REST catalog and a configurable S3 endpoint |

## When they fit

- Topics already on that platform.
- Tables kept longer than the log's retention.
- Upsert tables from compacted topics, with Tableflow.
- Engines that read only from cloud storage, such as managed warehouse services.

## Sources

- [Confluent Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html)
- [Redpanda Iceberg Topics](https://docs.redpanda.com/current/manage/iceberg/about-iceberg-topics/)
- [AutoMQ Table Topic](https://docs.automq.com/automq/table-topic)
- [StreamNative Ursa for Kafka](https://streamnative.io/blog/ursa-for-kafka-deep-dive-write-path-read-path-and-data-compaction)
- [Aiven Iceberg Topics whitepaper](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/blob/main/iceberg_whitepaper.md)
