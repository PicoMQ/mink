# Why not Apache Fluss

Apache Fluss replaces Kafka with its own streaming storage. Producers, consumers and engines move to Fluss clients, and a Flink job tiers its tables into Paimon or Iceberg. The data is then held in two forms, the Fluss log and the lake table. Mink keeps Kafka and its clients as they are, and serves topics as Iceberg tables.

## How Fluss works

| Part | Fluss |
| --- | --- |
| Servers | CoordinatorServers and TabletServers, with ZooKeeper |
| Clients | Its own Java, Rust, Python and C++ clients, and Flink and Spark connectors. The Kafka protocol is off by default and still in development. |
| Tables | Log tables in Arrow, and primary key tables in RocksDB |
| Lake | A Flink tiering job writes Paimon, Iceberg or Lance tables, at most `table.datalake.freshness` behind, 3 minutes by default |
| Fresh reads | Union reads of Fluss and the lake, in Flink and Spark. Other engines read the lake only. |

## Differences

| | Fluss | Mink |
| --- | --- | --- |
| Clients | A Fluss integration in every producer, consumer and engine | Kafka clients stay. Engines use Iceberg. |
| Copies | The Fluss log, and the lake tables | Kafka only. The bucket holds metadata. |
| Freshest data in Trino, DuckDB or PyIceberg | What the tiering job last committed | Every record committed when the table loads |
| Moving parts | Fluss cluster, ZooKeeper, Flink tiering job | Mink nodes and Postgres |
| Updates | Primary key tables, written to Iceberg as equality deletes | None. One row per record. |
| History | Lake tables outlive the log's TTL | As far back as Kafka's retention |

## When Fluss fits

- Primary key tables with updates and point lookups.
- Flink and Spark pipelines that read fresh data through union reads.
- Lake history kept longer than the log.

## Sources

- [Fluss architecture](https://fluss.apache.org/docs/concepts/architecture/)
- [Fluss tiering service](https://fluss.apache.org/docs/streaming-lakehouse/tiering-service/)
- [Fluss union read](https://fluss.apache.org/docs/streaming-lakehouse/union-read/)
- [Fluss Iceberg format](https://fluss.apache.org/docs/streaming-lakehouse/datalake-formats/iceberg/)
- [Fluss configuration](https://fluss.apache.org/docs/maintenance/configuration/)
