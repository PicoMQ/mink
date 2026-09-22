# Why not Apache Fluss

::: info Fun fact
Fluss's mascot is an otter. A mink is the otter's smaller, more slender cousin in the same family, Mustelidae: partially webbed feet instead of fully webbed, solitary instead of living in groups, and stealthy rather than playful. The analogy: Mink carries no Flink, RocksDB or ZooKeeper dependency, keeps no durable local state and the node is one binary.
:::

[Apache Fluss](https://fluss.apache.org) is the project Mink is modelled on. The table model is: log tables and primary-key tables, buckets and partitions, a changelog per primary-key table, merge engines, lakehouse tiering with a union read over lake plus log. The difference is where the bytes live and how the cluster agrees on anything.

## Storage

Fluss tablet servers keep the log and the KV state on local disks and replicate each bucket to an in-sync replica set. Mink nodes hold no durable local state. Both the log and KV snapshots go to object storage. A node that dies is replaced by another node opening the same stream.

| | Mink | Fluss |
| --- | --- | --- |
| Log durability | WAL on object storage through s3stream, one copy in the object store | Local disks, ISR replication across tablet servers, `acks` |
| Log segments | Stream objects in the object store | Local segments, tiered to remote log storage after a delay |
| KV state | Local working copy rebuilt from a KV snapshot in the object store plus log replay | RocksDB on local disk, snapshots to remote storage |
| Cross-AZ traffic | Object store only | Replication between servers |
| Append latency | Object-store write latency, tens of milliseconds | Local disk plus replication, milliseconds |

## Coordination

| | Mink | Fluss |
| --- | --- | --- |
| Metadata | Ordered command log in Postgres or SQLite, tailed by every node | CoordinatorServer with ZooKeeper |
| Bucket leadership | Coordinator on the lease holder assigns. Stream epochs fence the old owner | Coordinator elects among replicas. Leader epoch fences |
| Failover | Any live node opens the stream with a higher epoch | Another in-sync replica becomes leader |
| Membership | Heartbeat rows in the metadata database | ZooKeeper sessions |

## Tables

| | Mink | Fluss |
| --- | --- | --- |
| Table types | Log, primary-key | Log, primary-key |
| Merge engines | Default (last write), `first_row`, `versioned`, `aggregation` | Default, `first_row`, `versioned`, `aggregation` |
| Partial update | Yes | Yes |
| Changelog image | `full` or `wal` | `full` or `wal` |
| Auto increment | Yes, one column | Yes, one column |
| Schema evolution | Add, drop, rename, modify with type promotion | Add, drop, rename, modify |
| Row format | Arrow IPC batches on the log, compacted rows in KV | Arrow or indexed rows on the log, compacted or indexed rows in KV |
| Bucketing | Native, Paimon and Iceberg hash functions | Native, Paimon and Iceberg hash functions |
| Default log TTL | 7 days | 7 days |

## Protocols

| | Mink | Fluss |
| --- | --- | --- |
| Native API | Arrow Flight over gRPC | Custom RPC over Netty |
| Kafka | Producers, consumers, consumer groups, idempotent producers. No transactions | Kafka protocol plugin |
| SQL | Flight SQL from `mink-query` on DataFusion | Flink SQL, Spark |
| Flink connector | Not yet | Source, sink, lookup join, delta join |
| Implementation | Rust, one static binary per role | Java, targets Java 11, runs on a JVM |
| Client languages | Rust | Java |
| Authentication | Not implemented | SASL/PLAIN, ACLs |

## Lakehouse

| | Mink | Fluss |
| --- | --- | --- |
| Formats | Iceberg | Paimon, Iceberg, Lance |
| Tiering | Worker on the coordinator leader, per-table freshness | Flink tiering service |
| Lake table schema | The user's schema | The user's schema since 1.0, `__bucket`, `__offset`, `__timestamp` on tables from earlier versions |
| Attach existing table | Yes, with schema and partition checks | No |
| Lake compaction | Tiering worker rewrites small files, opt in per table | Tiering service, opt in per table |
| Union read | Server-side, Arrow Flight and Flight SQL | Flink connector |
| Offset bookkeeping | `mink-offsets` snapshot property, per bucket | Snapshot property, per bucket |

::: info Coming soon
Mink tiering to Paimon, Lance and DuckLake.
:::

## Choosing

Both projects store tables, tier history into a lakehouse format and let lakehouse engines read the tiered files directly. They differ in where the work is done and what a client has to know.

| | Mink | Fluss |
| --- | --- | --- |
| Union read | On the server. A client asks for the table and gets lake plus log as one stream | In the Flink connector. The client loads the Paimon or Iceberg plugin and merges lake and log itself |
| What a client knows | Table, bucket, offset | Table, bucket, offset, the lake format and its catalog |
| Client path | Kafka, Arrow Flight, Flight SQL | Flink connector, Java client, Kafka protocol plugin |
| Compute coupling | None. Any engine over Flight SQL or Iceberg | Flink for tiering, joins and the primary connector. Spark and StarRocks in progress |
| Processes | `mink` nodes | CoordinatorServer, TabletServers, ZooKeeper, a Flink cluster for tiering and connectors |
| Durable state on nodes | None. Log and snapshots are objects | Log and RocksDB on local disk, replicated across TabletServers |
| Coordination | SQL database | ZooKeeper |
| Runtime | Rust binary | Java 11 JVM |

::: info Coming soon
Mink connectors for Flink, Spark, Trino and DuckDB.
:::
