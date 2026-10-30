# Why not connectors

A connector is a stream processor that copies a topic into an Iceberg table: Flink, the Iceberg Kafka Connect sink, Spark Structured Streaming, RisingWave or Amazon Data Firehose. Some run as one binary, some as a cluster. The table is a second copy, as fresh as the last commit. Mink serves the topic itself as the table.

<div class="mink-diagram">
<svg viewBox="0 0 690 310" width="690" role="img" aria-label="A connector reads Kafka and writes Parquet and metadata to a bucket, which engines read. Mink reads Kafka and serves engines directly, writing only metadata to its bucket.">
  <defs>
    <marker id="connector-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="20" y="24" class="sub">connector</text>
  <rect x="20" y="36" width="140" height="50" class="box"/>
  <text x="90" y="57" text-anchor="middle" class="label">Kafka</text>
  <text x="90" y="75" text-anchor="middle" class="sub">records</text>
  <rect x="190" y="36" width="140" height="50" class="box"/>
  <text x="260" y="57" text-anchor="middle" class="label">connector</text>
  <text x="260" y="75" text-anchor="middle" class="sub">periodic commits</text>
  <rect x="360" y="36" width="140" height="50" class="box"/>
  <text x="430" y="57" text-anchor="middle" class="label">bucket</text>
  <text x="430" y="75" text-anchor="middle" class="sub">Parquet, metadata</text>
  <rect x="530" y="36" width="140" height="50" class="box"/>
  <text x="600" y="57" text-anchor="middle" class="label">engine</text>
  <text x="600" y="75" text-anchor="middle" class="sub">reads the copy</text>
  <path d="M160 61 L186 61" class="edge" marker-end="url(#connector-arrow)"/>
  <path d="M330 61 L356 61" class="edge" marker-end="url(#connector-arrow)"/>
  <path d="M500 61 L526 61" class="edge" marker-end="url(#connector-arrow)"/>
  <text x="20" y="138" class="sub">Mink</text>
  <rect x="20" y="150" width="140" height="50" class="box"/>
  <text x="90" y="171" text-anchor="middle" class="label">Kafka</text>
  <text x="90" y="189" text-anchor="middle" class="sub">the only copy</text>
  <rect x="190" y="150" width="140" height="50" class="box-accent"/>
  <text x="260" y="171" text-anchor="middle" class="label">Mink</text>
  <text x="260" y="189" text-anchor="middle" class="sub">builds on read</text>
  <rect x="530" y="150" width="140" height="50" class="box"/>
  <text x="600" y="171" text-anchor="middle" class="label">engine</text>
  <text x="600" y="189" text-anchor="middle" class="sub">reads up to the end</text>
  <rect x="190" y="244" width="140" height="50" class="box"/>
  <text x="260" y="265" text-anchor="middle" class="label">bucket</text>
  <text x="260" y="283" text-anchor="middle" class="sub">metadata only</text>
  <path d="M160 175 L186 175" class="edge" marker-end="url(#connector-arrow)"/>
  <path d="M330 175 L526 175" class="edge" marker-end="url(#connector-arrow)"/>
  <path d="M260 200 L260 240" class="edge" marker-end="url(#connector-arrow)"/>
</svg>
</div>

## Freshness

A connector's table changes only when it commits. Records written since the last commit are not in it.

| Connector | Commits | Default |
| --- | --- | --- |
| Flink | After each checkpoint | Checkpointing is off by default, and without it nothing commits |
| Iceberg Kafka Connect sink | Every `iceberg.control.commit.interval-ms` | 5 minutes |
| Spark Structured Streaming | Every trigger | Iceberg recommends at least 1 minute |
| RisingWave | Every `commit_checkpoint_interval` checkpoints | 60, about 60 seconds |
| Amazon Data Firehose | When its buffer fills or its interval ends | 300 seconds or 5 MiB |

A Mink table load covers every record committed in Kafka at that moment. Snapshots set nothing but how much of a load is tail, see [Snapshots](/docs/design/snapshots).

## Upkeep

Frequent commits write small files and many snapshots. A connector's table needs maintenance jobs.

| Task | Connector | Mink |
| --- | --- | --- |
| Small files | `rewrite_data_files` | Assembly joins an ended time bucket's files, without reading Kafka |
| Old snapshots | `expire_snapshots` | Expired after `snapshot_retention` |
| Unused files | `remove_orphan_files` | Collected after `collection_grace` |
| Manifests | `rewrite_manifests` | Grouped by time bucket, one per bucket once assembled |

Expiring snapshots or deleting orphan files under a running Flink job can break its state. Mink's writer runs all of these itself, see [Retention](/docs/design/retention).

## Differences

| | Connector | Mink |
| --- | --- | --- |
| Copies | Kafka, and Parquet in the bucket | Kafka only. The bucket holds metadata. |
| Freshness | The last commit | Every record committed when the table loads |
| Moving parts | A Flink, Connect, Spark or RisingWave deployment, a catalog, maintenance jobs | Mink nodes and Postgres |
| Schema changes | Off by default in the Connect sink. Firehose drops fields the table lacks. | New Schema Registry versions evolve the table |
| History | Kept in the table after Kafka deletes it | As far back as Kafka's retention |
| Transforms | Any, in the job | None. One row per record. |

## When a connector fits

- Tables kept longer than the topic's retention.
- Records transformed, joined or filtered before they land.
- Engines that read only from cloud storage.

## Sources

- [Iceberg Flink writes](https://iceberg.apache.org/docs/latest/flink-writes/)
- [Iceberg Kafka Connect sink](https://iceberg.apache.org/docs/latest/kafka-connect/)
- [Iceberg Spark Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/)
- [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [RisingWave Iceberg sink](https://docs.risingwave.com/iceberg/write-to-iceberg)
- [Firehose buffering hints](https://docs.aws.amazon.com/firehose/latest/APIReference/API_BufferingHints.html)
- [Firehose Iceberg considerations](https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-considerations.html)
