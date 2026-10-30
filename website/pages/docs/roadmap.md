# Roadmap

Planned: Mink writes ended time buckets to Iceberg and serves one catalog over that history and Kafka.

## History in Iceberg

Planned. Not built.

| | Built | Planned |
| --- | --- | --- |
| Table | The records Kafka holds | The Iceberg history and the newer offsets in Kafka, with no gap or overlap between them |
| Data files | Built from Kafka on read, and cached | Ended, assembled time buckets written to the bucket as Parquet. Newer buckets still built from Kafka. |
| File sizes | Assembled up to `assembled_bytes` | Written slowly, after assembly, as large files only. No small files and no compaction jobs. |
| Bucket | Iceberg metadata only | Iceberg metadata, and the history's data files |
| History | As far back as Kafka's retention | Kept after Kafka's retention deletes the records |
| Freshness | Every record committed when the table loads | Unchanged |
| Catalog | Read-only | Read-only. Mink is the only writer. |

<div class="mink-diagram">
<svg viewBox="0 0 680 160" width="680" role="img" aria-label="One table spans two parts. Older offsets are Parquet files in the bucket, written once a time bucket is assembled. Newer offsets up to the last stable offset are built from Kafka on read.">
  <path d="M20 40 L660 40" class="edge-soft"/>
  <path d="M20 34 L20 46" class="edge-soft"/>
  <path d="M660 34 L660 46" class="edge-soft"/>
  <text x="340" y="26" text-anchor="middle" class="sub">one table, one catalog</text>
  <rect x="20" y="60" width="400" height="56" class="box"/>
  <text x="220" y="84" text-anchor="middle" class="label">history</text>
  <text x="220" y="102" text-anchor="middle" class="sub">Parquet in the bucket, large files only</text>
  <rect x="420" y="60" width="240" height="56" class="box-accent"/>
  <text x="540" y="84" text-anchor="middle" class="label">newer offsets</text>
  <text x="540" y="102" text-anchor="middle" class="sub">built from Kafka on read</text>
  <text x="20" y="140" class="sub">oldest</text>
  <text x="420" y="140" text-anchor="middle" class="sub">last written bucket</text>
  <text x="660" y="140" text-anchor="end" class="sub">latest</text>
</svg>
</div>
