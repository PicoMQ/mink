# Snapshots

A snapshot is a table at one moment: for each partition, the offsets the table covers. Each snapshot is an Iceberg snapshot of the same id. A writer commits one every `snapshot_interval`, 10 seconds by default, and a table load adds the newest records on top.

<div class="mink-diagram">
<svg viewBox="0 0 660 230" width="660" role="img" aria-label="Each partition's committed snapshot covers offsets up to its end offset. A table load adds a tail from that end offset to each partition's last stable offset at the moment of the load.">
  <text x="550" y="18" text-anchor="middle" class="sub">load, at each last stable offset</text>
  <path d="M550 26 L550 214" class="edge-soft"/>
  <text x="20" y="61" class="sub">partition 0</text>
  <rect x="120" y="40" width="300" height="36" class="box"/>
  <text x="270" y="63" text-anchor="middle" class="label">snapshot N</text>
  <rect x="420" y="40" width="130" height="36" class="box-accent"/>
  <text x="485" y="63" text-anchor="middle" class="sub">tail</text>
  <text x="20" y="117" class="sub">partition 1</text>
  <rect x="160" y="96" width="220" height="36" class="box"/>
  <text x="270" y="119" text-anchor="middle" class="label">snapshot N</text>
  <rect x="380" y="96" width="170" height="36" class="box-accent"/>
  <text x="465" y="119" text-anchor="middle" class="sub">tail</text>
  <text x="20" y="173" class="sub">partition 2</text>
  <rect x="140" y="152" width="330" height="36" class="box"/>
  <text x="305" y="175" text-anchor="middle" class="label">snapshot N</text>
  <rect x="470" y="152" width="80" height="36" class="box-accent"/>
  <text x="510" y="175" text-anchor="middle" class="sub">tail</text>
</svg>
</div>

## Commits

Every `snapshot_interval` the table's writer reads the indexed ranges from Postgres and commits the next snapshot when anything changed. It writes the Iceberg metadata to the bucket first, then commits the snapshot in Postgres. It never reads Kafka records.

| Part | Holds |
| --- | --- |
| Snapshot id | `1`, `2`, `3`, … in commit order. The Iceberg snapshot has the same id. |
| Offsets | For each partition, `[start_offset, end_offset)` |
| Files | The data files inside those offsets |
| Summary | `mink.partition.{p}.start-offset` and `mink.partition.{p}.end-offset` for each partition `p` |

## Loads

A table load returns the latest snapshot. When a partition's last stable offset is past the snapshot's end, the load adds a tail: the offsets in between, as data files built in memory to take their statistics, like an indexer's. The tail is never committed. Two loads that see the same offsets share one tail.

A load covers every record committed in Kafka before it read the last stable offsets.

## Committed records only

Mink reads Kafka as a `read_committed` consumer would.

| Record | In the table |
| --- | --- |
| Committed, or written without a transaction | Yes |
| In an aborted transaction | No |
| In an open transaction | Not until it commits |
| Control record, such as a transaction marker | No |

## Time travel

Engines read older snapshots by id or by time. A snapshot expires after `snapshot_retention`, 24 hours by default, or once Kafka's retention deleted records it covers. The latest snapshot never expires. See [Retention](/docs/design/retention).
