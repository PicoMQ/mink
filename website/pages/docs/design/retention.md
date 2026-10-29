# Retention

Nothing Mink keeps grows without bound. Snapshots expire, and the objects and rows only expired snapshots used are collected. Kafka's own retention decides how far back a table reaches.

## Snapshot expiry

| A snapshot expires when | Setting | Default |
| --- | --- | --- |
| It is older than | `snapshot_retention` | `24h` |
| Kafka's retention deleted records it covers | Kafka's `retention.ms`, `retention.bytes` | |

The latest snapshot never expires. Snapshots expire oldest first, as part of a commit, so the table keeps a contiguous run of snapshots ending at the latest.

## Collection

Every `collection_interval`, 10 minutes by default, the writer deletes:

| Object | Deleted once |
| --- | --- |
| Metadata objects only expired snapshots used | Unreferenced for `collection_grace`, 1 hour by default |
| Objects of a commit that failed | Older than `collection_grace` |
| Objects of load tails | Older than `collection_grace` |
| Postgres rows of expired snapshots and of files no snapshot lists | With the snapshot |

The grace period lets queries that started on a snapshot just before it expired finish reading. Deleting is safe to repeat, so a collection that stops part way finishes on the next run.

## Kafka retention

| Kafka deletes | Mink |
| --- | --- |
| Records at the start of a partition | Moves the indexed range's start to the next file boundary. At most one file per partition leaves the table early. |
| Records a snapshot covers | Expires that snapshot |
| The whole topic | Marks the table `missing` |

Mink never deletes anything from Kafka.
