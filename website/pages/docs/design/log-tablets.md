# Log tablets

A log tablet is one bucket's log. It assigns offsets, deduplicates idempotent writers, appends batches to the bucket's s3stream stream, serves range reads bounded by the high watermark, and resolves timestamps to offsets. Every bucket of every table has one, and for a primary-key table it holds the changelog.

## Record batches

The log is a sequence of record batches. A batch is a 52-byte header followed by an optional change-type vector and an Arrow IPC message holding the rows.

| Field | Size | Meaning |
| --- | --- | --- |
| `base_offset` | 8 | Offset of the first row, assigned by the tablet |
| `length` | 4 | Bytes after this field |
| `magic` | 1 | Format version, `1` |
| `commit_timestamp` | 8 | Assigned by the tablet on append, milliseconds |
| `leader_epoch` | 4 | Bucket leader epoch |
| `crc` | 4 | CRC32C over `schema_id` to the end |
| `schema_id` | 2 | Schema version the rows were encoded with |
| `attributes` | 1 | Bit 0: append-only, no change-type vector |
| `last_offset_delta` | 4 | `record_count - 1` |
| `writer_id`, `batch_sequence` | 8 + 4 | Idempotent writer identity, `-1` when unused |
| `record_count` | 4 | Rows in the batch |

`base_offset`, `commit_timestamp` and `leader_epoch` sit outside the CRC so the tablet can fill them in without re-hashing the client's bytes. Change types are `+A` append-only, `+I` insert, `-U` update-before, `+U` update-after, `-D` delete.

## Offsets

<div class="mink-diagram">
<svg viewBox="0 0 640 150" width="640" role="img" aria-label="A bucket log from log_start to log_end. Batches up to the high watermark are durable and readable. Batches between the high watermark and log_end are in flight.">
  <rect x="20" y="50" width="140" height="44" class="box"/>
  <text x="90" y="77" text-anchor="middle" class="sub">trimmed</text>
  <rect x="160" y="50" width="300" height="44" class="box-accent"/>
  <text x="310" y="77" text-anchor="middle" class="label">durable, readable</text>
  <rect x="460" y="50" width="160" height="44" class="box"/>
  <text x="540" y="77" text-anchor="middle" class="sub">in flight</text>
  <path d="M160 40 L160 104" class="edge"/>
  <path d="M460 40 L460 104" class="edge"/>
  <path d="M620 40 L620 104" class="edge"/>
  <text x="160" y="30" text-anchor="middle" class="sub">log_start</text>
  <text x="460" y="30" text-anchor="middle" class="sub">high_watermark</text>
  <text x="620" y="30" text-anchor="end" class="sub">log_end</text>
  <text x="160" y="124" text-anchor="middle" class="sub">retention trims here</text>
  <text x="460" y="124" text-anchor="middle" class="sub">reads stop here</text>
</svg>
</div>

| Offset | Source | Meaning |
| --- | --- | --- |
| `log_start` | stream start offset | Oldest retained row. Moves forward on trim |
| `high_watermark` | stream confirm offset | Highest offset whose WAL upload completed. The read bound |
| `log_end` | stream next offset | Next offset to assign. Rows between the watermark and here are not yet durable |

Readers pick an isolation of `HighWatermark` (default) or `LogEnd`. The KV tablet uses `LogEnd` during recovery to rebuild its unflushed prewrite buffer. Clients only ever see the watermark.

## Append

1. Parse each batch header, check the record count and CRC.
2. For a batch with a writer id, check the sequence against the writer's window. A duplicate returns the offsets it got the first time and nothing is appended.
3. Assign `base_offset` and `commit_timestamp`, write the leader epoch, submit the batch to the stream.
4. Await the stream's durable acknowledgement, then advance the high watermark watch so tailing readers wake up.

The stream is an s3stream stream. Its WAL is a sequence of small objects on the WAL bucket, one `PUT` per group commit, acknowledged in submission order. A background task drains sealed WAL data into read-optimized stream-set and stream objects on the data bucket and commits them through the metadata log. WAL objects that are fully covered are deleted. A single append on an idle bucket pays one object-store round trip.

## Idempotent writers

A writer id comes from the `init_writer` Flight action or the Kafka `InitProducerId` request. Both draw from the same allocator in the metadata log. The tablet keeps the last five batches per writer: sequence, base offset, offset delta and timestamp.

| Case | Result |
| --- | --- |
| Sequence is `last + 1`, or wraps from `i32::MAX` to `0` | Appended |
| Sequence matches one of the retained five | Duplicate, prior offsets returned |
| Sequence is out of order otherwise | Rejected |
| Writer unknown or idle past the writer TTL, default 7 days | Any sequence accepted, window restarts |

The writer window is snapshotted to the metadata KV under `mink/writers/{table}/{partition}/{bucket}` together with the offset it was taken at. On open the tablet loads the snapshot, replays the stream from that offset to rebuild the window, and is then exact from the first request.

## Read

A read asks for `[from, to)` on one bucket with an optional column projection. The tablet checks the range against `log_start` and the isolation bound, fetches from the stream, and rewrites each batch to the projected columns when a projection is given. The fetch comes from the s3stream log cache for the tail, the block cache for recent history, and the object store for the rest.

`wait_past(offset, timeout)` subscribes to the high-watermark watch and returns when the watermark passes the offset or the timeout expires. Tail reads and Kafka fetches with `max_wait` are built on it.

## Timestamp lookup

`offset_for_timestamp(ts)` returns the first offset whose batch has `commit_timestamp >= ts`. Timestamps in the future are rejected. A timestamp after the newest batch returns the high watermark. The search is a binary search over batch headers, each probe a fetch of one batch, so it costs `log2(batches)` reads and no index.

## Trim and lifecycle

| Operation | Effect |
| --- | --- |
| `trim(offset)` | Stream start moves to `offset`. Objects fully below it become garbage. Driven by [Retention](/docs/design/retention) |
| `close` | Writer window snapshotted, stream closed. Another node can open it |
| `destroy` | Stream deleted, writer snapshot deleted. Table or partition drop |
