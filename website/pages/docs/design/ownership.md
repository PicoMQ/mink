# Ownership and failover

Every bucket has exactly one leader node at a time. The leader is recorded in the metadata log with a leader epoch, the node opens the bucket's stream under that epoch, and s3stream rejects writes from any other epoch. Ownership is therefore enforced by the storage layer, not by the nodes agreeing among themselves.

## Assignment

| Event | Action |
| --- | --- |
| Table or partition created | The coordinator assigns bucket `i` to live node `(i + start) % n`, `start` derived from the clock so consecutive tables begin on different nodes, and proposes `LeadBucket` for each |
| Node stops heartbeating | On the next tick the coordinator finds buckets whose leader is not live and proposes `LeadBucket` to the least-loaded live node, lowest id on ties |
| Node joins | Nothing moves by itself. New tables spread over the new node |
| `mink cluster rebalance` | Orphans first, then buckets move from nodes above 110% of the mean load to nodes below 90% of it, one `LeadBucket` per move |

`LeadBucket` carries the coordinator's epoch, so a stale coordinator that lost the lease cannot reassign. It returns the new leader epoch for the bucket.

## Opening a bucket

Each node tails the metadata view. When a bucket it should lead appears, or a bucket it leads disappears, the node's sync task opens or closes it.

<div class="mink-diagram">
<svg viewBox="0 0 680 250" width="680" role="img" aria-label="Open decision: if the stream is opened on a live node, the bucket is held; if on a dead node, take over; if closed at the same epoch, start a new term; then open the stream at the leader epoch.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="10" y="97" width="150" height="56" class="box-accent"/>
  <text x="85" y="121" text-anchor="middle" class="label">view says</text>
  <text x="85" y="139" text-anchor="middle" class="sub">lead this bucket</text>
  <rect x="210" y="20" width="200" height="50" class="box"/>
  <text x="310" y="41" text-anchor="middle" class="label">opened on live node</text>
  <text x="310" y="58" text-anchor="middle" class="sub">HeldBy, retry later</text>
  <rect x="210" y="100" width="200" height="50" class="box"/>
  <text x="310" y="121" text-anchor="middle" class="label">opened on dead node</text>
  <text x="310" y="138" text-anchor="middle" class="sub">take over</text>
  <rect x="210" y="180" width="200" height="50" class="box"/>
  <text x="310" y="201" text-anchor="middle" class="label">closed, same epoch</text>
  <text x="310" y="218" text-anchor="middle" class="sub">LeadBucket, new term</text>
  <rect x="470" y="97" width="190" height="56" class="box-accent"/>
  <text x="565" y="121" text-anchor="middle" class="label">open stream</text>
  <text x="565" y="139" text-anchor="middle" class="sub">at leader epoch</text>
  <path d="M160 115 L202 50" class="edge" marker-end="url(#arr)"/>
  <path d="M160 125 L202 125" class="edge" marker-end="url(#arr)"/>
  <path d="M160 135 L202 200" class="edge" marker-end="url(#arr)"/>
  <path d="M410 130 L462 125" class="edge" marker-end="url(#arr)"/>
  <path d="M410 200 L462 135" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

| Stream state in the view | Action |
| --- | --- |
| Opened by another node that is live | `HeldBy`. The other node has not yet seen the change. Retry on the next sync |
| Opened by a node that is dead | Take over: register the dead node at `epoch + 1`, run s3stream failover to replay and seal its WAL, close its streams. Then open |
| Closed at the current leader epoch | This node restarted. Propose `LeadBucket` to get a fresh epoch, then open |
| Closed at an older epoch | Open at the leader epoch from the view |

Opening the stream at the leader epoch is the fence. The metadata log records the epoch with `OpenStream`. An append or WAL upload tagged with a lower epoch is refused. A primary-key bucket also restores its KV tablet from the latest snapshot and replays the changelog before it is served. Until open completes, requests get `Unavailable`.

## Failover

<div class="mink-diagram">
<svg viewBox="0 0 680 120" width="680" role="img" aria-label="Failover timeline: heartbeats stop, the lease TTL passes, the coordinator re-leads, the new node takes over the WAL and opens, then serves.">
  <path d="M20 60 L660 60" class="edge"/>
  <path d="M60 50 L60 70" class="edge"/>
  <text x="60" y="40" text-anchor="middle" class="sub">node 2 dies</text>
  <path d="M220 50 L220 70" class="edge"/>
  <text x="220" y="40" text-anchor="middle" class="sub">heartbeat TTL passes</text>
  <path d="M380 50 L380 70" class="edge"/>
  <text x="380" y="40" text-anchor="middle" class="sub">LeadBucket to node 1</text>
  <path d="M540 50 L540 70" class="edge"/>
  <text x="540" y="40" text-anchor="middle" class="sub">WAL replayed, opened</text>
  <text x="140" y="90" text-anchor="middle" class="sub">redirects fail</text>
  <text x="300" y="90" text-anchor="middle" class="sub">next tick</text>
  <text x="460" y="90" text-anchor="middle" class="sub">Unavailable</text>
  <text x="610" y="90" text-anchor="middle" class="sub">serving</text>
</svg>
</div>

1. **Detection.** Heartbeats stop. After `lease_ttl` (30 seconds) the node is no longer in `live_nodes()`.
2. **Reassignment.** On the next coordinator tick (`coordinator_tick`, 5 seconds) every bucket the dead node led is re-led to a live node.
3. **Takeover.** The new leader sees the stream opened by a dead node, bumps that node's epoch, and runs s3stream failover: the dead node's WAL objects are replayed into stream objects and the WAL is sealed. Acknowledged writes are recovered because acknowledgement required the WAL upload. In-flight, unacknowledged writes are gone, which is what the missing acknowledgement meant.
4. **Open and serve.** The stream is opened at the new epoch. For primary-key buckets, KV snapshot restore plus changelog replay from the snapshot's `log_offset`.

If the dead node comes back, its old epoch is fenced at the stream, its `RegisterNode` gets a fresh epoch, and it re-syncs from the view: the buckets now belong to someone else and it does not open them.

## Coordinator failover

The coordinator, tiering worker and lifecycle loops run only on the lease holder. When the lease expires another node acquires it, registers as coordinator with a new coordinator epoch, and rebuilds its in-memory schedule from the view. A tiering round in progress on the old holder is fenced: its heartbeats carry the old epoch and are rejected, and the round is retried.

## Redirects

A request for a bucket the node does not lead is answered from the node's view:

| Case | Response |
| --- | --- |
| Another node leads it | `NotLeader` with a `Redirect { bucket, to }` carrying the leader's advertised address. The client updates its route and retries there |
| This node leads it but has not finished opening | `Unavailable`. The client retries |
| Table-addressed write (`AppendTable`, `PutTable`) | No redirect. The node splits the batch and forwards each bucket's part to its leader |
| Coordinator action on a non-leader node | Forwarded to the lease holder |

The Rust client caches leaders per bucket and refreshes on `NotLeader`. Kafka clients get the same information through `Metadata` responses, which map each partition's leader to the node advertising the Kafka listener.
