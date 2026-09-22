# Metadata

Cluster state is a replicated state machine whose log lives in a SQL database. Nodes propose commands, the sink appends them in order, every node tails the log and applies each command to an in-memory `State`, and the result is published as a `View`. There is no consensus protocol in the nodes. The database's single-writer transaction is the ordering point.

## The command log

<div class="mink-diagram">
<svg viewBox="0 0 680 260" width="680" role="img" aria-label="Nodes propose commands. The sink appends them to meta_log in the SQL database. Each node tails the log, applies commands to its state, and publishes a view. The lease holder writes snapshots and truncates.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="30" width="150" height="56" class="box"/>
  <text x="95" y="54" text-anchor="middle" class="label">node 1</text>
  <text x="95" y="72" text-anchor="middle" class="sub">propose, apply</text>
  <rect x="20" y="170" width="150" height="56" class="box"/>
  <text x="95" y="194" text-anchor="middle" class="label">node 2</text>
  <text x="95" y="212" text-anchor="middle" class="sub">propose, apply</text>
  <rect x="260" y="20" width="170" height="60" class="box-accent"/>
  <text x="345" y="44" text-anchor="middle" class="label">meta_log</text>
  <text x="345" y="62" text-anchor="middle" class="sub">idx, payload</text>
  <rect x="260" y="100" width="170" height="60" class="box"/>
  <text x="345" y="124" text-anchor="middle" class="label">meta_snapshot</text>
  <text x="345" y="142" text-anchor="middle" class="sub">applied_idx, state</text>
  <rect x="260" y="180" width="170" height="60" class="box"/>
  <text x="345" y="204" text-anchor="middle" class="label">meta_lease, node</text>
  <text x="345" y="222" text-anchor="middle" class="sub">leader, heartbeats</text>
  <rect x="510" y="30" width="150" height="56" class="box"/>
  <text x="585" y="54" text-anchor="middle" class="label">View</text>
  <text x="585" y="72" text-anchor="middle" class="sub">index and State</text>
  <rect x="510" y="170" width="150" height="56" class="box"/>
  <text x="585" y="194" text-anchor="middle" class="label">lease holder</text>
  <text x="585" y="212" text-anchor="middle" class="sub">snapshot, truncate</text>
  <path d="M170 50 L252 44" class="edge" marker-end="url(#arr)"/>
  <path d="M170 190 L252 66" class="edge" marker-end="url(#arr)"/>
  <path d="M430 50 L502 56" class="edge" marker-end="url(#arr)"/>
  <path d="M430 130 L502 160" class="edge" marker-end="url(#arr)"/>
  <path d="M510 180 L438 140" class="edge" marker-end="url(#arr)"/>
  <path d="M170 210 L252 210" class="edge-soft" marker-end="url(#arr)"/>
  <path d="M170 76 L252 200" class="edge-soft" marker-end="url(#arr)"/>
</svg>
</div>

| Table | Contents |
| --- | --- |
| `meta_log` | `(idx, payload)`, append-only, one encoded command per row |
| `meta_snapshot` | `(id, applied_idx, payload)`, a full `State` at an index |
| `meta_lease` | One row. Whoever holds it runs the coordinator, the tiering worker and the lifecycle loops |
| `meta_node` | One row per node with its last heartbeat |

The same schema is created on Postgres and on SQLite. SQLite, including `sqlite::memory:`, is for a single node. Postgres is for a cluster.

## Proposals and views

- A **proposal** is a `Command`. The sink batches concurrent proposals from the local node into one transaction, tails the log to learn its own index, applies the command and resolves the proposer with the applied index.
- A **View** is `(applied_index, State)`. Readers take a snapshot of the current view or `wait_applied(index)` for a specific index.
- Every Flight response carries `mink-applied-index`, the index the serving node had applied. A client sends it back as `mink-min-applied-index` on the next request, and the receiving node waits until its own view has caught up. This gives read-your-writes across nodes without pinning a client to one node.

## Commands

| Group | Commands |
| --- | --- |
| s3stream | `RegisterNode`, `CreateStream(s)`, `OpenStream`, `CloseStream`, `TrimStream`, `DeleteStream`, `PlaceStream`, `TransferStream`, `CompleteTransfer`, `PrepareObject`, `CommitStreamSetObject`, `CompactStreamObject`, `ExpirePreparedObjects`, `CleanDestroyedObjects`, `AllocateProducerIds` |
| KV | `PutKv`, `PutKvIfAbsent`, `DeleteKv`, `DeleteKvIfMatches` |
| Catalog | `CreateDatabase`, `DropDatabase`, `CreateTable`, `DropTable`, `AlterTable`, `CreatePartition`, `DropPartition`, `LeadBucket`, `CommitKvSnapshot`, `DropKvSnapshot`, `CommitLakeSnapshot`, `Allocate`, `RegisterCoordinator` |
| Offsets | `RegisterProducerOffsets`, `DeleteProducerOffsets`, `ExpireProducerOffsets`, `CommitGroupOffsets`, `DeleteGroupOffsets`, `ExpireGroupOffsets` |

The s3stream group is the stream engine's own metadata: which node has a stream open and at which epoch, which objects hold which stream ranges, which objects are prepared but not yet committed. The catalog group is Mink's. Both apply through the same state machine, so a table create and the creation of its streams are one ordered sequence. `Allocate` hands out ids and auto-increment ranges.

## Snapshots and truncation

The lease holder encodes the whole `State` into `meta_snapshot` periodically and deletes `meta_log` rows at or below the snapshot index. A starting node loads the latest snapshot and replays the log from there. Snapshot encoding is versioned with a leading version byte.

## Lease and heartbeats

| | Detail |
| --- | --- |
| Lease | One row updated with compare-and-set. Acquired or renewed every `lease_ttl / 4`. Lost when a renew fails. Leadership is published as a watch that the coordinator, tiering worker and lifecycle loops follow |
| Heartbeat | Each node updates its `meta_node` row every `lease_ttl / 4`. A node whose heartbeat is older than the TTL is dead for assignment and failover |
| Shutdown | A node releases its lease and expires its own heartbeat row on the way out |

Default `lease_ttl` is 30 seconds. See [Ownership and failover](/docs/design/ownership) for what happens when a node stops heartbeating.
