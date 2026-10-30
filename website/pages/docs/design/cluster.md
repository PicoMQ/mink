# Cluster

Mink nodes share one Postgres and nothing else. Work moves between them through leases in Postgres. Nodes never talk to each other, except to forward a data file read to its owner and an admin request to the node it names.

<div class="mink-diagram">
<svg viewBox="0 0 660 230" width="660" role="img" aria-label="Three nodes each hold some partition leases and writer leases. Each renews its leases and its membership in Postgres.">
  <defs>
    <marker id="cluster-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="20" width="180" height="76" class="box"/>
  <text x="110" y="44" text-anchor="middle" class="label">node 1</text>
  <text x="110" y="62" text-anchor="middle" class="sub">orders/0, orders/3</text>
  <text x="110" y="80" text-anchor="middle" class="sub">writer of orders</text>
  <rect x="240" y="20" width="180" height="76" class="box"/>
  <text x="330" y="44" text-anchor="middle" class="label">node 2</text>
  <text x="330" y="62" text-anchor="middle" class="sub">orders/1, orders/4</text>
  <text x="330" y="80" text-anchor="middle" class="sub">writer of clicks</text>
  <rect x="460" y="20" width="180" height="76" class="box"/>
  <text x="550" y="44" text-anchor="middle" class="label">node 3</text>
  <text x="550" y="62" text-anchor="middle" class="sub">orders/2, clicks/0</text>
  <text x="550" y="80" text-anchor="middle" class="sub">no writer</text>
  <rect x="20" y="150" width="620" height="60" class="box-accent"/>
  <text x="330" y="176" text-anchor="middle" class="label">Postgres</text>
  <text x="330" y="194" text-anchor="middle" class="sub">members with an expiry, leases with an expiry and an epoch</text>
  <path d="M110 96 L110 146" class="edge" marker-end="url(#cluster-arrow)"/>
  <path d="M330 96 L330 146" class="edge" marker-end="url(#cluster-arrow)"/>
  <path d="M550 96 L550 146" class="edge" marker-end="url(#cluster-arrow)"/>
  <text x="118" y="126" class="sub">renew</text>
  <text x="338" y="126" class="sub">renew</text>
  <text x="558" y="126" class="sub">renew</text>
</svg>
</div>

## Members

A node registers in Postgres when it starts and renews its row every `lease_ttl / 3`, 10 seconds by default. A node is live while its row has not expired. A node that stops cleanly releases its leases and leaves at once. A node that crashes drops out when its row expires, after `lease_ttl`, 30 seconds by default.

<div class="mink-diagram">
<svg viewBox="0 0 660 180" width="660" role="img" aria-label="Node 1 holds a partition lease and renews it every lease_ttl / 3, then crashes. The lease expires lease_ttl after the last renewal, and node 2 takes it on its next round.">
  <text x="40" y="28" class="sub">renews every lease_ttl / 3</text>
  <path d="M40 36 L40 44" class="edge"/>
  <path d="M120 36 L120 44" class="edge"/>
  <path d="M200 36 L200 44" class="edge"/>
  <rect x="40" y="44" width="200" height="36" class="box"/>
  <text x="140" y="67" text-anchor="middle" class="label">node 1 holds the lease</text>
  <text x="248" y="67" class="sub">crashes</text>
  <path d="M440 36 L440 140" class="edge-soft"/>
  <text x="448" y="28" class="sub">lease expires</text>
  <text x="448" y="42" class="sub">last renewal + lease_ttl</text>
  <rect x="456" y="96" width="144" height="36" class="box-accent"/>
  <text x="528" y="119" text-anchor="middle" class="label">node 2 takes it</text>
  <path d="M40 150 L600 150" class="edge"/>
  <path d="M40 150 L40 155" class="edge"/>
  <text x="40" y="170" text-anchor="middle" class="sub">0s</text>
  <path d="M120 150 L120 155" class="edge"/>
  <text x="120" y="170" text-anchor="middle" class="sub">10s</text>
  <path d="M200 150 L200 155" class="edge"/>
  <text x="200" y="170" text-anchor="middle" class="sub">20s</text>
  <path d="M280 150 L280 155" class="edge"/>
  <text x="280" y="170" text-anchor="middle" class="sub">30s</text>
  <path d="M360 150 L360 155" class="edge"/>
  <text x="360" y="170" text-anchor="middle" class="sub">40s</text>
  <path d="M440 150 L440 155" class="edge"/>
  <text x="440" y="170" text-anchor="middle" class="sub">50s</text>
  <path d="M520 150 L520 155" class="edge"/>
  <text x="520" y="170" text-anchor="middle" class="sub">60s</text>
  <path d="M600 150 L600 155" class="edge"/>
  <text x="600" y="170" text-anchor="middle" class="sub">70s</text>
</svg>
</div>

## Leases

| Lease | Scope | Holder runs |
| --- | --- | --- |
| Partition lease | One partition of one table | The partition's indexer |
| Writer lease | One table | The table's writer: commits, assembly, statistics, expiry, collection |

Each lease has an epoch that grows every time it changes hands. Postgres rejects every write under an old epoch, so a node that lost a lease cannot change anything.

## Sharing

Every `lease_ttl / 3`, each node with the `indexer` role applies the same rules.

| Rule | Effect |
| --- | --- |
| A table's partition share is its partition count divided by the indexing nodes, rounded up | Partitions spread evenly |
| A node below its share takes free leases, the ones it prefers most first | New and freed partitions get an indexer |
| A node above its share releases one lease of that table per round | A joining node takes work gradually |
| Each table's writer lease goes to the node that prefers it most | One writer per table |
| `max_partitions` caps the partition leases one node holds | Limits one node's indexers |

A node's preference for a lease is a hash of its id and the lease's scope. Every node computes the same preferences.

## Failures

| Failure | Effect |
| --- | --- |
| A node crashes | Its leases expire after `lease_ttl`. Other nodes take them, and indexing resumes from the indexed ranges. |
| A node joins | Every share drops. Other nodes hand back one partition per table per round. |
| Postgres is unreachable | Table loads, and reads of data files not in the owner's cache, answer `503`. Cached files are still served. Indexers and writers stop. Once Postgres answers, every node takes its share again without a restart. |
| Kafka is unreachable | Table loads, builds and indexing fail until Kafka answers. Cached data files are still served. |
