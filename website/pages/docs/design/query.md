# SQL

`mink-query` is the SQL engine. It is a separate process built on DataFusion with the cluster registered as a catalog. A statement is planned into one read per bucket, the reads run in parallel, and the result streams out as Arrow. Nothing is cached between statements and nothing is written.

<div class="mink-diagram">
<svg viewBox="0 0 680 230" width="680" role="img" aria-label="A SQL client sends a statement to mink-query. DataFusion plans it against the catalog and the Iceberg snapshot, then MinkScan streams log tails from the nodes over Flight and Iceberg files from object storage.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="10" y="90" width="150" height="50" class="box"/>
  <text x="85" y="111" text-anchor="middle" class="label">SQL client</text>
  <text x="85" y="128" text-anchor="middle" class="sub">Flight SQL, shell</text>
  <rect x="210" y="20" width="200" height="190" class="box"/>
  <text x="310" y="44" text-anchor="middle" class="label">mink-query</text>
  <rect x="230" y="60" width="160" height="44" class="box-accent"/>
  <text x="310" y="79" text-anchor="middle" class="label">DataFusion plan</text>
  <text x="310" y="95" text-anchor="middle" class="sub">catalog, pushdown, splits</text>
  <rect x="230" y="120" width="160" height="44" class="box-accent"/>
  <text x="310" y="139" text-anchor="middle" class="label">MinkScan</text>
  <text x="310" y="155" text-anchor="middle" class="sub">one stream per split</text>
  <text x="310" y="192" text-anchor="middle" class="sub">merge, filter, aggregate</text>
  <path d="M310 104 L310 112" class="edge" marker-end="url(#arr)"/>
  <rect x="480" y="20" width="180" height="50" class="box"/>
  <text x="570" y="41" text-anchor="middle" class="label">Iceberg catalog</text>
  <text x="570" y="58" text-anchor="middle" class="sub">snapshot, file list</text>
  <rect x="480" y="90" width="180" height="50" class="box"/>
  <text x="570" y="111" text-anchor="middle" class="label">nodes :9123</text>
  <text x="570" y="128" text-anchor="middle" class="sub">offsets, log tail</text>
  <rect x="480" y="160" width="180" height="50" class="box"/>
  <text x="570" y="181" text-anchor="middle" class="label">object storage</text>
  <text x="570" y="198" text-anchor="middle" class="sub">data, delete files</text>
  <path d="M160 115 L202 115" class="edge" marker-end="url(#arr)"/>
  <path d="M390 82 L472 45" class="edge-soft" marker-end="url(#arr)"/>
  <path d="M390 82 L472 115" class="edge-soft" marker-end="url(#arr)"/>
  <path d="M390 142 L472 115" class="edge" marker-end="url(#arr)"/>
  <path d="M390 142 L472 185" class="edge" marker-end="url(#arr)"/>
</svg>
</div>

## Catalog

| DataFusion | Mink |
| --- | --- |
| Catalog `mink` | The cluster |
| Schema | Database |
| Table | Table, with its Arrow schema from the descriptor |

The catalog is refreshed before every statement with `list_databases` and `list_tables`, so a table created a moment ago is visible. A table referenced by a statement is opened on first use: descriptor, partition list, and a lookup client when it has a primary key. `information_schema` is on. Unqualified names resolve in the database given by `-d`, or `default`.

## Pushdown

DataFusion hands the table provider a projection, a limit and a list of filters. Each filter is classified:

| Filter | Classification | Effect |
| --- | --- | --- |
| Pins only partition keys | Exact | Prunes partitions. Not re-applied |
| Pins a partition, bucket or primary-key column | Inexact | Routes to buckets or keys. Re-applied above the scan |
| Converts to a lake predicate | Inexact | Skips Iceberg files and row groups. Re-applied above the scan |
| Anything else | Unsupported | Evaluated by DataFusion above the scan |

A filter **pins** a column when it is `column = literal`, `column IN (literals)`, an `OR` of those on one column, or an `AND` of pins. Pins on the same column across filters intersect. A **lake predicate** is a comparison of a column with a literal, `IN`, `IS NULL`, `IS NOT NULL`, `NOT`, `AND` or `OR`, over the types the Iceberg scan can evaluate. A widening cast that DataFusion inserts around a column folds into the literal. A narrowing one does not.

Projection is pushed to every split as field indexes: the node projects log batches before sending and the Iceberg reader projects Parquet columns. The limit stops each split's stream at `limit` rows and DataFusion applies the global limit above.

## Splits

One statement over one table becomes a set of splits. The shape of the table and what the filters pin decide the kind:

<div class="mink-diagram">
<svg viewBox="0 0 680 250" width="680" role="img" aria-label="Decision tree for split kinds. A pinned full primary key gives one Lookup. Without a lake snapshot a primary-key table gives one Snapshot per bucket and a log table one Union per bucket over the log. With a lake snapshot a table with bucket keys gives one Union per bucket over lake files and tail, and a table without bucket keys gives one Lake per partition plus one Union tail per bucket.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="250" y="20" width="180" height="40" class="box"/>
  <text x="340" y="45" text-anchor="middle" class="label">primary key pinned</text>
  <rect x="500" y="20" width="160" height="40" class="box-accent"/>
  <text x="580" y="45" text-anchor="middle" class="label">Lookup, one split</text>
  <path d="M430 40 L492 40" class="edge" marker-end="url(#arr)"/>
  <text x="461" y="33" text-anchor="middle" class="sub">yes</text>
  <path d="M340 60 L340 82" class="edge" marker-end="url(#arr)"/>
  <text x="348" y="75" class="sub">no</text>
  <rect x="250" y="90" width="180" height="40" class="box"/>
  <text x="340" y="115" text-anchor="middle" class="label">lake snapshot</text>
  <rect x="20" y="90" width="160" height="40" class="box"/>
  <text x="100" y="115" text-anchor="middle" class="label">primary key</text>
  <rect x="500" y="90" width="160" height="40" class="box"/>
  <text x="580" y="115" text-anchor="middle" class="label">bucket keys</text>
  <path d="M250 110 L188 110" class="edge" marker-end="url(#arr)"/>
  <text x="219" y="103" text-anchor="middle" class="sub">no</text>
  <path d="M430 110 L492 110" class="edge" marker-end="url(#arr)"/>
  <text x="461" y="103" text-anchor="middle" class="sub">yes</text>
  <rect x="0" y="180" width="165" height="56" class="box-accent"/>
  <text x="82" y="203" text-anchor="middle" class="label">Snapshot</text>
  <text x="82" y="221" text-anchor="middle" class="sub">one per bucket</text>
  <rect x="170" y="180" width="165" height="56" class="box-accent"/>
  <text x="252" y="203" text-anchor="middle" class="label">Union per bucket</text>
  <text x="252" y="221" text-anchor="middle" class="sub">log only</text>
  <rect x="340" y="180" width="170" height="56" class="box-accent"/>
  <text x="425" y="203" text-anchor="middle" class="label">Lake per partition</text>
  <text x="425" y="221" text-anchor="middle" class="sub">+ tail per bucket</text>
  <rect x="515" y="180" width="165" height="56" class="box-accent"/>
  <text x="597" y="203" text-anchor="middle" class="label">Union per bucket</text>
  <text x="597" y="221" text-anchor="middle" class="sub">lake + tail</text>
  <path d="M100 130 L82 172" class="edge" marker-end="url(#arr)"/>
  <text x="78" y="155" text-anchor="end" class="sub">yes</text>
  <path d="M100 130 L250 172" class="edge" marker-end="url(#arr)"/>
  <text x="186" y="150" text-anchor="middle" class="sub">no</text>
  <path d="M580 130 L596 172" class="edge" marker-end="url(#arr)"/>
  <text x="602" y="155" class="sub">yes</text>
  <path d="M580 130 L428 172" class="edge" marker-end="url(#arr)"/>
  <text x="494" y="150" text-anchor="middle" class="sub">no</text>
</svg>
</div>

| Split | Reads | From |
| --- | --- | --- |
| `Lookup` | The pinned keys, at most 1024 combinations, one `lookup` action | Bucket leaders |
| `Snapshot` | The KV store of one bucket | Bucket leader |
| `Union` | Lake files for one bucket at the pinned snapshot, then the log tail from `log_end_offset` to the high watermark, merged as in [Union read](/docs/design/union-read) | Object storage and the bucket leader |
| `Lake` | Lake files for one partition, no bucket. Used when rows are not bucketed by key so files cannot be assigned to a bucket | Object storage |

Which buckets appear:

| Pinned | Buckets |
| --- | --- |
| Every partition key and every bucket key, at most 1024 combinations | Only the buckets the rows route to |
| Some partition keys | Every bucket of the matching partitions |
| Nothing routable | Every bucket |

The lake snapshot is pinned once per statement from `lake_snapshot`: one `snapshot_id` and each bucket's `log_end_offset`. Every split reads the same snapshot, so the statement is consistent across buckets. Iceberg planning happens once per table with the lake predicate applied and the resulting file tasks are handed to the splits.

## Execution

`MinkScan` is the physical operator. It declares one DataFusion partition per split, so a scan over a table with 16 buckets runs 16 streams concurrently. `target_partitions` does not change this. It sets the parallelism of the operators above the scan.

Each stream is opened when DataFusion polls it:

- A `Union` split reads Parquet from object storage through the Iceberg catalog in `[lake]` and the log tail from the bucket leader with a `Scan` bounded to `log_end_offset` through the high watermark. The merge runs in `mink-query` on the same code the node uses for its `Union` ticket.
- A `Lake` split reads its files the same way, with no log.
- A `Snapshot` split is one `Snapshot` ticket to the leader.
- A `Lookup` split is one `lookup` action with every key row.

`EXPLAIN` shows the scan as `MinkScan: table=, splits=, files=, keys=, filter=, projection=, limit=`. Row and byte statistics come from the Iceberg file metadata and the log offsets, so DataFusion can choose join sides.

| Setting | Default | Effect |
| --- | --- | --- |
| `batch_size` | 8192 | Rows per Arrow batch through the plan |
| `target_partitions` | Cores | Parallelism above the scan |
| `memory_limit` | None | Fair spill pool for sorts, joins and aggregates |

## Flight SQL

`mink-query serve` exposes Flight SQL on `0.0.0.0:9130`. The statement handle is the SQL text, so any `mink-query` replica behind a load balancer can serve any ticket.

| Command | Result |
| --- | --- |
| `CommandStatementQuery` | Plans once for the schema, then plans and executes on `DoGet` |
| `CreatePreparedStatement`, `CommandPreparedStatementQuery` | Same, with the SQL as the handle. No parameters |
| `GetCatalogs`, `GetDbSchemas`, `GetTables`, `GetTableTypes` | From the refreshed catalog. One catalog, one table type `TABLE` |
| `GetSqlInfo` | Server name, version, Arrow version, `FlightSqlServerReadOnly = true` |

DDL and DML are not served.

## Configuration

| Flag | Environment | Default |
| --- | --- | --- |
| `-b`, `--bootstrap` | `MINK_BOOTSTRAP` | `grpc://127.0.0.1:9123`, comma separated for several |
| `-c`, `--config` | `MINK_QUERY_CONFIG` | None. TOML file with `[lake]` and engine settings |
| `-d`, `--database` | `MINK_QUERY_DATABASE` | `default` |
| `--listen` on `serve` | `MINK_QUERY_LISTEN` | `0.0.0.0:9130` |

The TOML file carries the same `[lake]` block as a node, so the engine reads the tables the node tiers, plus `batch_size`, `target_partitions` and `memory_limit`. `MINK_QUERY_*` environment variables override the file. Without a `[lake]` block every table is read from the log and KV state alone.

```toml
memory_limit = 4294967296

[lake]
format = "iceberg"
catalog = "rest"
uri = "http://iceberg-rest:8181"
warehouse = "s3://mink-lake"

[lake.properties]
"s3.endpoint" = "http://rustfs:9000"
"s3.region" = "us-east-1"
```

| Command | Purpose |
| --- | --- |
| `serve` | Flight SQL |
| `exec -e <sql>`, `exec -f <file>`, `exec < stdin` | Run statements. `--format table`, `json` or `csv` |
| `shell` | Interactive |
