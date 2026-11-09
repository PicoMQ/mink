# CLI

`mink` runs a node and manages the set of tables. Table commands talk to Postgres and Kafka directly, so they need no running node.

| Command | Does |
| --- | --- |
| `mink serve [-c <file>] [--roles <roles>]` | Runs a node |
| `mink table add <topic> [-c <file>] [--granularity <g>]` | Makes a topic a table |
| `mink table remove <topic> [-c <file>]` | Removes a table. Its objects and rows are deleted after `lease_ttl`. |
| `mink table list [-c <file>]` | Lists the tables and their states |
| `mink table attach new\|existing <topic> [-c <file>] <options>` | Attaches an external table. See [Attach](#attach). |
| `mink table detach <topic> [-c <file>]` | Detaches the external table. See [Tiering](/docs/design/tiering#attach-and-detach). |
| `mink table schema <topic> [-c <file>]` | Prints the table's Iceberg schema as JSON, field ids included |
| `mink granularity <topic> <g> [-c <file>]` | Changes a table's time partitioning for new files |

`-c` names a TOML config file. `MINK_*` environment variables override it, and `--roles` overrides both. See [Configuration](/docs/operations/configuration).

`<g>` is `hour`, `day`, `month` or `none`. Without `--granularity`, a table gets the `granularity` setting, `hour` by default.

Every command but `serve` and `table schema` has an admin API route that does the same. See [Admin API](/docs/operations/admin).

```bash
mink serve -c mink.toml --roles catalog,s3
mink table add orders -c mink.toml --granularity day
mink table list -c mink.toml
```

## Attach

`mink table attach new` creates an Iceberg table in an external catalog and attaches it. `mink table attach existing` attaches one that exists and meets the rules in [Tiering](/docs/design/tiering#attach-and-detach). The table's older records then tier into it.

| Option | Of | Default | Purpose |
| --- | --- | --- | --- |
| `--catalog <c>` | both | | An entry of `external_catalogs` |
| `--namespace <n>`, `--table <t>` | both | | The Iceberg table |
| `--tier-after <d>` | both | `1h` | Only records older than this tier |
| `--tier-interval <d>` | both | `1h` | How often tiered files are committed |
| `--partition <term>` | `new` | | A partition term, repeatable: `day(_kafka_timestamp)`, `bucket(16, _kafka_key)`, `truncate(4, region)`, or a column for identity |
| `--unpartitioned` | `new` | | A table without partition terms. `new` needs this or `--partition`. |
| `--sort <term>` | `new` | none | A sort term, repeatable: a partition term, then `asc` or `desc`, then `nulls first` or `nulls last` |
| `--property <k=v>` | `new` | none | A table property, repeatable |
| `--location <s3://…>` | `new` | the catalog's | Where the table keeps its files |
| `--end-offset <p=o>` | `existing` | the table's properties | The table holds every record of partition `p` before offset `o`, repeatable |

```bash
mink table attach new orders -c mink.toml --catalog tables --namespace kafka --table orders \
  --partition "day(_kafka_timestamp)" --sort "_kafka_key"
mink table detach orders -c mink.toml
mink table attach existing orders -c mink.toml --catalog tables --namespace kafka --table orders
```
