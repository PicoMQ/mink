# CLI

`mink` runs a node and manages the set of tables. Table commands talk to Postgres and Kafka directly, so they need no running node.

| Command | Does |
| --- | --- |
| `mink serve [-c <file>] [--roles <roles>]` | Runs a node |
| `mink table add <topic> [-c <file>] [--granularity <g>]` | Makes a topic a table |
| `mink table remove <topic> [-c <file>]` | Removes a table. Its objects and rows are deleted after `lease_ttl`. |
| `mink table list [-c <file>]` | Lists the tables and their states |
| `mink table attach <topic> [-c <file>] <options>` | Attaches an external table. See [Attach](#attach). |
| `mink table detach <topic> [-c <file>]` | Detaches the external table. See [Tiering](/docs/design/tiering#attach-and-detach). |
| `mink table schema <topic> [-c <file>]` | Prints the table's Iceberg schema as JSON, field ids included |
| `mink granularity <topic> <g> [-c <file>]` | Changes a table's time partitioning for new files |

`-c` names a TOML config file. `MINK_*` environment variables override it, and `--roles` overrides both. See [Configuration](/docs/operations/configuration).

`<g>` is `hour`, `day`, `month` or `none`. Without `--granularity`, a table gets the `granularity` setting, `hour` by default.

`add`, `remove`, `attach` and `detach` have admin API routes that do the same. See [Admin API](/docs/operations/admin).

```bash
mink serve -c mink.toml --roles catalog,s3
mink table add orders -c mink.toml --granularity day
mink table list -c mink.toml
```

## Attach

`mink table attach` creates an Iceberg table in an external catalog, or with `--existing` attaches one that exists. The table's older records then [tier](/docs/design/tiering) into it.

| Option | Default | Purpose |
| --- | --- | --- |
| `--catalog <c>` | | An entry of `external_catalogs` |
| `--namespace <n>`, `--table <t>` | | The Iceberg table |
| `--existing` | off | Attach a table that exists |
| `--partition <term>` | | A partition term, repeatable: `day(_kafka_timestamp)`, `bucket(16, _kafka_key)`, `truncate(4, region)`, or a column for identity |
| `--unpartitioned` | | A new table without partition terms. A new table needs this or `--partition`. |
| `--sort <term>` | none | A sort term, repeatable: a partition term, then `asc` or `desc`, then `nulls first` or `nulls last` |
| `--property <k=v>` | none | A table property, repeatable |
| `--location <s3://…>` | the catalog's | Where a new table keeps its files |
| `--end-offset <p=o>` | the table's properties | For `--existing`: the table holds every record of partition `p` before offset `o` |
| `--tier-after <d>` | `1h` | Only records older than this tier |
| `--tier-interval <d>` | `1h` | How often tiered files are committed |

`--partition`, `--unpartitioned`, `--sort`, `--property` and `--location` apply to a new table. `--end-offset` applies to an existing one.

```bash
mink table attach orders -c mink.toml --catalog lake --namespace kafka --table orders \
  --partition "day(_kafka_timestamp)" --sort "_kafka_key"
```
