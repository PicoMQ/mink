# CLI

`mink` runs a node and manages the set of tables. Table commands talk to Postgres and Kafka directly, so they need no running node.

| Command | Does |
| --- | --- |
| `mink serve [-c <file>] [--roles <roles>]` | Runs a node |
| `mink table add <topic> [-c <file>] [--granularity <g>]` | Makes a topic a table |
| `mink table remove <topic> [-c <file>]` | Removes a table. Its objects and rows are deleted after `lease_ttl`. |
| `mink table list [-c <file>]` | Lists the tables and their states |
| `mink granularity <topic> <g> [-c <file>]` | Changes a table's time partitioning for new files |

`-c` names a TOML config file. `MINK_*` environment variables override it, and `--roles` overrides both. See [Configuration](/docs/operations/configuration).

`<g>` is `hour`, `day`, `month` or `none`. Without `--granularity`, a table gets the `granularity` setting, `hour` by default.

Each table command has an admin API route that does the same. See [Admin API](/docs/operations/admin).

```bash
mink serve -c mink.toml --roles catalog,s3
mink table add orders -c mink.toml --granularity day
mink table list -c mink.toml
```
