# Contribute

Thank you for your interest in contributing to Mink. The project lives at [github.com/PicoMQ/mink](https://github.com/PicoMQ/mink). Bug reports, design discussion, and pull requests all go there.

::: tip
If you are new to the project, new to Rust, or just unsure whether a change belongs here, open a PR or an issue anyway. Review is part of how we learn, and there is always more to learn.
:::

## Repository layout

| Path | Holds |
| --- | --- |
| `mink/mink-core` | The domain model: topics, partitions, files, snapshots and metadata objects |
| `mink/mink-kafka` | The Kafka client: topic discovery, offsets, and the committed records of an offset range |
| `mink/mink-schema` | Schema Registry schemas, decoding Avro, Protobuf and JSON payloads, and merging them into the table schema |
| `mink/mink-iceberg` | Kafka records as rows, the Parquet files that hold them, and the Iceberg metadata over those files |
| `mink/mink-store` | State in Postgres: tables, indexed ranges, files, snapshots, leases and metadata objects |
| `mink/mink-metrics` | The Prometheus metrics |
| `mink/mink-server` | A node: config and roles, indexers and writers, the catalog, the S3 endpoint and the admin API |
| `mink/mink-cli` | The `mink` binary |
| `mink/mink-conformance` | The fixture generator, and end-to-end tests against Kafka, DuckDB, PyIceberg, Spark and Trino |
| `console/` | The admin console, embedded in the binary |
| `specification/` | The file formats, see [File formats](/docs/formats) |
| `conformance/fixtures/` | The fixtures each format is checked against |
| `harness/` | Compose files for Kafka, Schema Registry, Postgres, RustFS, Spark and Trino |
| `website/` | This site |

## Build and test

Rust 1.98 or newer. `rust-toolchain.toml` pins the exact version. The console is built first, so the binary embeds it.

```bash
(cd console && npm ci && npm run build)
cargo build --workspace
cargo test
```

`cargo test` runs every crate except `mink-conformance`. Tests that need a service run when its variable is set, and are skipped otherwise. The compose stack provides the services:

```bash
docker compose -f harness/compose.yml up -d postgres kafka rustfs schema-registry createbucket
TEST_POSTGRES_URL=postgres://mink:mink@localhost:5432/mink \
TEST_KAFKA_BOOTSTRAP=localhost:29092 \
TEST_S3_ENDPOINT=http://localhost:9000 \
TEST_SCHEMA_REGISTRY=http://localhost:8081 \
cargo test
```

The console has its own checks. From `console/`:

```bash
npm run check && npm run lint && npm test
```

### End to end

`scripts/e2e.sh` runs `mink-conformance` in Docker against DuckDB, PyIceberg, Spark and Trino, then the console's browser tests against a running node. It needs Docker only.

```bash
scripts/e2e.sh                      # everything
SKIP_CONSOLE=1 scripts/e2e.sh       # without the console
KEEP=1 scripts/e2e.sh               # leave the stack running
```

The `e2e` workflow runs the same script on demand from GitHub Actions.

### Toolchain

- `unsafe_code` is denied workspace-wide.
- CI runs `cargo fmt --all --check` and `cargo clippy --workspace --all-targets` with warnings denied.

## File formats

Every file Mink writes has a specification in `specification/` and fixtures in `conformance/fixtures/`.

- A change to a format updates its specification, its version and its fixtures in the same PR.
- Never regenerate or edit fixtures to make a failing test pass. A failing fixture test means the format changed.

## Docs

The site is VitePress. From `website/`:

```bash
npm install
npm run dev
```

Pages are markdown under `website/pages/docs/`, and the sidebar is defined in `website/.vitepress/config.mts`. Docs follow the same review bar as code.

## Pull requests

Small, focused PRs against `main`. A good PR description says *why* the change exists, not just what it touches. If it changes a file format, a catalog behavior, or an operational default, call that out explicitly.

AI-generated (or largely generated) pull requests are welcome, provided that you:

- Call out in the PR description that AI was used, and which tool or model.
- Understand the change and can explain it in review.
- Keep PR discussion human. Descriptions, comments, and review replies.
- Have reviewed the diff yourself before opening the PR.

For anything larger than a bug fix (a new schema format, a catalog API change, a new role), [open an issue](https://github.com/PicoMQ/mink/issues) first so the design can be discussed before the code shows up.

By contributing, you agree your work is licensed under Apache 2.0.
