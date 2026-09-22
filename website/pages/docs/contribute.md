# Contribute

Thank you for your interest in contributing to Mink. The project lives at [github.com/PicoMQ/mink](https://github.com/PicoMQ/mink). Bug reports, design discussion, and pull requests all go there.

::: tip
If you are new to the project, new to Rust, or just unsure whether a change belongs here, open a PR or an issue anyway. Review is part of how we learn, and there is always more to learn.
:::

## Repository layout

The workspace is split into two areas with a hard boundary between them:

- **`mink/`** is the node and everything it is built from: metadata plane (`mink-metadata`, `mink-sql`), tablets (`mink-log`, `mink-kv`, `mink-tablet`), coordinator, lake tiering (`mink-lake`), union reader (`mink-read`), the Arrow Flight and Kafka frontends, the server, the client and the `mink` CLI. The stream engine underneath is [`s3stream`](https://github.com/PicoMQ/s3stream), pulled in as a dependency. Node crates depend only on the `s3stream` facade crate, never on engine internals.
- **`query/`** is the SQL engine: `mink-query`, the DataFusion catalog and table provider, and `mink-query-cli`, the `mink-query` binary. It reaches the cluster through `mink-client` and the lake through `mink-lake` and `mink-read`, the same libraries a node uses, and never through server internals.

Keeping that boundary intact is a review criterion. If a change in `query/*` needs something from inside the node, the right move is to widen the client.

The docs are at `website/` (`VitePress`). The deployment harness lives in `harness/` (compose stacks for a single node, a cluster, a SQLite and local-files variant, and the query engine).

## Build and test

Rust 1.98+ (`rust-toolchain.toml` pins the exact version). The default test suite has no external dependencies. SQLite and `file://` object storage stand in for Postgres and S3:

```bash
cargo build --workspace
cargo test --workspace
```

Postgres-backed tests are env-gated and skipped unless a URL is provided:

```bash
MINK_PG_URL=postgres://user:pass@localhost:5432/mink \
    cargo test -p mink-sql --test pg
```

End-to-end scenarios run against the compose stacks in `harness/` through `scripts/e2e.sh`. Each scenario builds the images, brings up the stack and runs the `mink-e2e` tests inside it:

```bash
scripts/e2e.sh lite            # SQLite and local files
scripts/e2e.sh cluster sql     # three nodes, Postgres, RustFS, Iceberg REST, mink-query
scripts/e2e.sh                 # lite single cluster load chaos sql
```

A few things the toolchain enforces:

- `unsafe_code` is denied workspace-wide.
- CI runs `cargo fmt --all --check` and `cargo clippy --workspace --all-targets` with warnings denied.
- Wire formats in `s3stream` are pinned upstream by golden fixtures. A change to a format belongs in that repository, not here.

## Docs

The site is VitePress. From `website/`:

```bash
npm install
npm run dev
```

Pages are markdown under `website/pages/docs/`, and the sidebar is defined in `website/.vitepress/config.mts`. Docs follow the same review bar as code.

## Pull requests

Small, focused PRs against `main`. A good PR description says *why* the change exists, not just what it touches. If it changes a wire format, a protocol behavior, or an operational default, call that out explicitly. Run `cargo fmt` and `cargo clippy --workspace` before pushing.

AI-generated (or largely generated) pull requests are welcome, provided that you:

- Call out in the PR description that AI was used, and which tool or model.
- Understand the change and can explain it in review.
- Keep PR discussion human. Descriptions, comments, and review replies.
- Have reviewed the diff yourself before opening the PR.

For anything larger than a bug fix (a new lake format, a KV store engine, a protocol frontend, a metadata backend), [open an issue](https://github.com/PicoMQ/mink/issues) first so the design can be discussed before the code shows up.

By contributing, you agree your work is licensed under Apache 2.0.
