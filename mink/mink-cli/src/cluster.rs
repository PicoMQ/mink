//! The `cluster` subcommands: describe, health, config, stats and rebalance.

use std::process;

use clap::Subcommand;
use mink_client::{Admin, proto};
use serde_json::Value;

use crate::buckets;
use crate::output::{Printer, duration_ms, pairs, table, when, yes_no};

#[derive(Subcommand)]
pub enum Cmd {
    #[command(about = "nodes with liveness and leader load, the coordinator, counts")]
    Describe,
    #[command(about = "a node's liveness; exit 1 when it is not registered")]
    Health {
        #[arg(long, help = "gRPC address of a node; the bootstrap node when omitted")]
        node: Option<String>,
    },
    #[command(about = "a node's effective configuration")]
    Config {
        #[arg(long)]
        node: Option<String>,
    },
    #[command(about = "hosted buckets and, on the coordinator, the tiering schedule")]
    Stats {
        #[arg(long, conflicts_with = "all")]
        node: Option<String>,
        #[arg(long, help = "every live node")]
        all: bool,
    },
    #[command(about = "even bucket leadership out across live nodes")]
    Rebalance,
}

impl Cmd {
    pub async fn run(self, admin: &Admin, out: &Printer) -> anyhow::Result<()> {
        match self {
            Cmd::Describe => {
                let info = admin.describe_cluster().await?;
                out.emit(&info, || describe(&info))
            }
            Cmd::Health { node } => {
                let health = admin.health(node.as_deref()).await?;
                out.emit(&health, || {
                    pairs(&[
                        ("node", health.node_id.to_string()),
                        ("epoch", health.node_epoch.to_string()),
                        ("registered", yes_no(health.registered).to_owned()),
                        ("coordinator", yes_no(health.coordinator).to_owned()),
                        ("hosted buckets", health.hosted_buckets.to_string()),
                        ("uptime", duration_ms(health.uptime_ms)),
                    ])
                })?;
                if !health.registered {
                    process::exit(1);
                }

                Ok(())
            }
            Cmd::Config { node } => {
                let config = admin.get_config(node.as_deref()).await?;
                out.emit(&config, || {
                    config
                        .entries
                        .iter()
                        .map(|(k, v)| format!("{k} = {v}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            }
            Cmd::Stats { node, all: false } => {
                let stats = admin.node_stats(node.as_deref()).await?;
                out.emit(&stats, || stats_text(&stats))
            }
            Cmd::Stats { all: true, .. } => {
                let all = admin.cluster_stats().await?;
                let json: Vec<Value> = all
                    .iter()
                    .map(|(node, stats)| match stats {
                        Ok(stats) => serde_json::json!({ "node": node, "stats": stats }),
                        Err(e) => serde_json::json!({ "node": node, "error": e.to_string() }),
                    })
                    .collect();
                out.emit(&json, || {
                    all.iter()
                        .map(|(node, stats)| {
                            let body = match stats {
                                Ok(stats) => stats_text(stats),
                                Err(e) => format!("error: {e}"),
                            };
                            format!("== node {} at {}\n{body}", node.node_id, node.address)
                        })
                        .collect::<Vec<_>>()
                        .join("\n\n")
                })
            }
            Cmd::Rebalance => {
                let moves = admin.rebalance().await?;
                out.emit(
                    &proto::Rebalanced {
                        moves: moves.clone(),
                    },
                    || {
                        if moves.is_empty() {
                            return "balanced; nothing moved".to_owned();
                        }

                        let rows: Vec<Vec<String>> = moves
                            .iter()
                            .map(|m| {
                                vec![
                                    buckets::text(m.bucket),
                                    m.from.to_string(),
                                    m.to.to_string(),
                                ]
                            })
                            .collect();
                        table(&["bucket", "from", "to"], &rows)
                    },
                )
            }
        }
    }
}

fn describe(info: &proto::ClusterInfo) -> String {
    let mut text = pairs(&[
        (
            "coordinator",
            info.coordinator.as_ref().map_or_else(
                || "-".to_owned(),
                |c| format!("node {} at {} (epoch {})", c.node_id, c.address, c.epoch),
            ),
        ),
        ("databases", info.databases.to_string()),
        ("tables", info.tables.to_string()),
        ("partitions", info.partitions.to_string()),
        (
            "buckets",
            format!("{} ({} without a leader)", info.buckets, info.unled_buckets),
        ),
    ]);

    text.push_str("\n\n");
    let rows: Vec<Vec<String>> = info
        .nodes
        .iter()
        .map(|n| {
            vec![
                n.node_id.to_string(),
                n.address.clone(),
                yes_no(n.live).to_owned(),
                n.leading.to_string(),
                n.epoch.to_string(),
                n.protocols
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(" "),
            ]
        })
        .collect();
    text.push_str(&table(
        &["node", "address", "live", "leading", "epoch", "protocols"],
        &rows,
    ));

    text
}

fn stats_text(stats: &proto::NodeStats) -> String {
    let dash = || "-".to_owned();
    let mut text = pairs(&[
        ("node", stats.node_id.to_string()),
        ("epoch", stats.node_epoch.to_string()),
        ("coordinator", yes_no(stats.coordinator).to_owned()),
        ("hosted buckets", stats.buckets.len().to_string()),
    ]);

    if !stats.buckets.is_empty() {
        text.push_str("\n\n");
        let rows: Vec<Vec<String>> = stats
            .buckets
            .iter()
            .map(|b| {
                let kv = |f: fn(&proto::KvStats) -> i64| {
                    b.kv.as_ref().map_or_else(dash, |kv| f(kv).to_string())
                };
                vec![
                    b.path.to_string(),
                    buckets::text(b.bucket),
                    b.leader_epoch.to_string(),
                    b.log_start_offset.to_string(),
                    b.high_watermark.to_string(),
                    b.log_end_offset.to_string(),
                    b.writers.to_string(),
                    kv(|kv| kv.row_count),
                    kv(|kv| kv.flushed_log_offset),
                    kv(|kv| kv.snapshot_log_offset),
                    b.retention.map_or_else(dash, |r| r.offset.to_string()),
                ]
            })
            .collect();
        text.push_str(&table(
            &[
                "table",
                "bucket",
                "epoch",
                "start",
                "hw",
                "end",
                "writers",
                "kv rows",
                "kv flushed",
                "kv snapshot",
                "retained from",
            ],
            &rows,
        ));
    }

    if !stats.tiering.is_empty() {
        text.push_str("\n\ntiering:\n");
        let rows: Vec<Vec<String>> = stats
            .tiering
            .iter()
            .map(|t| {
                vec![
                    t.path.to_string(),
                    t.state.clone(),
                    t.epoch.to_string(),
                    when(t.last_tiered_ms),
                    t.due_ms.map_or_else(dash, when),
                    t.heartbeat_ms.map_or_else(dash, when),
                ]
            })
            .collect();
        text.push_str(&table(
            &["table", "state", "epoch", "last tiered", "due", "heartbeat"],
            &rows,
        ));
    }

    text
}
