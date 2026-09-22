//! The `mink` binary: parses the command line, then either runs a node or connects to one
//! and dispatches to the subcommand.

mod bench;
mod buckets;
mod cluster;
mod data;
mod db;
mod output;
mod parse;
mod partition;
mod producer;
mod serve;
mod table;

use std::io;

use clap::{Parser, Subcommand};
use mink_client::Cluster;

use crate::output::Printer;

#[derive(Parser)]
#[command(name = "mink", version, about = "streaming storage for tables")]
struct Cli {
    #[arg(
        long,
        short = 'b',
        global = true,
        env = "MINK_BOOTSTRAP",
        default_value = "grpc://127.0.0.1:9123",
        help = "gRPC address of any node, comma separated for several (also MINK_BOOTSTRAP)"
    )]
    bootstrap: String,
    #[arg(long, global = true, help = "print protocol messages as JSON")]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "run a node")]
    Serve(serve::Serve),
    #[command(subcommand, about = "nodes, config, stats, health, rebalance")]
    Cluster(cluster::Cmd),
    #[command(subcommand, about = "databases")]
    Db(db::Cmd),
    #[command(subcommand, about = "tables: DDL, offsets, snapshots")]
    Table(table::Cmd),
    #[command(subcommand, about = "partitions of a partitioned table")]
    Partition(partition::Cmd),
    #[command(
        subcommand,
        name = "producer-offsets",
        about = "a sink's registered start offsets"
    )]
    ProducerOffsets(producer::Cmd),
    #[command(about = "rows of a table as JSON lines")]
    Read(data::Read),
    #[command(about = "JSON lines from stdin into a table")]
    Write(data::Write),
    #[command(about = "concurrent append or upsert load with latency percentiles")]
    Bench(bench::Bench),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(io::stderr)
        .init();

    let cli = Cli::parse();
    let out = Printer::new(cli.json);
    match cli.command {
        Command::Serve(serve) => serve.run().await,
        command => {
            let cluster = Cluster::connect_all(
                cli.bootstrap
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect(),
            )?;
            let admin = cluster.admin();
            match command {
                Command::Serve(_) => unreachable!(),
                Command::Cluster(cmd) => cmd.run(&admin, &out).await,
                Command::Db(cmd) => cmd.run(&admin, &out).await,
                Command::Table(cmd) => cmd.run(&admin, &out).await,
                Command::Partition(cmd) => cmd.run(&admin, &out).await,
                Command::ProducerOffsets(cmd) => cmd.run(&admin, &out).await,
                Command::Read(cmd) => cmd.run(&cluster).await,
                Command::Write(cmd) => cmd.run(&cluster, &out).await,
                Command::Bench(cmd) => cmd.run(&cluster, &out).await,
            }
        }
    }
}
