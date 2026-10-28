mod granularity;
mod serve;
mod table;

use std::io;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "mink", version, about = "Kafka topics as Iceberg tables")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "run a node")]
    Serve(serve::Serve),
    #[command(about = "add, remove or list tables")]
    Table(table::Table),
    #[command(about = "change a table's granularity")]
    Granularity(granularity::SetGranularity),
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

    match Cli::parse().command {
        Command::Serve(serve) => serve.run().await,
        Command::Table(table) => table.run().await,
        Command::Granularity(granularity) => granularity.run().await,
    }
}
