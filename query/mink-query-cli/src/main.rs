//! The `mink-query` binary: connects an engine from flags and an optional TOML file, then serves
//! Flight SQL, runs statements, or opens a shell.

mod flight;
mod run;
mod shell;

use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::{Parser, Subcommand};
use figment::Figment;
use figment::providers::{Env, Format as _, Serialized, Toml};
use mink_query::{Config, Engine};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::flight::Service;
use crate::run::Format;

#[derive(Parser)]
#[command(name = "mink-query", version, about = "SQL over Mink tables")]
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
    #[arg(
        long,
        short = 'c',
        global = true,
        env = "MINK_QUERY_CONFIG",
        help = "TOML file with the [lake] block and engine settings; MINK_QUERY_* env vars override it"
    )]
    config: Option<PathBuf>,
    #[arg(
        long,
        short = 'd',
        global = true,
        env = "MINK_QUERY_DATABASE",
        help = "database that unqualified table names resolve in"
    )]
    database: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "serve Flight SQL")]
    Serve {
        #[arg(long, env = "MINK_QUERY_LISTEN", default_value = "0.0.0.0:9130")]
        listen: SocketAddr,
    },
    #[command(about = "run statements from -e, a file, or stdin")]
    Exec {
        #[arg(short = 'e', long, conflicts_with = "file")]
        sql: Option<String>,
        #[arg(short = 'f', long)]
        file: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = Format::Table)]
        format: Format,
    },
    #[command(about = "interactive shell")]
    Shell {
        #[arg(long, value_enum, default_value_t = Format::Table)]
        format: Format,
    },
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct File {
    lake: Option<mink_lake::Config>,
    memory_limit: Option<usize>,
    target_partitions: Option<usize>,
    batch_size: Option<usize>,
}

impl File {
    fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let mut figment = Figment::from(Serialized::defaults(File::default()));
        if let Some(path) = path {
            figment = figment.merge(Toml::file(path));
        }

        Ok(figment.merge(Env::prefixed("MINK_QUERY_")).extract()?)
    }
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
    let file = File::load(cli.config.as_deref())?;
    let mut config = Config::new(cli.bootstrap.split(',').map(str::to_owned).collect());
    config.database = cli.database;
    config.lake = file.lake;
    config.memory_limit = file.memory_limit;
    config.target_partitions = file.target_partitions;
    if let Some(batch_size) = file.batch_size {
        config.batch_size = batch_size;
    }
    let engine = Engine::connect(config).await?;

    match cli.command {
        Command::Serve { listen } => serve(engine, listen).await,
        Command::Exec { sql, file, format } => {
            let text = match (sql, file) {
                (Some(sql), _) => sql,
                (None, Some(path)) => std::fs::read_to_string(path)?,
                (None, None) => {
                    let mut text = String::new();
                    io::stdin().read_to_string(&mut text)?;
                    text
                }
            };
            for statement in run::split(&text) {
                run::statement(&engine, &statement, format).await?;
            }
            Ok(())
        }
        Command::Shell { format } => shell::run(&engine, format).await,
    }
}

async fn serve(engine: Engine, listen: SocketAddr) -> anyhow::Result<()> {
    let service = Service::new(Arc::new(engine))?;
    info!(%listen, "serving Flight SQL");
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve_with_shutdown(listen, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}
