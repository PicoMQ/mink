use std::io;
use std::path::PathBuf;

use mink_server::{Config, Node, Role, parse_role};

#[derive(clap::Args)]
pub struct Serve {
    #[arg(
        long,
        short = 'c',
        help = "TOML config file; MINK_* env vars override it, flags override both"
    )]
    config: Option<PathBuf>,
    #[arg(
        long,
        value_delimiter = ',',
        value_parser = parse_role,
        help = "roles to run, e.g. catalog,s3"
    )]
    roles: Vec<Role>,
}

impl Serve {
    pub async fn run(self) -> anyhow::Result<()> {
        let mut config = Config::load(self.config.as_deref())?;
        if !self.roles.is_empty() {
            config = config.with_roles(self.roles);
        }
        for (key, value) in config.describe() {
            tracing::info!("config {key} = {value}");
        }

        let node = Node::start(config).await?;
        tracing::info!(catalog_address = ?node.catalog_address(), s3_address = ?node.s3_address(), "started");

        stopped().await?;
        tracing::info!("shutting down");
        node.shutdown().await?;

        Ok(())
    }
}

#[cfg(unix)]
async fn stopped() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;

    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn stopped() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}
