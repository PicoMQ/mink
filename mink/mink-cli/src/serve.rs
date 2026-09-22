//! The `serve` command: loads the node configuration, applies flag overrides and runs a node
//! until Ctrl-C.

use std::net::SocketAddr;
use std::path::PathBuf;

use mink_runtime::ServerConfig;

#[derive(clap::Args)]
pub struct Serve {
    #[arg(
        long,
        short = 'c',
        help = "TOML config file; MINK_* env vars override it, these flags override both"
    )]
    config: Option<PathBuf>,
    #[arg(long)]
    node_id: Option<i32>,
    #[arg(long)]
    cluster_id: Option<String>,
    #[arg(long, help = "sqlite::memory:, sqlite:<path>, or a postgres URL")]
    meta_url: Option<String>,
    #[arg(long, help = "s3stream id-URI of the data bucket")]
    storage_uri: Option<String>,
    #[arg(long)]
    wal_uri: Option<String>,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long, help = "Flight bind address, e.g. 0.0.0.0:9123")]
    listen: Option<SocketAddr>,
    #[arg(
        long,
        help = "gRPC address clients and other nodes use to reach this node"
    )]
    advertise: Option<String>,
    #[arg(long, help = "Kafka bind address; enables the Kafka listener")]
    kafka_listen: Option<SocketAddr>,
    #[arg(long, help = "host:port Kafka clients use to reach this node")]
    kafka_advertise: Option<String>,
}

impl Serve {
    fn config(self) -> anyhow::Result<ServerConfig> {
        let mut config = ServerConfig::load(self.config.as_deref())?;
        if let Some(v) = self.node_id {
            config.node_id = v;
        }
        if let Some(v) = self.cluster_id {
            config.cluster_id = v;
        }
        if let Some(v) = self.meta_url {
            config.meta_url = v;
        }
        if let Some(v) = self.storage_uri {
            config.storage_uri = v;
        }
        if self.wal_uri.is_some() {
            config.wal_uri = self.wal_uri;
        }
        if let Some(v) = self.data_dir {
            config.data_dir = v;
        }
        if let Some(v) = self.listen {
            config.listen = v;
        }
        if let Some(v) = self.advertise {
            config.advertise = v;
        }
        if self.kafka_listen.is_some() || self.kafka_advertise.is_some() {
            let mut kafka = config.kafka.unwrap_or_default();
            if let Some(listen) = self.kafka_listen {
                kafka.listen = listen;
            }
            if self.kafka_advertise.is_some() {
                kafka.advertise = self.kafka_advertise;
            }
            config.kafka = Some(kafka);
        }

        Ok(config)
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let server = mink_runtime::start(self.config()?).await?;
        tokio::signal::ctrl_c().await?;
        tracing::info!("shutting down");
        server.shutdown().await;
        Ok(())
    }
}
