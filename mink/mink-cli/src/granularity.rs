use std::path::PathBuf;

use mink_server::{Config, Granularity, parse_granularity};

#[derive(clap::Args)]
pub struct SetGranularity {
    #[arg(
        long,
        short = 'c',
        help = "TOML config file; MINK_* env vars override it"
    )]
    config: Option<PathBuf>,
    #[arg(help = "a table")]
    topic: String,
    #[arg(help = "hour, day, month or none", value_parser = parse_granularity)]
    granularity: Granularity,
}

impl SetGranularity {
    pub async fn run(self) -> anyhow::Result<()> {
        let config = Config::load(self.config.as_deref())?;

        let spec_id = mink_server::set_granularity(&config, &self.topic, self.granularity).await?;

        println!(
            "{} is partitioned by {} under spec {spec_id}",
            self.topic,
            self.granularity.name()
        );
        Ok(())
    }
}
