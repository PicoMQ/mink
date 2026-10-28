use std::path::PathBuf;

use mink_server::{Config, Granularity, parse_granularity};

#[derive(clap::Args)]
pub struct Table {
    #[arg(
        long,
        short = 'c',
        global = true,
        help = "TOML config file; MINK_* env vars override it"
    )]
    config: Option<PathBuf>,
    #[command(subcommand)]
    action: Action,
}

#[derive(clap::Subcommand)]
enum Action {
    #[command(about = "make a Kafka topic a table")]
    Add {
        #[arg(help = "a Kafka topic")]
        topic: String,
        #[arg(long, help = "hour, day, month or none; the granularity setting when absent", value_parser = parse_granularity)]
        granularity: Option<Granularity>,
    },
    #[command(about = "remove a table; its objects and rows are deleted after lease_ttl")]
    Remove {
        #[arg(help = "a table")]
        topic: String,
    },
    #[command(about = "list the tables and their states")]
    List,
}

impl Table {
    pub async fn run(self) -> anyhow::Result<()> {
        let config = Config::load(self.config.as_deref())?;

        match self.action {
            Action::Add { topic, granularity } => {
                let added = mink_server::add_table(&config, &topic, granularity).await?;
                println!(
                    "{} is a table with {} partitions",
                    added.name(),
                    added.partition_count()
                );
            }
            Action::Remove { topic } => {
                mink_server::remove_table(&config, &topic).await?;
                println!("{topic} is being removed");
            }
            Action::List => {
                for (topic, state) in mink_server::list_tables(&config).await? {
                    println!(
                        "{}\t{}\t{}",
                        topic.name(),
                        state.name(),
                        topic.partition_count()
                    );
                }
            }
        }

        Ok(())
    }
}
