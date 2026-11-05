use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use mink_server::{
    Attachment, CatalogTable, Config, Granularity, SortTerm, Source, Term, TierSchedule,
    parse_duration, parse_granularity,
};

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
    #[command(
        about = "attach a new or existing Iceberg table that the table's older records tier into"
    )]
    Attach(Box<Attach>),
    #[command(about = "detach the table's external table, which is left as it is")]
    Detach {
        #[arg(help = "a table")]
        topic: String,
    },
    #[command(about = "print a table's Iceberg schema, field ids included")]
    Schema {
        #[arg(help = "a table")]
        topic: String,
    },
}

#[derive(clap::Args)]
struct Attach {
    #[arg(help = "a table")]
    topic: String,
    #[arg(long, help = "an external catalog of the config")]
    catalog: String,
    #[arg(long, help = "the Iceberg namespace")]
    namespace: String,
    #[arg(long = "table", help = "the Iceberg table")]
    table: String,
    #[arg(
        long,
        help = "attach a table that already exists instead of creating one"
    )]
    existing: bool,
    #[arg(
        long,
        help = "a partition term such as day(_kafka_timestamp) or bucket(16, _kafka_key), repeatable"
    )]
    partition: Vec<Term>,
    #[arg(
        long,
        help = "create the table unpartitioned",
        conflicts_with = "partition"
    )]
    unpartitioned: bool,
    #[arg(
        long,
        help = "a sort term such as _kafka_key desc nulls last, repeatable"
    )]
    sort: Vec<SortTerm>,
    #[arg(long, value_parser = property, help = "a table property key=value, repeatable")]
    property: Vec<(String, String)>,
    #[arg(long, help = "where the new table keeps its files")]
    location: Option<String>,
    #[arg(long = "end-offset", value_parser = end_offset, help = "partition=offset: the existing table holds every record of the partition before offset, repeatable")]
    end_offsets: Vec<(i32, i64)>,
    #[arg(long, value_parser = parse_duration, help = "only records older than this are tiered, 1h when absent")]
    tier_after: Option<Duration>,
    #[arg(long, value_parser = parse_duration, help = "how often tiered files are committed, 1h when absent")]
    tier_interval: Option<Duration>,
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
            Action::Attach(attach) => {
                let external =
                    mink_server::attach_external_table(&config, attach.attachment()?).await?;
                let table = external.table();
                println!(
                    "{} tiers into {}.{} at {}",
                    external.topic(),
                    table.namespace(),
                    table.name(),
                    external.location()
                );
            }
            Action::Detach { topic } => {
                mink_server::detach_external_table(&config, &topic).await?;
                println!("{topic} is no longer tiered");
            }
            Action::Schema { topic } => {
                let schema = mink_server::table_schema(&config, &topic).await?;
                println!("{}", serde_json::to_string_pretty(&schema)?);
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

impl Attach {
    fn attachment(self) -> anyhow::Result<Attachment> {
        let schedule = TierSchedule::given(self.tier_after, self.tier_interval)?;
        let partition = match (self.unpartitioned, self.partition.is_empty()) {
            (true, _) => Some(Vec::new()),
            (false, true) => None,
            (false, false) => Some(self.partition),
        };
        let source = Source::new(
            self.existing,
            partition,
            self.sort,
            self.property.into_iter().collect::<HashMap<_, _>>(),
            self.location,
            self.end_offsets.into_iter().collect::<BTreeMap<_, _>>(),
        )?;

        Ok(Attachment {
            topic: self.topic,
            table: CatalogTable::new(&self.catalog, &self.namespace, &self.table),
            schedule,
            source,
        })
    }
}

fn property(text: &str) -> Result<(String, String), String> {
    text.split_once('=')
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .ok_or_else(|| format!("{text:?} is not key=value"))
}

fn end_offset(text: &str) -> Result<(i32, i64), String> {
    let unreadable = || format!("{text:?} is not partition=offset");
    let (partition, offset) = text.split_once('=').ok_or_else(unreadable)?;

    Ok((
        partition.parse().map_err(|_| unreadable())?,
        offset.parse().map_err(|_| unreadable())?,
    ))
}
