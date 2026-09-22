//! The `partition` subcommands: list, create and drop partitions of a partitioned table.

use clap::Subcommand;
use mink_client::{Admin, proto};
use mink_table::Path;

use crate::output::{Printer, table};
use crate::parse::{Pair, key_value, partition_spec};

#[derive(Subcommand)]
pub enum Cmd {
    List {
        path: Path,
    },
    #[command(about = "one key=value pair per partition key")]
    Create {
        path: Path,
        #[arg(required = true, value_parser = key_value)]
        spec: Vec<Pair>,
        #[arg(long)]
        if_not_exists: bool,
    },
    Drop {
        path: Path,
        #[arg(required = true, value_parser = key_value)]
        spec: Vec<Pair>,
        #[arg(long)]
        if_exists: bool,
    },
}

impl Cmd {
    pub async fn run(self, admin: &Admin, out: &Printer) -> anyhow::Result<()> {
        match self {
            Cmd::List { path } => {
                let partitions = admin.list_partitions(&path).await?;
                out.emit(&partitions, || {
                    let rows: Vec<Vec<String>> = partitions
                        .iter()
                        .map(|p| vec![p.partition_id.0.to_string(), p.name.to_string()])
                        .collect();
                    table(&["id", "partition"], &rows)
                })
            }
            Cmd::Create {
                path,
                spec,
                if_not_exists,
            } => {
                let spec = partition_spec(spec)?;
                let id = admin.create_partition(&path, &spec, if_not_exists).await?;
                out.emit(&proto::PartitionCreated { partition_id: id }, || match id {
                    Some(id) => {
                        format!("created partition {} of {path} (id {})", spec.name(), id.0)
                    }
                    None => format!("partition {} of {path} already exists", spec.name()),
                })
            }
            Cmd::Drop {
                path,
                spec,
                if_exists,
            } => {
                let spec = partition_spec(spec)?;
                admin.drop_partition(&path, &spec, if_exists).await?;
                out.done(format!("dropped partition {} of {path}", spec.name()))
            }
        }
    }
}
