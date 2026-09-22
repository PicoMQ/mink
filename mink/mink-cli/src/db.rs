//! The `db` subcommands: list, create and drop databases.

use clap::Subcommand;
use mink_client::Admin;

use crate::output::Printer;
use crate::parse::{Pair, key_value};

#[derive(Subcommand)]
pub enum Cmd {
    List,
    Create {
        name: String,
        #[arg(long)]
        comment: Option<String>,
        #[arg(long = "property", value_parser = key_value, help = "key=value, repeatable")]
        properties: Vec<Pair>,
        #[arg(long)]
        if_not_exists: bool,
    },
    Drop {
        name: String,
        #[arg(long)]
        if_exists: bool,
        #[arg(long, help = "drop the tables in it too")]
        cascade: bool,
    },
}

impl Cmd {
    pub async fn run(self, admin: &Admin, out: &Printer) -> anyhow::Result<()> {
        match self {
            Cmd::List => {
                let names = admin.list_databases().await?;
                out.emit(&names, || names.join("\n"))
            }
            Cmd::Create {
                name,
                comment,
                properties,
                if_not_exists,
            } => {
                admin
                    .create_database(
                        &name,
                        comment.as_deref(),
                        properties.into_iter().collect(),
                        if_not_exists,
                    )
                    .await?;
                out.done(format!("created database {name}"))
            }
            Cmd::Drop {
                name,
                if_exists,
                cascade,
            } => {
                admin.drop_database(&name, if_exists, cascade).await?;
                out.done(format!("dropped database {name}"))
            }
        }
    }
}
