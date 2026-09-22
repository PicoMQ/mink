//! The `producer-offsets` subcommands: show or forget a sink's registered start offsets.

use clap::Subcommand;
use mink_client::{Admin, proto};

use crate::buckets;
use crate::output::{Printer, table, when};

#[derive(Subcommand)]
pub enum Cmd {
    #[command(about = "a sink's registered start offsets")]
    Get { producer_id: String },
    #[command(about = "forget them")]
    Delete { producer_id: String },
}

impl Cmd {
    pub async fn run(self, admin: &Admin, out: &Printer) -> anyhow::Result<()> {
        match self {
            Cmd::Get { producer_id } => {
                let snapshot = admin.producer_offsets(&producer_id).await?;
                out.emit(
                    &proto::ProducerOffsetsResult {
                        snapshot: snapshot.clone(),
                    },
                    || {
                        let Some(s) = &snapshot else {
                            return format!("nothing registered under {producer_id}");
                        };

                        let rows: Vec<Vec<String>> = s
                            .offsets
                            .iter()
                            .map(|o| {
                                vec![
                                    o.bucket.table().0.to_string(),
                                    buckets::text(o.bucket),
                                    o.offset.to_string(),
                                ]
                            })
                            .collect();
                        format!(
                            "expires {}\n{}",
                            when(s.expires_ms),
                            table(&["table id", "bucket", "offset"], &rows)
                        )
                    },
                )
            }
            Cmd::Delete { producer_id } => {
                admin.delete_producer_offsets(&producer_id).await?;
                out.done(format!("deleted producer offsets {producer_id}"))
            }
        }
    }
}
