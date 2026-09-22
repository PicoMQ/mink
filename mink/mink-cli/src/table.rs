//! The `table` subcommands: list, describe, create, drop and alter tables, and inspect
//! their log offsets, lake snapshot and KV snapshots.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use mink_client::{Admin, proto};
use mink_table::{
    Change, ChangelogImage, Descriptor, KvFormat, LakeFormat, LogFormat, MergeEngine, Options,
    PartitionName, Path, PrimaryKey, Schema,
};

use crate::buckets;
use crate::output::{Printer, opt, pairs, table, when, yes_no};
use crate::parse::{Pair, column, duration, enum_value, key_value, names, offset_spec};

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Cmd {
    #[command(about = "table names of a database")]
    List {
        database: String,
    },
    #[command(about = "descriptor, schemas and bucket leaders")]
    Describe {
        path: Path,
    },
    Create(Create),
    Drop {
        path: Path,
        #[arg(long)]
        if_exists: bool,
    },
    #[command(about = "add columns and change options, all or nothing")]
    Alter {
        path: Path,
        #[arg(
            long = "add-column",
            help = "name TYPE, appended after the existing columns"
        )]
        add_columns: Vec<String>,
        #[arg(long = "set", value_parser = key_value, help = "key=value option or property, repeatable")]
        sets: Vec<Pair>,
        #[arg(
            long = "reset",
            help = "back to the default, or gone for a free-form property"
        )]
        resets: Vec<String>,
        #[arg(long)]
        if_exists: bool,
    },
    #[command(about = "log offsets of every bucket, or one")]
    Offsets {
        path: Path,
        #[arg(long)]
        bucket: Option<u32>,
        #[arg(long)]
        partition: Option<PartitionName>,
        #[arg(
            long,
            default_value = "latest",
            help = "earliest, latest, or a timestamp (ms or RFC 3339)"
        )]
        at: String,
    },
    #[command(about = "what the lake holds of a lake-enabled table")]
    LakeSnapshot {
        path: Path,
    },
    #[command(about = "newest completed KV snapshot of a primary-key bucket")]
    KvSnapshot {
        path: Path,
        #[arg(long)]
        bucket: u32,
        #[arg(long)]
        partition: Option<PartitionName>,
    },
}

#[derive(Args)]
pub struct Create {
    path: Path,
    #[arg(
        long = "column",
        short = 'C',
        help = "name TYPE [NOT NULL], in order; e.g. BIGINT, STRING, DECIMAL(10,2), ARRAY<INT>"
    )]
    columns: Vec<String>,
    #[arg(long, help = "makes it a primary-key table: k or k,region")]
    primary_key: Option<String>,
    #[arg(long)]
    partition_by: Option<String>,
    #[arg(
        long,
        help = "columns rows are hashed by; primary key minus partition keys when omitted"
    )]
    bucket_key: Option<String>,
    #[arg(long, help = "the cluster default when omitted")]
    bucket_count: Option<u32>,
    #[arg(long)]
    comment: Option<String>,
    #[arg(long, help = "arrow")]
    log_format: Option<String>,
    #[arg(long, help = "compacted or indexed; primary-key tables")]
    kv_format: Option<String>,
    #[arg(long, help = "first_row, aggregation, or versioned:<column>")]
    merge_engine: Option<String>,
    #[arg(long, help = "full or wal")]
    changelog_image: Option<String>,
    #[arg(long, help = "how long log records are kept: 7d, 12h, or forever")]
    log_ttl: Option<String>,
    #[arg(long, help = "tier into the cluster's lake: iceberg")]
    lake: Option<String>,
    #[arg(long, help = "how far the lake may lag: 3m")]
    lake_freshness: Option<String>,
    #[arg(long)]
    lake_auto_compaction: bool,
    #[arg(
        long,
        help = "attach to the existing lake table instead of creating one"
    )]
    lake_attach: bool,
    #[arg(long = "property", value_parser = key_value, help = "free-form key=value, repeatable")]
    properties: Vec<Pair>,
    #[arg(
        long,
        conflicts_with_all = ["columns", "primary_key", "partition_by", "bucket_key", "bucket_count", "comment", "log_format", "kv_format", "merge_engine", "changelog_image", "log_ttl", "lake", "lake_freshness", "lake_auto_compaction", "lake_attach", "properties"],
        help = "a whole Descriptor as JSON (what describe --json prints)"
    )]
    descriptor: Option<PathBuf>,
    #[arg(long)]
    if_not_exists: bool,
}

impl Create {
    fn descriptor(&self) -> anyhow::Result<Descriptor> {
        if let Some(file) = &self.descriptor {
            let text =
                fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
            return serde_json::from_str(&text)
                .with_context(|| format!("parse {}", file.display()));
        }
        if self.columns.is_empty() && !self.lake_attach {
            bail!("a table needs at least one --column (or --descriptor, or --lake-attach)");
        }

        let mut schema = Schema::builder();
        for text in &self.columns {
            schema = schema.column(column(text)?);
        }
        if let Some(key) = &self.primary_key {
            schema = schema.primary_key(PrimaryKey::new(names(key))?);
        }

        let mut builder = Descriptor::builder(schema.build()?);
        if let Some(keys) = &self.partition_by {
            builder = builder.partitioned_by(names(keys));
        }
        if let Some(keys) = &self.bucket_key {
            builder = builder.bucket_keys(names(keys));
        }
        if let Some(count) = self.bucket_count {
            builder = builder.bucket_count(count);
        }
        if let Some(comment) = &self.comment {
            builder = builder.comment(comment.clone());
        }

        Ok(builder
            .options(self.options()?)
            .customs(self.properties.iter().cloned().collect())
            .build()?)
    }

    fn options(&self) -> anyhow::Result<Options> {
        let mut options = Options::default();
        if let Some(v) = &self.log_format {
            options.log_format = enum_value::<LogFormat>("log format", v)?;
        }
        if let Some(v) = &self.kv_format {
            options.kv_format = enum_value::<KvFormat>("kv format", v)?;
        }
        if let Some(v) = &self.merge_engine {
            options.merge_engine = Some(match v.split_once(':') {
                Some(("versioned", column)) => MergeEngine::Versioned {
                    column: column.trim().to_owned(),
                },
                _ => enum_value::<MergeEngine>("merge engine", v)?,
            });
        }
        if let Some(v) = &self.changelog_image {
            options.changelog_image = enum_value::<ChangelogImage>("changelog image", v)?;
        }
        if let Some(v) = &self.log_ttl {
            options.log_ttl = match v.as_str() {
                "forever" | "none" | "-1" => None,
                v => Some(duration("log ttl", v)?),
            };
        }
        if let Some(v) = &self.lake {
            options.lake = Some(enum_value::<LakeFormat>("lake format", v)?);
        }
        if let Some(v) = &self.lake_freshness {
            options.lake_freshness = duration("lake freshness", v)?;
        }
        options.lake_auto_compaction = self.lake_auto_compaction;
        options.lake_attach = self.lake_attach;

        Ok(options)
    }
}

impl Cmd {
    pub async fn run(self, admin: &Admin, out: &Printer) -> anyhow::Result<()> {
        match self {
            Cmd::List { database } => {
                let names = admin.list_tables(&database).await?;
                out.emit(&names, || names.join("\n"))
            }
            Cmd::Describe { path } => {
                let info = admin.get_table(&path).await?;
                out.emit(&info, || describe(&info))
            }
            Cmd::Create(create) => {
                let descriptor = create.descriptor()?;
                let id = admin
                    .create_table(&create.path, &descriptor, create.if_not_exists)
                    .await?;
                out.emit(&proto::Created { table_id: id }, || match id {
                    Some(id) => format!("created table {} (id {})", create.path, id.0),
                    None => format!("table {} already exists", create.path),
                })
            }
            Cmd::Drop { path, if_exists } => {
                admin.drop_table(&path, if_exists).await?;
                out.done(format!("dropped table {path}"))
            }
            Cmd::Alter {
                path,
                add_columns,
                sets,
                resets,
                if_exists,
            } => {
                let mut changes = Vec::new();
                for text in &add_columns {
                    let column = column(text)?;
                    changes.push(Change::add_column(
                        column.name(),
                        column.data_type().clone(),
                    ));
                }
                changes.extend(sets.into_iter().map(|(key, value)| Change::set(key, value)));
                changes.extend(resets.into_iter().map(Change::reset));
                if changes.is_empty() {
                    bail!("nothing to alter: pass --add-column, --set or --reset");
                }

                let schema_id = admin.alter_table(&path, changes, if_exists).await?;
                out.emit(&proto::Altered { schema_id }, || match schema_id {
                    Some(id) => format!("altered {path}; schema id is now {}", id.0),
                    None => format!("table {path} does not exist"),
                })
            }
            Cmd::Offsets {
                path,
                bucket,
                partition,
                at,
            } => {
                let spec = offset_spec(&at)?;
                let info = admin.get_table(&path).await?;
                let buckets = buckets::resolve(admin, &info, bucket, partition.as_ref()).await?;

                let mut offsets = Vec::with_capacity(buckets.len());
                for bucket in buckets {
                    let offset = admin.list_offset(&path, bucket, spec).await?;
                    offsets.push(proto::BucketOffset { bucket, offset });
                }

                out.emit(&offsets, || {
                    let rows: Vec<Vec<String>> = offsets
                        .iter()
                        .map(|o| vec![buckets::text(o.bucket), o.offset.to_string()])
                        .collect();
                    table(&["bucket", "offset"], &rows)
                })
            }
            Cmd::LakeSnapshot { path } => {
                let snapshot = admin.lake_snapshot(&path).await?;
                out.emit(
                    &proto::LakeSnapshotResult {
                        snapshot: snapshot.clone(),
                    },
                    || {
                        let Some(snapshot) = &snapshot else {
                            return format!("{path} has no lake snapshot yet");
                        };

                        let rows: Vec<Vec<String>> = snapshot
                            .bucket_log_end_offset
                            .iter()
                            .map(|(b, o)| vec![buckets::text(*b), o.to_string()])
                            .collect();
                        format!(
                            "snapshot {}\n{}",
                            snapshot.snapshot_id,
                            table(&["bucket", "tiered to"], &rows)
                        )
                    },
                )
            }
            Cmd::KvSnapshot {
                path,
                bucket,
                partition,
            } => {
                let info = admin.get_table(&path).await?;
                let bucket =
                    buckets::resolve(admin, &info, Some(bucket), partition.as_ref()).await?[0];
                let snapshot = admin.latest_kv_snapshot(bucket).await?;
                out.emit(
                    &proto::LatestKvSnapshot {
                        snapshot: snapshot.clone(),
                    },
                    || match &snapshot {
                        None => format!("{path} {} has no KV snapshot yet", buckets::text(bucket)),
                        Some(s) => pairs(&[
                            ("snapshot", s.snapshot_id.to_string()),
                            ("log offset", s.log_offset.to_string()),
                            ("rows", s.row_count.to_string()),
                            ("path", s.path.clone()),
                        ]),
                    },
                )
            }
        }
    }
}

fn describe(info: &proto::TableInfo) -> String {
    let d = &info.descriptor;
    let o = d.options();
    let list = |keys: &[String]| {
        if keys.is_empty() {
            "-".to_owned()
        } else {
            keys.join(", ")
        }
    };

    let mut text = pairs(&[
        ("table", info.path.to_string()),
        ("id", info.table_id.0.to_string()),
        ("comment", opt(&d.comment().map(str::to_owned))),
        ("created", when(info.created_ms)),
        ("modified", when(info.modified_ms)),
        (
            "primary key",
            d.schema()
                .primary_key()
                .map_or_else(|| "-".to_owned(), |k| k.columns().join(", ")),
        ),
        ("partition keys", list(d.partition_keys())),
        ("bucket keys", list(d.bucket_keys())),
        ("bucket count", opt(&d.bucket_count())),
        ("log format", o.log_format.to_string()),
        ("kv format", o.kv_format.to_string()),
        ("merge engine", opt(&o.merge_engine)),
        (
            "log ttl",
            o.log_ttl.map_or_else(
                || "forever".to_owned(),
                |ttl| humantime::format_duration(ttl).to_string(),
            ),
        ),
        ("lake", opt(&o.lake)),
        (
            "lake freshness",
            humantime::format_duration(o.lake_freshness).to_string(),
        ),
        (
            "lake auto-compaction",
            yes_no(o.lake_auto_compaction).to_owned(),
        ),
        ("lake attached", yes_no(o.lake_attach).to_owned()),
        ("schemas", info.schemas.len().to_string()),
    ]);

    text.push_str("\n\ncolumns:\n");
    let rows: Vec<Vec<String>> = d
        .schema()
        .columns()
        .iter()
        .map(|c| {
            vec![
                c.name().to_owned(),
                c.data_type().to_string(),
                c.description().unwrap_or("").to_owned(),
            ]
        })
        .collect();
    text.push_str(&table(&["name", "type", "comment"], &rows));

    if !d.custom().is_empty() {
        text.push_str("\n\nproperties:\n");
        text.push_str(
            &d.custom()
                .iter()
                .map(|(k, v)| format!("{k} = {v}"))
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }

    if !info.buckets.is_empty() {
        text.push_str("\n\nbuckets:\n");
        let rows: Vec<Vec<String>> = info
            .buckets
            .iter()
            .map(|b| {
                vec![
                    buckets::text(b.bucket),
                    b.leader.as_ref().map_or_else(
                        || "-".to_owned(),
                        |l| format!("node {} at {}", l.node_id, l.address),
                    ),
                ]
            })
            .collect();
        text.push_str(&table(&["bucket", "leader"], &rows));
    }

    text
}
