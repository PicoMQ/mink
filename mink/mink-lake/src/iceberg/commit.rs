//! Produces Iceberg snapshots for row deltas and file rewrites, and commits them with conflict retries.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;
use iceberg::spec::{
    DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestListWriter, ManifestWriter,
    ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention,
    SnapshotSummaryCollector, Struct, Summary,
};
use iceberg::table::Table;
use iceberg::{TableIdent, TableRequirement, TableUpdate};
use uuid::Uuid;

use crate::error::{Error, Result};

const MAIN_BRANCH: &str = "main";
pub(crate) const METADATA_DIR: &str = "metadata";
const TOTALS: [(&str, &str, Option<&str>); 6] = [
    (
        "total-data-files",
        "added-data-files",
        Some("deleted-data-files"),
    ),
    (
        "total-delete-files",
        "added-delete-files",
        Some("removed-delete-files"),
    ),
    ("total-records", "added-records", Some("deleted-records")),
    (
        "total-files-size",
        "added-files-size",
        Some("removed-files-size"),
    ),
    (
        "total-position-deletes",
        "added-position-deletes",
        Some("removed-position-deletes"),
    ),
    (
        "total-equality-deletes",
        "added-equality-deletes",
        Some("removed-equality-deletes"),
    ),
];

#[async_trait]
pub trait CommitTarget: Send + Sync {
    async fn load(&self, ident: &TableIdent) -> Result<Table>;

    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<Table>;
}

#[derive(Debug, Default, Clone)]
pub struct RowDelta {
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug)]
pub struct Produced {
    pub snapshot_id: i64,
    pub updates: Vec<TableUpdate>,
    pub requirements: Vec<TableRequirement>,
}

pub async fn produce(table: &Table, delta: &RowDelta) -> Result<Produced> {
    let metadata = table.metadata();
    if metadata.format_version() == FormatVersion::V1 {
        return Err(Error::invalid(
            "row deltas need Iceberg format version 2 or later",
        ));
    }
    if delta.data_files.is_empty() && delta.delete_files.is_empty() {
        return Err(Error::invalid("nothing to commit: no data or delete files"));
    }
    let partition_type = metadata.default_partition_type();
    for file in delta.data_files.iter().chain(&delta.delete_files) {
        if file.partition().fields().len() != partition_type.fields().len() {
            return Err(Error::invalid(format!(
                "file {} has a {}-field partition tuple but the table's default spec has {}",
                file.file_path(),
                file.partition().fields().len(),
                partition_type.fields().len()
            )));
        }
    }

    let mut producer = Producer::new(table);
    let mut manifests = current_manifests(table).await?;
    if !delta.data_files.is_empty() {
        manifests.push(
            producer
                .added_manifest(ManifestContentType::Data, &delta.data_files)
                .await?,
        );
    }
    if !delta.delete_files.is_empty() {
        manifests.push(
            producer
                .added_manifest(ManifestContentType::Deletes, &delta.delete_files)
                .await?,
        );
    }

    let operation = if delta.delete_files.is_empty() {
        Operation::Append
    } else {
        Operation::Overwrite
    };
    let summary = summary(
        table,
        operation,
        delta.data_files.iter().chain(&delta.delete_files),
        [].iter(),
        &delta.properties,
    );
    producer.finish(manifests, summary).await
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Rewrite {
    pub added: Vec<DataFile>,
    pub deleted: Vec<DataFile>,
    pub from_snapshot_id: i64,
    pub properties: HashMap<String, String>,
}

pub(crate) async fn produce_rewrite(table: &Table, rewrite: &Rewrite) -> Result<Produced> {
    let metadata = table.metadata();
    if metadata.format_version() == FormatVersion::V1 {
        return Err(Error::invalid(
            "rewrites need Iceberg format version 2 or later",
        ));
    }
    if rewrite.deleted.is_empty() {
        return Err(Error::invalid("a rewrite must replace at least one file"));
    }
    let Some(current) = metadata.current_snapshot() else {
        return Err(Error::CommitConflict(
            "the table has no snapshot to rewrite".into(),
        ));
    };

    let mut newer: HashSet<i64> = HashSet::new();
    let mut cursor = Some(current.clone());
    loop {
        match cursor {
            Some(snapshot) if snapshot.snapshot_id() == rewrite.from_snapshot_id => break,
            Some(snapshot) => {
                newer.insert(snapshot.snapshot_id());
                cursor = snapshot
                    .parent_snapshot_id()
                    .and_then(|id| metadata.snapshot_by_id(id).cloned());
            }
            None => {
                return Err(Error::CommitConflict(format!(
                    "snapshot {} the rewrite was planned on is not an ancestor of the current snapshot {}",
                    rewrite.from_snapshot_id,
                    current.snapshot_id()
                )));
            }
        }
    }

    let replaced: HashSet<&str> = rewrite.deleted.iter().map(|f| f.file_path()).collect();
    let partitions: Vec<&Struct> = rewrite.deleted.iter().map(|f| f.partition()).collect();
    let mut producer = Producer::new(table);
    let mut manifests = Vec::new();
    let mut found: HashSet<String> = HashSet::new();
    for manifest_file in current_manifests(table).await? {
        match manifest_file.content {
            ManifestContentType::Deletes => {
                if newer.contains(&manifest_file.added_snapshot_id) {
                    let manifest = manifest_file.load_manifest(table.file_io()).await?;
                    if let Some(entry) = manifest
                        .entries()
                        .iter()
                        .find(|e| e.is_alive() && partitions.contains(&e.data_file().partition()))
                    {
                        return Err(Error::CommitConflict(format!(
                            "delete file {} was added to a rewritten partition after snapshot {}",
                            entry.data_file().file_path(),
                            rewrite.from_snapshot_id
                        )));
                    }
                }
                manifests.push(manifest_file);
            }
            ManifestContentType::Data => {
                let manifest = manifest_file.load_manifest(table.file_io()).await?;
                if !manifest
                    .entries()
                    .iter()
                    .any(|e| e.is_alive() && replaced.contains(e.data_file().file_path()))
                {
                    manifests.push(manifest_file);
                    continue;
                }
                let mut writer = producer.manifest_writer(ManifestContentType::Data)?;
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let file = entry.data_file();
                    let sequence_number = entry.sequence_number().unwrap_or(0);
                    if replaced.contains(file.file_path()) {
                        found.insert(file.file_path().to_string());
                        writer.add_delete_file(
                            file.clone(),
                            sequence_number,
                            entry.file_sequence_number,
                        )?;
                    } else {
                        writer.add_existing_file(
                            file.clone(),
                            entry
                                .snapshot_id()
                                .unwrap_or(manifest_file.added_snapshot_id),
                            sequence_number,
                            entry.file_sequence_number,
                        )?;
                    }
                }
                manifests.push(writer.write_manifest_file().await?);
            }
        }
    }
    if let Some(missing) = replaced.iter().find(|p| !found.contains(**p)) {
        return Err(Error::CommitConflict(format!(
            "rewritten file {missing} is no longer in the table"
        )));
    }
    if !rewrite.added.is_empty() {
        manifests.push(
            producer
                .added_manifest(ManifestContentType::Data, &rewrite.added)
                .await?,
        );
    }

    let summary = summary(
        table,
        Operation::Replace,
        rewrite.added.iter(),
        rewrite.deleted.iter(),
        &rewrite.properties,
    );
    producer.finish(manifests, summary).await
}

async fn current_manifests(table: &Table) -> Result<Vec<ManifestFile>> {
    Ok(match table.metadata().current_snapshot() {
        Some(snapshot) => table
            .manifest_list_reader(snapshot)
            .load()
            .await?
            .entries()
            .iter()
            .filter(|m| m.has_added_files() || m.has_existing_files() || m.has_deleted_files())
            .cloned()
            .collect(),
        None => Vec::new(),
    })
}

struct Producer<'a> {
    table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    sequence_number: i64,
    parent: Option<i64>,
    manifest_counter: usize,
}

impl<'a> Producer<'a> {
    fn new(table: &'a Table) -> Self {
        let metadata = table.metadata();
        Producer {
            table,
            snapshot_id: unique_snapshot_id(table),
            commit_uuid: Uuid::now_v7(),
            sequence_number: metadata.next_sequence_number(),
            parent: metadata.current_snapshot_id(),
            manifest_counter: 0,
        }
    }

    fn manifest_writer(&mut self, content: ManifestContentType) -> Result<ManifestWriter> {
        let metadata = self.table.metadata();
        let path = format!(
            "{}/{METADATA_DIR}/{}-m{}.avro",
            metadata.location(),
            self.commit_uuid,
            self.manifest_counter
        );
        self.manifest_counter += 1;

        let builder = ManifestWriterBuilder::new(
            self.table.file_io().new_output(path)?,
            Some(self.snapshot_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        );
        Ok(match (metadata.format_version(), content) {
            (FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
            (FormatVersion::V2, ManifestContentType::Deletes) => builder.build_v2_deletes(),
            (_, ManifestContentType::Data) => builder.build_v3_data(),
            (_, ManifestContentType::Deletes) => builder.build_v3_deletes(),
        })
    }

    async fn added_manifest(
        &mut self,
        content: ManifestContentType,
        files: &[DataFile],
    ) -> Result<ManifestFile> {
        let mut writer = self.manifest_writer(content)?;
        for file in files {
            writer.add_file(file.clone(), self.sequence_number)?;
        }

        Ok(writer.write_manifest_file().await?)
    }

    async fn finish(self, manifests: Vec<ManifestFile>, summary: Summary) -> Result<Produced> {
        let metadata = self.table.metadata();
        let manifest_list_path = format!(
            "{}/{METADATA_DIR}/snap-{}-0-{}.avro",
            metadata.location(),
            self.snapshot_id,
            self.commit_uuid
        );
        let output = self
            .table
            .file_io()
            .new_output(manifest_list_path.clone())?;
        let mut list = match metadata.format_version() {
            FormatVersion::V1 => unreachable!("rejected by the producers"),
            FormatVersion::V2 => ManifestListWriter::v2(
                output.writer().await?,
                self.snapshot_id,
                self.parent,
                self.sequence_number,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                output.writer().await?,
                self.snapshot_id,
                self.parent,
                self.sequence_number,
                Some(metadata.next_row_id()),
            ),
        };
        list.add_manifests(manifests.into_iter())?;
        let row_range = list
            .next_row_id()
            .map(|next| (metadata.next_row_id(), next - metadata.next_row_id()));
        list.close().await?;

        let snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.parent)
            .with_sequence_number(self.sequence_number)
            .with_summary(summary)
            .with_schema_id(metadata.current_schema_id())
            .with_timestamp_ms(Utc::now().timestamp_millis());
        let snapshot = match row_range {
            Some((first_row_id, added_rows)) => {
                snapshot.with_row_range(first_row_id, added_rows).build()
            }
            None => snapshot.build(),
        };

        Ok(Produced {
            snapshot_id: self.snapshot_id,
            updates: vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference::new(
                        self.snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ],
            requirements: vec![
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: self.parent,
                },
            ],
        })
    }
}

fn summary<'f>(
    table: &Table,
    operation: Operation,
    added: impl Iterator<Item = &'f DataFile>,
    removed: impl Iterator<Item = &'f DataFile>,
    extra: &HashMap<String, String>,
) -> Summary {
    let metadata = table.metadata();
    let mut collector = SnapshotSummaryCollector::default();
    for file in added {
        collector.add_file(
            file,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
        );
    }
    for file in removed {
        collector.remove_file(
            file,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
        );
    }

    let mut properties = extra.clone();
    properties.extend(collector.build());
    let previous = metadata.current_snapshot().map(|s| s.summary());
    let count = |props: &HashMap<String, String>, key: &str| {
        props
            .get(key)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    for (total, added, removed) in TOTALS {
        let before = previous.map_or(0, |s| count(&s.additional_properties, total));
        let now = before + count(&properties, added);
        let now = removed.map_or(now, |removed| {
            now.saturating_sub(count(&properties, removed))
        });
        properties.insert(total.to_string(), now.to_string());
    }

    Summary {
        operation,
        additional_properties: properties,
    }
}

fn unique_snapshot_id(table: &Table) -> i64 {
    loop {
        let (hi, lo) = Uuid::new_v4().as_u64_pair();
        let id = ((hi ^ lo) >> 1) as i64;
        if id > 0 && table.metadata().snapshot_by_id(id).is_none() {
            return id;
        }
    }
}

pub async fn commit(
    target: &dyn CommitTarget,
    ident: &TableIdent,
    delta: &RowDelta,
) -> Result<(Table, i64)> {
    commit_with(target, ident, Plan::Delta(delta)).await
}

pub(crate) async fn commit_rewrite(
    target: &dyn CommitTarget,
    ident: &TableIdent,
    rewrite: &Rewrite,
) -> Result<(Table, i64)> {
    commit_with(target, ident, Plan::Rewrite(rewrite)).await
}

enum Plan<'a> {
    Delta(&'a RowDelta),
    Rewrite(&'a Rewrite),
}

impl Plan<'_> {
    async fn produce(&self, table: &Table) -> Result<Produced> {
        match self {
            Plan::Delta(delta) => produce(table, delta).await,
            Plan::Rewrite(rewrite) => produce_rewrite(table, rewrite).await,
        }
    }
}

async fn commit_with(
    target: &dyn CommitTarget,
    ident: &TableIdent,
    plan: Plan<'_>,
) -> Result<(Table, i64)> {
    const ATTEMPTS: usize = 4;
    let mut table = target.load(ident).await?;
    for attempt in 1..=ATTEMPTS {
        let produced = plan.produce(&table).await?;
        match target
            .commit(ident, produced.requirements, produced.updates)
            .await
        {
            Ok(table) => return Ok((table, produced.snapshot_id)),
            Err(Error::CommitConflict(reason)) if attempt < ATTEMPTS => {
                tracing::info!(
                    table = %ident,
                    attempt,
                    %reason,
                    "iceberg commit conflicted; reloading and retrying"
                );
                table = target.load(ident).await?;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("loop returns on the last attempt")
}
