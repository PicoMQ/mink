//! Commits written data, delete and rewritten files as one Iceberg snapshot tagged with the tiering user.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::TableIdent;
use iceberg::spec::{DataFile, SnapshotRef};
use iceberg::table::Table;
use mink_table::Path;

use crate::committer::{CommitResult, CommittedSnapshot};
use crate::error::{Error, Result};
use crate::iceberg::catalog::Catalog;
use crate::iceberg::commit::{self, CommitTarget, Rewrite, RowDelta};
use crate::iceberg::compact::RewriteResult;
use crate::iceberg::write::{Committable, WriteResult};
use crate::writer::COMMIT_USER;

pub const COMMIT_USER_PROPERTY: &str = "commit-user";

pub(crate) struct Committer {
    target: Arc<dyn CommitTarget>,
    ident: TableIdent,
    path: Path,
    table: Table,
}

impl Committer {
    pub(crate) async fn open(target: Arc<dyn CommitTarget>, path: &Path) -> Result<Self> {
        let ident = Catalog::identifier(path);
        let table = target
            .load(&ident)
            .await
            .map_err(|e| Error::Other(format!("Failed to get table {path} in Iceberg: {e}")))?;

        Ok(Committer {
            target,
            ident,
            path: path.clone(),
            table,
        })
    }

    async fn refresh(&mut self) -> Result<()> {
        self.table = self.target.load(&self.ident).await?;

        Ok(())
    }

    async fn delete_files<'f>(&self, files: impl Iterator<Item = &'f DataFile>) {
        let io = self.table.file_io();
        for file in files {
            if let Err(e) = io.delete(file.file_path()).await {
                tracing::warn!(path = file.file_path(), error = %e, "failed to delete lake file");
            }
        }
    }

    async fn commit_rewrites(
        &mut self,
        rewrites: Vec<RewriteResult>,
        properties: HashMap<String, String>,
    ) -> Result<Option<i64>> {
        let from_snapshot_id = rewrites[0].snapshot_id;
        let rewrite = Rewrite {
            added: rewrites.iter().flat_map(|r| r.added.clone()).collect(),
            deleted: rewrites.iter().flat_map(|r| r.deleted.clone()).collect(),
            from_snapshot_id,
            properties,
        };
        let result = if rewrites.iter().any(|r| r.snapshot_id != from_snapshot_id) {
            Err(Error::invalid(
                "Rewrite data file results must have same snapshot id.",
            ))
        } else {
            commit::commit_rewrite(self.target.as_ref(), &self.ident, &rewrite).await
        };

        match result {
            Ok((table, snapshot_id)) => {
                self.table = table;
                tracing::info!(
                    table = %self.path,
                    snapshot_id,
                    replaced = rewrite.deleted.len(),
                    with = rewrite.added.len(),
                    "committed iceberg rewrite snapshot"
                );
                Ok(Some(snapshot_id))
            }
            Err(e) => {
                tracing::warn!(
                    table = %self.path,
                    error = %e,
                    "Failed to commit rewrite files to iceberg, delete rewrite added files"
                );
                self.delete_files(rewrite.added.iter()).await;

                Err(e)
            }
        }
    }

    fn latest(&self) -> Option<&SnapshotRef> {
        self.table
            .metadata()
            .snapshots()
            .filter(|s| {
                s.summary()
                    .additional_properties
                    .get(COMMIT_USER_PROPERTY)
                    .map(String::as_str)
                    == Some(COMMIT_USER)
            })
            .max_by_key(|s| s.sequence_number())
    }
}

#[async_trait]
impl crate::committer::Committer<WriteResult, Committable> for Committer {
    async fn to_committable(&mut self, results: Vec<WriteResult>) -> Result<Committable> {
        Ok(Committable::from_results(results))
    }

    fn is_empty(&self, committable: &Committable) -> bool {
        committable.is_empty()
    }

    async fn commit(
        &mut self,
        committable: Committable,
        properties: BTreeMap<String, String>,
    ) -> Result<CommitResult> {
        if committable.is_empty() {
            return Err(Error::invalid(format!(
                "nothing to commit to Iceberg table {}",
                self.path
            )));
        }

        let mut snapshot_properties: HashMap<String, String> = properties.into_iter().collect();
        snapshot_properties.insert(COMMIT_USER_PROPERTY.to_string(), COMMIT_USER.to_string());

        let mut snapshot_id = None;
        if !committable.data_files.is_empty() || !committable.delete_files.is_empty() {
            let delta = RowDelta {
                data_files: committable.data_files,
                delete_files: committable.delete_files,
                properties: snapshot_properties.clone(),
            };
            let (table, id) = commit::commit(self.target.as_ref(), &self.ident, &delta)
                .await
                .map_err(|e| Error::Other(format!("Failed to commit to Iceberg table: {e}")))?;
            self.table = table;
            tracing::info!(table = %self.path, snapshot_id = id, "committed iceberg snapshot");
            snapshot_id = Some(id);
        }
        if !committable.rewrites.is_empty() {
            match self
                .commit_rewrites(committable.rewrites, snapshot_properties)
                .await
            {
                Ok(Some(id)) => snapshot_id = Some(id),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(table = %self.path, error = %e, "iceberg rewrite commit failed")
                }
            }
        }

        snapshot_id.map(CommitResult::readable).ok_or_else(|| {
            Error::invalid(format!("nothing to commit to Iceberg table {}", self.path))
        })
    }

    async fn abort(&mut self, committable: Committable) -> Result<()> {
        let rewritten = committable.rewrites.iter().flat_map(|r| &r.added);
        let files = committable
            .data_files
            .iter()
            .chain(&committable.delete_files)
            .chain(rewritten);
        self.delete_files(files).await;

        Ok(())
    }

    async fn missing_snapshot(&mut self, known: Option<i64>) -> Result<Option<CommittedSnapshot>> {
        self.refresh().await?;
        let Some(latest) = self.latest() else {
            return Ok(None);
        };
        if let Some(known) = known {
            let known_snapshot = self.table.metadata().snapshot_by_id(known).ok_or_else(|| {
                Error::Other(format!(
                    "recorded lake snapshot {known} not found in Iceberg table"
                ))
            })?;
            if latest.sequence_number() <= known_snapshot.sequence_number() {
                return Ok(None);
            }
        }

        Ok(Some(CommittedSnapshot {
            snapshot_id: latest.snapshot_id(),
            properties: latest
                .summary()
                .additional_properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }))
    }
}
