use iceberg::spec::{ManifestFile, Snapshot as IcebergSnapshot};
use mink_core::View;
use uuid::Uuid;

use super::Table;
use super::commit::{Commit, Counts, summary};
use super::manifests::Manifest;

impl Table {
    pub(super) fn uuid_of(&self, view: View) -> Uuid {
        match view {
            View::Merged => self.uuid,
            other => Uuid::new_v5(&self.uuid, other.name().as_bytes()),
        }
    }

    pub(super) fn listed_by(&self, view: View, manifests: &[Manifest]) -> Vec<ManifestFile> {
        let own = manifests.iter().map(|manifest| manifest.file.clone());
        let external = self
            .external_snapshot
            .iter()
            .flat_map(|external| external.manifests())
            .cloned();

        match view {
            View::Merged => own.chain(external).collect(),
            View::Stream => own.collect(),
            View::Lake => external.collect(),
        }
    }

    pub(super) fn snapshot_of(
        &self,
        view: View,
        commit: &Commit<'_>,
        additions: &Counts,
        deletions: &Counts,
        totals: &Counts,
    ) -> IcebergSnapshot {
        let none = Counts::default();
        let external = self.external_snapshot.as_ref();
        let summary = match view {
            View::Merged => summary(&commit.offsets, additions, deletions, totals, external),
            View::Stream => summary(&commit.offsets, additions, deletions, totals, None),
            View::Lake => summary(&[], &none, &none, &none, external),
        };

        IcebergSnapshot::builder()
            .with_snapshot_id(commit.snapshot_id)
            .with_parent_snapshot_id(self.snapshot_id())
            .with_sequence_number(commit.sequence_number)
            .with_timestamp_ms(commit.timestamp_ms)
            .with_manifest_list(self.location().manifest_list(&commit.key, view))
            .with_summary(summary)
            .with_schema_id(commit.current_schema().schema_id())
            .build()
    }
}
