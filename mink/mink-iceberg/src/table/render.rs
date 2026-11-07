use iceberg::spec::{PartitionSpec, Schema, Snapshot as IcebergSnapshot, SortOrder};
use mink_schema::binary_schema;
use serde_json::{Map, Value, json};

use super::commit::Commit;
use super::external::ExternalSpecs;
use super::specs::partition_specs;
use super::{FormatVersion, Table};
use crate::Error;

impl Table {
    pub(super) fn render(&self, commit: &Commit<'_>) -> Result<Vec<u8>, Error> {
        let latest = self
            .snapshots
            .last()
            .expect("a rendered table has the snapshot of its commit");
        let schemas: Vec<&Schema> = [binary_schema()]
            .into_iter()
            .chain(commit.schemas)
            .collect();
        let declared: Vec<&ExternalSpecs> = self.external_specs.values().collect();
        let mut specs = partition_specs(commit.specs)?;
        specs.extend(
            declared
                .iter()
                .flat_map(|declared| declared.partition_specs())
                .cloned(),
        );
        let last_partition_id = specs
            .iter()
            .filter_map(PartitionSpec::highest_field_id)
            .max();
        let mut sort_orders = vec![SortOrder::unsorted_order()];
        sort_orders.extend(
            declared
                .last()
                .into_iter()
                .flat_map(|declared| declared.sort_orders())
                .cloned(),
        );

        let metadata = match self.version {
            FormatVersion::V2 => json!({
                "format-version": self.version.number(),
                "table-uuid": self.uuid,
                "location": self.location().path(),
                "last-sequence-number": latest.sequence_number(),
                "last-updated-ms": latest.timestamp_ms(),
                "last-column-id": schemas.iter().map(|schema| schema.highest_field_id()).max(),
                "current-schema-id": commit.current_schema().schema_id(),
                "schemas": schemas,
                "default-spec-id": commit.specs.current().id(),
                "partition-specs": specs,
                "last-partition-id": last_partition_id,
                "default-sort-order-id": SortOrder::unsorted_order().order_id,
                "sort-orders": sort_orders,
                "current-snapshot-id": latest.snapshot_id(),
                "refs": { "main": { "snapshot-id": latest.snapshot_id(), "type": "branch" } },
                "snapshots": self.snapshots.iter().map(render_snapshot).collect::<Vec<_>>(),
                "snapshot-log": self.snapshots.iter().map(|snapshot| json!({
                    "snapshot-id": snapshot.snapshot_id(),
                    "timestamp-ms": snapshot.timestamp_ms(),
                })).collect::<Vec<_>>(),
                "metadata-log": &self.metadata_log[..self.metadata_log.len() - 1],
                "statistics": self.statistics.iter().collect::<Vec<_>>(),
            }),
        };

        Ok(serde_json::to_vec_pretty(&metadata)?)
    }
}

fn render_snapshot(snapshot: &IcebergSnapshot) -> Value {
    let summary = snapshot.summary();
    let mut rendered: Map<String, Value> = summary
        .additional_properties
        .iter()
        .map(|(key, value)| (key.clone(), Value::from(value.as_str())))
        .collect();
    rendered.insert("operation".to_owned(), json!(summary.operation));

    let mut rendered_snapshot = json!({
        "snapshot-id": snapshot.snapshot_id(),
        "sequence-number": snapshot.sequence_number(),
        "timestamp-ms": snapshot.timestamp_ms(),
        "manifest-list": snapshot.manifest_list(),
        "summary": rendered,
        "schema-id": snapshot.schema_id(),
    });
    if let Some(parent) = snapshot.parent_snapshot_id() {
        rendered_snapshot["parent-snapshot-id"] = json!(parent);
    }

    rendered_snapshot
}
