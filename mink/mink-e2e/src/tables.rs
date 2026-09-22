//! The two shapes every scenario works with: an `events` log table and a `users` primary key
//! table, plus the Arrow batches that feed them.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use mink_client::{Admin, Cluster, Table};
use mink_table::{
    Column, Descriptor, DescriptorBuilder, LakeFormat, Options, Path, PrimaryKey, Schema,
};
use mink_types::DataType;

pub const LAKE_FRESHNESS: Duration = Duration::from_secs(1);

pub fn events_schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .build()
        .unwrap()
}

pub fn users_schema() -> Schema {
    Schema::builder()
        .column(Column::new("id", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("name", DataType::string()).unwrap())
        .column(Column::new("score", DataType::int()).unwrap())
        .primary_key(PrimaryKey::new(vec!["id".into()]).unwrap())
        .build()
        .unwrap()
}

pub fn partitioned_events_schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .column(Column::new("region", DataType::string().with_nullable(false)).unwrap())
        .build()
        .unwrap()
}

pub fn lake_options() -> Options {
    Options {
        lake: Some(LakeFormat::Iceberg),
        lake_freshness: LAKE_FRESHNESS,
        ..Options::default()
    }
}

pub fn events_descriptor(buckets: u32, lake: bool) -> DescriptorBuilder {
    let mut builder = Descriptor::builder(events_schema())
        .bucket_keys(["k"])
        .bucket_count(buckets);
    if lake {
        builder = builder.options(lake_options());
    }
    builder
}

pub fn users_descriptor(buckets: u32, lake: bool) -> DescriptorBuilder {
    let mut builder = Descriptor::builder(users_schema()).bucket_count(buckets);
    if lake {
        builder = builder.options(lake_options());
    }
    builder
}

pub fn events(rows: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(events_schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
        ],
    )
    .unwrap()
}

pub fn events_range(from: i64, to: i64, tag: &str) -> RecordBatch {
    let values: Vec<String> = (from..to).map(|k| format!("{tag}-{k}")).collect();
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(events_schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(from..to)),
            Arc::new(StringArray::from_iter_values(
                values.iter().map(String::as_str),
            )),
        ],
    )
    .unwrap()
}

pub fn partitioned_events(rows: &[(i64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(
            partitioned_events_schema().fields(),
        )),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

pub fn users(rows: &[(i64, &str, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(users_schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

pub fn users_range(from: i64, to: i64) -> RecordBatch {
    let names: Vec<String> = (from..to).map(|id| format!("user-{id}")).collect();
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(users_schema().fields())),
        vec![
            Arc::new(Int64Array::from_iter_values(from..to)),
            Arc::new(StringArray::from_iter_values(
                names.iter().map(String::as_str),
            )),
            Arc::new(Int32Array::from_iter_values(
                (from..to).map(|id| (id % 100) as i32),
            )),
        ],
    )
    .unwrap()
}

pub fn user_keys(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from_iter_values(ids.iter().copied())) as ArrayRef],
    )
    .unwrap()
}

pub async fn create(
    admin: &Admin,
    cluster: &Cluster,
    path: &Path,
    descriptor: &Descriptor,
) -> Table {
    admin
        .create_table(path, descriptor, false)
        .await
        .unwrap_or_else(|e| panic!("create {path}: {e}"));
    crate::wait_for(&format!("{path} buckets led"), async || {
        let info = cluster.admin().get_table(path).await?;
        Ok(info
            .buckets
            .iter()
            .all(|b| b.leader.is_some())
            .then_some(()))
    })
    .await;
    cluster.table(path).await.unwrap()
}
