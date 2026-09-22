//! Reading a table back through every path a client has: the hot log, the KV snapshot, lookups,
//! and the hot + cold union.

use std::collections::BTreeMap;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use futures::TryStreamExt;
use mink_client::{Cluster, Table, proto};
use mink_table::{Bucket, Path};

pub type Event = (i64, String);
pub type User = (String, i32);

pub fn event_rows(batch: &RecordBatch) -> Vec<Event> {
    let k = batch
        .column_by_name("k")
        .expect("k column")
        .as_primitive::<Int64Type>();
    let v = batch
        .column_by_name("v")
        .expect("v column")
        .as_string::<i32>();
    (0..batch.num_rows())
        .map(|i| (k.value(i), v.value(i).to_owned()))
        .collect()
}

pub fn keys(batch: &RecordBatch, column: &str) -> Vec<i64> {
    let k = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("{column} column in {:?}", batch.schema()))
        .as_primitive::<Int64Type>();
    (0..batch.num_rows()).map(|i| k.value(i)).collect()
}

pub fn user_rows(batch: &RecordBatch) -> BTreeMap<i64, User> {
    let id = batch
        .column_by_name("id")
        .expect("id column")
        .as_primitive::<Int64Type>();
    let name = batch
        .column_by_name("name")
        .expect("name column")
        .as_string::<i32>();
    let score = batch
        .column_by_name("score")
        .expect("score column")
        .as_primitive::<Int32Type>();
    (0..batch.num_rows())
        .map(|i| {
            let score = if score.is_valid(i) { score.value(i) } else { 0 };
            (id.value(i), (name.value(i).to_owned(), score))
        })
        .collect()
}

pub fn user(name: &str, score: i32) -> User {
    (name.to_owned(), score)
}

pub fn count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

pub async fn offsets(table: &Table) -> BTreeMap<Bucket, (i64, i64)> {
    let mut out = BTreeMap::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        out.insert(bucket, table.offsets(bucket).await.unwrap());
    }
    out
}

pub async fn high_watermarks(table: &Table) -> BTreeMap<Bucket, i64> {
    offsets(table)
        .await
        .into_iter()
        .map(|(b, (_, hw))| (b, hw))
        .collect()
}

pub async fn scan_all(table: &Table) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        let (start, end) = table.offsets(bucket).await.unwrap();
        let batches: Vec<_> = table
            .scan(bucket, start, end, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        out.extend(batches.into_iter().map(|b| b.rows));
    }
    out
}

pub async fn scan_events(table: &Table) -> Vec<Event> {
    let mut rows: Vec<Event> = scan_all(table).await.iter().flat_map(event_rows).collect();
    rows.sort();
    rows
}

pub async fn union_by_bucket(table: &Table) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        let batches: Vec<RecordBatch> = crate::retry(&format!("union {bucket:?}"), || async {
            table.union(bucket, None).await?.try_collect().await
        })
        .await;
        out.extend(batches);
    }
    out
}

pub async fn union_all(table: &Table) -> Vec<RecordBatch> {
    crate::retry(&format!("union_all {}", table.path()), || async {
        table.union_all(None).await?.try_collect().await
    })
    .await
}

pub async fn union_events(table: &Table) -> Vec<Event> {
    let mut rows: Vec<Event> = union_all(table).await.iter().flat_map(event_rows).collect();
    rows.sort();
    rows
}

pub async fn union_users(table: &Table) -> BTreeMap<i64, User> {
    union_by_bucket(table)
        .await
        .iter()
        .flat_map(user_rows)
        .collect()
}

pub async fn snapshot_all(table: &Table) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        let batches: Vec<RecordBatch> = crate::retry(&format!("snapshot {bucket:?}"), || async {
            table
                .snapshot(bucket, None)
                .await?
                .batches
                .try_collect()
                .await
        })
        .await;
        out.extend(batches);
    }
    out
}

pub async fn snapshot_users(table: &Table) -> BTreeMap<i64, User> {
    let mut out = BTreeMap::new();
    for bucket in table.buckets().collect::<Vec<_>>() {
        let snapshot = table.snapshot(bucket, None).await.unwrap();
        let batches: Vec<RecordBatch> = snapshot.batches.try_collect().await.unwrap();
        out.extend(batches.iter().flat_map(user_rows));
    }
    out
}

pub async fn lake_snapshot(cluster: &Cluster, path: &Path) -> Option<proto::LakeSnapshot> {
    cluster.admin().lake_snapshot(path).await.unwrap()
}

pub async fn wait_tiered(
    cluster: &Cluster,
    path: &Path,
    watermarks: &BTreeMap<Bucket, i64>,
) -> proto::LakeSnapshot {
    let admin = cluster.admin();
    crate::wait_for(&format!("{path} tiered to {watermarks:?}"), async || {
        let Some(snapshot) = admin.lake_snapshot(path).await? else {
            return Ok(None);
        };
        let covered: BTreeMap<_, _> = snapshot.bucket_log_end_offset.iter().copied().collect();
        Ok(watermarks
            .iter()
            .all(|(b, hw)| *hw <= 0 || covered.get(b).is_some_and(|end| end >= hw))
            .then_some(snapshot))
    })
    .await
}

pub async fn wait_tiered_to_head(cluster: &Cluster, table: &Table) -> proto::LakeSnapshot {
    let watermarks = high_watermarks(table).await;
    wait_tiered(cluster, table.path(), &watermarks).await
}
