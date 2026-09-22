//! Union reads over fake lake and log sources: log-only, lake-only, merged, projected and empty cases.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use mink_lake::{
    BucketSource, Error, LakeSplit, LogSource, Predicate, Reader, Result, ScanOptions,
    SnapshotRead, Split, TableInfo, Tasks, TieredBatch, take_rows,
};
use mink_read::{LakePosition, Options, Plan, Read};
use mink_record::{ChangeType, Changes};
use mink_table::{Bucket, BucketId, Column, Id, Path, PrimaryKey, Schema};
use mink_types::DataType;

fn schema(primary_key: bool) -> Schema {
    let mut builder = Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap());
    if primary_key {
        builder = builder.primary_key(PrimaryKey::new(vec!["k".into()]).unwrap());
    }
    builder.build().unwrap()
}

fn user_rows(kv: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("k", ArrowType::Int64, false),
            Field::new("v", ArrowType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(kv.iter().map(|(k, _)| *k))),
            Arc::new(StringArray::from_iter_values(kv.iter().map(|(_, v)| *v))),
        ],
    )
    .unwrap()
}

fn lake_rows(kv: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("k", ArrowType::Int64, false),
            Field::new("v", ArrowType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(kv.iter().map(|(k, _)| *k))),
            Arc::new(StringArray::from_iter_values(kv.iter().map(|(_, v)| *v))),
        ],
    )
    .unwrap()
}

fn appends(kv: &[(i64, &str)], base_offset: i64) -> TieredBatch {
    TieredBatch {
        changes: Changes::AppendOnly(kv.len()),
        rows: user_rows(kv),
        base_offset,
        timestamp_ms: 0,
    }
}

fn changes(kv: &[(ChangeType, i64, &str)], base_offset: i64) -> TieredBatch {
    let bytes: Vec<u8> = kv.iter().map(|(c, _, _)| c.byte()).collect();
    let rows: Vec<(i64, &str)> = kv.iter().map(|(_, k, v)| (*k, *v)).collect();
    TieredBatch {
        changes: Changes::vector(Bytes::from(bytes)).unwrap(),
        rows: user_rows(&rows),
        base_offset,
        timestamp_ms: 0,
    }
}

type LakeKey = (i64, Option<BucketId>);

#[derive(Default)]
struct FakeLake {
    rows: Mutex<BTreeMap<LakeKey, Vec<RecordBatch>>>,
    asked: Mutex<Vec<Option<Vec<usize>>>>,
    filters: Mutex<Vec<Option<Predicate>>>,
}

#[async_trait]
impl Reader for FakeLake {
    async fn user_schema(&self, _path: &Path) -> Result<SchemaRef> {
        Ok(user_rows(&[]).schema())
    }

    async fn plan(
        &self,
        _path: &Path,
        snapshot_id: i64,
        filter: Option<&Predicate>,
    ) -> Result<Vec<LakeSplit>> {
        self.filters.lock().unwrap().push(filter.cloned());
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .iter()
            .filter(|((snapshot, _), _)| *snapshot == snapshot_id)
            .map(|(key, batches)| Split {
                bucket: key.1,
                partition: None,
                files: batches.len(),
                rows: Some(batches.iter().map(|b| b.num_rows() as u64).sum()),
                bytes: 0,
                inner: Tasks::new(*key),
            })
            .collect())
    }

    async fn read(
        &self,
        split: LakeSplit,
        options: ScanOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        self.asked.lock().unwrap().push(options.projection.clone());
        let key: &LakeKey = split.inner.downcast()?;
        let batches = self
            .rows
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| Error::Other(format!("no snapshot {}", key.0)))?;
        let projected: Vec<RecordBatch> = batches
            .into_iter()
            .map(|b| match &options.projection {
                Some(columns) => b.project(columns).unwrap(),
                None => b,
            })
            .collect();
        let rows = futures::stream::iter(projected.into_iter().map(Ok));
        Ok(match options.limit {
            Some(limit) => Box::pin(take_rows(rows, limit)),
            None => Box::pin(rows),
        })
    }
}

#[derive(Default)]
struct FakeLog {
    batches: Mutex<BTreeMap<Bucket, Vec<TieredBatch>>>,
    asked: Mutex<Vec<Option<Vec<usize>>>>,
}

#[async_trait]
impl LogSource for FakeLog {
    async fn offsets(&self, bucket: Bucket) -> Result<(i64, i64)> {
        let all = self.batches.lock().unwrap();
        let end = all
            .get(&bucket)
            .and_then(|b| b.last())
            .map(|b| b.last_offset() + 1)
            .unwrap_or(0);
        Ok((0, end))
    }

    fn log(
        &self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
    ) -> BoxStream<'static, Result<TieredBatch>> {
        self.asked.lock().unwrap().push(columns.clone());
        let all = self.batches.lock().unwrap();
        let batches: Vec<TieredBatch> = all
            .get(&bucket)
            .map(|b| {
                b.iter()
                    .filter(|b| b.base_offset >= from && b.last_offset() < to)
                    .map(|b| TieredBatch {
                        rows: match &columns {
                            Some(columns) => b.rows.project(columns).unwrap(),
                            None => b.rows.clone(),
                        },
                        changes: b.changes.clone(),
                        base_offset: b.base_offset,
                        timestamp_ms: b.timestamp_ms,
                    })
                    .collect()
            })
            .unwrap_or_default();
        Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
    }
}

#[async_trait]
impl BucketSource for FakeLog {
    fn table(&self, _path: &Path) -> Result<Option<TableInfo>> {
        unimplemented!("the union read does not ask")
    }

    async fn snapshot(&self, _bucket: Bucket) -> Result<SnapshotRead> {
        unimplemented!("the union read does not ask")
    }
}

struct Harness {
    lake: Arc<FakeLake>,
    log: Arc<FakeLog>,
    read: Read,
    path: Path,
    bucket: Bucket,
}

impl Harness {
    fn new() -> Self {
        let lake = Arc::new(FakeLake::default());
        let log = Arc::new(FakeLog::default());
        Harness {
            read: Read::new(Some(lake.clone()), log.clone()),
            lake,
            log,
            path: "db.t".parse().unwrap(),
            bucket: Bucket::new(Id(1), BucketId(0)),
        }
    }

    fn lake(&self, snapshot_id: i64, batches: Vec<RecordBatch>) {
        self.lake
            .rows
            .lock()
            .unwrap()
            .insert((snapshot_id, Some(self.bucket.bucket())), batches);
    }

    fn whole_lake(&self, snapshot_id: i64, batches: Vec<RecordBatch>) {
        self.lake
            .rows
            .lock()
            .unwrap()
            .insert((snapshot_id, None), batches);
    }

    fn log(&self, batches: Vec<TieredBatch>) {
        self.log
            .batches
            .lock()
            .unwrap()
            .insert(self.bucket, batches);
    }

    async fn split(&self, snapshot_id: i64, bucket: Option<BucketId>) -> Option<LakeSplit> {
        self.read
            .plan_lake(&self.path, snapshot_id, None)
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.bucket == bucket)
    }

    async fn plan(&self, lake: Option<LakePosition>) -> Plan {
        let split = match lake {
            Some(position) => {
                self.split(position.snapshot_id, Some(self.bucket.bucket()))
                    .await
            }
            None => None,
        };
        self.read
            .plan(self.bucket, None, lake, split)
            .await
            .unwrap()
    }

    async fn rows(&self, schema: &Schema, plan: Plan, options: Options) -> Vec<RecordBatch> {
        self.read
            .read(schema, plan, &options)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }
}

fn projected(columns: Vec<usize>) -> Options {
    Options {
        projection: Some(columns),
        limit: None,
    }
}

fn kv(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    batches
        .iter()
        .flat_map(|b| {
            let k = b.column(0).as_primitive::<Int64Type>();
            let v = b.column(1).as_string::<i32>();
            (0..b.num_rows()).map(move |i| (k.value(i), v.value(i).to_string()))
        })
        .collect()
}

#[tokio::test]
async fn log_table_reads_lake_then_log_from_the_lake_offset() {
    let h = Harness::new();
    h.lake(7, vec![lake_rows(&[(1, "a"), (2, "b")])]);
    h.log(vec![
        appends(&[(1, "a"), (2, "b")], 0),
        appends(&[(3, "c")], 2),
        appends(&[(4, "d")], 3),
    ]);
    let position = LakePosition {
        snapshot_id: 7,
        log_end_offset: 2,
    };
    let plan = h.plan(Some(position)).await;
    assert_eq!(
        (plan.bucket, plan.lake, plan.log_from, plan.log_to),
        (h.bucket, Some(position), 2, 4)
    );
    assert_eq!(plan.split.as_ref().map(|s| s.files), Some(1));
    let out = h.rows(&schema(false), plan, Options::default()).await;
    assert_eq!(
        kv(&out),
        vec![
            (1, "a".into()),
            (2, "b".into()),
            (3, "c".into()),
            (4, "d".into())
        ]
    );
    assert!(out[0].schema().field(1).is_nullable());
    assert_eq!(out[0].schema().fields().len(), 2);
}

#[tokio::test]
async fn a_bucket_without_lake_files_reads_only_its_tail() {
    let h = Harness::new();
    h.whole_lake(7, vec![lake_rows(&[(1, "a")])]);
    h.log(vec![appends(&[(1, "a")], 0), appends(&[(2, "b")], 1)]);
    let plan = h
        .plan(Some(LakePosition {
            snapshot_id: 7,
            log_end_offset: 1,
        }))
        .await;
    assert!(plan.split.is_none());
    let out = h.rows(&schema(false), plan, Options::default()).await;
    assert_eq!(kv(&out), vec![(2, "b".into())]);
    assert!(h.lake.asked.lock().unwrap().is_empty());
}

#[tokio::test]
async fn lake_read_covers_a_partition_and_projects() {
    let h = Harness::new();
    h.whole_lake(5, vec![lake_rows(&[(1, "a")]), lake_rows(&[(2, "b")])]);
    let split = h.split(5, None).await.unwrap();
    let out: Vec<RecordBatch> = h
        .read
        .read_lake(&schema(false), split.clone(), &Options::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(kv(&out), vec![(1, "a".into()), (2, "b".into())]);
    assert!(out[0].schema().field(1).is_nullable());

    let out: Vec<RecordBatch> = h
        .read
        .read_lake(&schema(false), split, &projected(vec![1]))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(out[0].schema().fields().len(), 1);
    assert_eq!(h.lake.asked.lock().unwrap().last(), Some(&Some(vec![1])));
}

#[tokio::test]
async fn the_filter_reaches_the_lake_plan() {
    let h = Harness::new();
    h.whole_lake(5, vec![lake_rows(&[(1, "a")])]);
    let filter = Predicate::Null {
        column: "v".into(),
        negated: true,
    };
    h.read.plan_lake(&h.path, 5, Some(&filter)).await.unwrap();
    assert_eq!(h.lake.filters.lock().unwrap().as_slice(), &[Some(filter)]);
}

#[tokio::test]
async fn without_a_lake_snapshot_the_log_is_everything() {
    let h = Harness::new();
    h.log(vec![appends(&[(1, "a")], 0), appends(&[(2, "b")], 1)]);
    let plan = h.plan(None).await;
    assert_eq!((plan.lake, plan.log_from, plan.log_to), (None, 0, 2));
    let out = h.rows(&schema(false), plan, Options::default()).await;
    assert_eq!(kv(&out), vec![(1, "a".into()), (2, "b".into())]);
}

#[tokio::test]
async fn a_limit_stops_the_lake_and_the_log_early() {
    let h = Harness::new();
    h.lake(
        7,
        vec![lake_rows(&[(1, "a"), (2, "b")]), lake_rows(&[(3, "c")])],
    );
    h.log(vec![appends(&[(4, "d")], 3), appends(&[(5, "e")], 4)]);
    let position = LakePosition {
        snapshot_id: 7,
        log_end_offset: 3,
    };
    let options = Options {
        projection: None,
        limit: Some(1),
    };
    let out = h
        .rows(&schema(false), h.plan(Some(position)).await, options)
        .await;
    assert_eq!(kv(&out), vec![(1, "a".into())]);

    let options = Options {
        projection: None,
        limit: Some(4),
    };
    let out = h
        .rows(&schema(false), h.plan(Some(position)).await, options)
        .await;
    assert_eq!(kv(&out).len(), 4);
}

#[tokio::test]
async fn primary_key_merge_applies_updates_deletes_and_inserts_whatever_the_lake_order() {
    let h = Harness::new();
    h.lake(
        3,
        vec![
            lake_rows(&[(3, "c"), (1, "a"), (2, "b")]),
            lake_rows(&[(6, "f"), (4, "d")]),
        ],
    );
    h.log(vec![
        appends(&[(99, "ignored: below the lake offset")], 0),
        changes(
            &[
                (ChangeType::UpdateBefore, 2, "b"),
                (ChangeType::UpdateAfter, 2, "B"),
                (ChangeType::Delete, 4, "d"),
                (ChangeType::Insert, 0, "zero"),
            ],
            10,
        ),
        changes(
            &[
                (ChangeType::Insert, 5, "e"),
                (ChangeType::Insert, 9, "i"),
                (ChangeType::UpdateBefore, 6, "f"),
                (ChangeType::UpdateAfter, 6, "F"),
                (ChangeType::Delete, 6, "F"),
                (ChangeType::Insert, 7, "g"),
                (ChangeType::UpdateBefore, 7, "g"),
                (ChangeType::UpdateAfter, 7, "G"),
            ],
            14,
        ),
    ]);
    let plan = h
        .plan(Some(LakePosition {
            snapshot_id: 3,
            log_end_offset: 10,
        }))
        .await;
    assert_eq!((plan.log_from, plan.log_to), (10, 22));
    let out = h.rows(&schema(true), plan, Options::default()).await;
    let mut rows = kv(&out);
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![
            (0, "zero".into()),
            (1, "a".into()),
            (2, "B".into()),
            (3, "c".into()),
            (5, "e".into()),
            (7, "G".into()),
            (9, "i".into()),
        ]
    );
}

#[tokio::test]
async fn a_primary_key_limit_applies_after_the_merge() {
    let h = Harness::new();
    h.lake(3, vec![lake_rows(&[(1, "a"), (2, "b"), (3, "c")])]);
    h.log(vec![changes(
        &[(ChangeType::Delete, 1, "a"), (ChangeType::Delete, 2, "b")],
        5,
    )]);
    let plan = h
        .plan(Some(LakePosition {
            snapshot_id: 3,
            log_end_offset: 5,
        }))
        .await;
    let options = Options {
        projection: None,
        limit: Some(1),
    };
    let out = h.rows(&schema(true), plan, options).await;
    assert_eq!(kv(&out), vec![(3, "c".into())]);
    assert_eq!(h.lake.asked.lock().unwrap().as_slice(), &[Some(vec![0, 1])]);
}

#[tokio::test]
async fn projecting_the_key_away_still_merges_on_it() {
    let h = Harness::new();
    h.lake(1, vec![lake_rows(&[(1, "a"), (2, "b")])]);
    h.log(vec![
        appends(&[(1, "a"), (2, "b")], 0),
        changes(
            &[
                (ChangeType::Delete, 1, "a"),
                (ChangeType::UpdateBefore, 2, "b"),
                (ChangeType::UpdateAfter, 2, "B"),
                (ChangeType::Insert, 3, "c"),
            ],
            2,
        ),
    ]);
    let plan = h
        .plan(Some(LakePosition {
            snapshot_id: 1,
            log_end_offset: 2,
        }))
        .await;
    let out = h.rows(&schema(true), plan, projected(vec![1])).await;
    let values: Vec<String> = out
        .iter()
        .flat_map(|b| {
            let v = b.column(0).as_string::<i32>();
            (0..b.num_rows()).map(move |i| v.value(i).to_string())
        })
        .collect();
    assert_eq!(values, vec!["B", "c"]);
    assert_eq!(out[0].schema().fields().len(), 1);
    assert_eq!(h.lake.asked.lock().unwrap().as_slice(), &[Some(vec![0, 1])]);
    assert_eq!(h.log.asked.lock().unwrap().as_slice(), &[Some(vec![0, 1])]);
}
