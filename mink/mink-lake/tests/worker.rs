//! The tiering worker against fake coordinator and bucket sources, and against a real Iceberg catalog.

use std::collections::BTreeMap;
use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use iceberg::spec::{Operation, PrimitiveType, Type};
use iceberg::table::Table;
use mink_coordinator::tiering::{Manager, Table as Lease};
use mink_coordinator::{LakeCatalog, TieringState};
use mink_lake::iceberg::{Catalog, Config, Source};
use mink_lake::{
    BucketSource, Coordinator, Error, LogSource, Reader, Result, SNAPSHOT_OFFSETS_PROPERTY,
    ScanOptions, SnapshotRead, TableInfo, TieredBatch, Worker,
};
use mink_metadata::LakeSnapshotRow;
use mink_record::{ChangeType, Changes};
use mink_table::{
    Bucket, BucketId, Column, Descriptor, Id, LakeFormat, Options, PartitionName, Path, PrimaryKey,
    Schema,
};
use mink_types::DataType;
use tempfile::TempDir;

const FRESHNESS: Duration = Duration::from_secs(60);

fn path(s: &str) -> Path {
    s.parse().unwrap()
}

fn schema(primary_key: bool) -> Schema {
    let builder = Schema::builder()
        .column(Column::new("k", DataType::big_int()).unwrap())
        .column(Column::new("v", DataType::string()).unwrap());
    if primary_key {
        builder
            .primary_key(PrimaryKey::new(vec!["k".into()]).unwrap())
            .build()
            .unwrap()
    } else {
        builder.build().unwrap()
    }
}

fn descriptor(primary_key: bool, auto_compaction: bool) -> Descriptor {
    Descriptor::builder(schema(primary_key))
        .bucket_count(2)
        .options(Options {
            lake: Some(LakeFormat::Iceberg),
            lake_auto_compaction: auto_compaction,
            ..Options::default()
        })
        .build()
        .unwrap()
}

fn rows(kv: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("k", ArrowType::Int64, false),
            ArrowField::new("v", ArrowType::Utf8, true),
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
        rows: rows(kv),
        base_offset,
        timestamp_ms: 1_700_000_000_000 + base_offset,
    }
}

fn changes(kv: &[(ChangeType, i64, &str)], base_offset: i64) -> TieredBatch {
    let bytes: Vec<u8> = kv.iter().map(|(c, _, _)| c.byte()).collect();
    let rows: Vec<(i64, &str)> = kv.iter().map(|(_, k, v)| (*k, *v)).collect();
    TieredBatch {
        changes: Changes::vector(Bytes::from(bytes)).unwrap(),
        rows: self::rows(&rows),
        base_offset,
        timestamp_ms: 1_700_000_000_000 + base_offset,
    }
}

struct FakeCoordinator {
    now_ms: Mutex<i64>,
    manager: Mutex<Manager>,
    lake: Mutex<BTreeMap<Id, LakeSnapshotRow>>,
}

impl FakeCoordinator {
    fn new() -> Arc<Self> {
        Arc::new(FakeCoordinator {
            now_ms: Mutex::new(0),
            manager: Mutex::new(Manager::new(Duration::from_secs(120))),
            lake: Mutex::new(BTreeMap::new()),
        })
    }

    fn add(&self, table_id: Id, path: &Path) {
        let now = *self.now_ms.lock().unwrap();
        self.manager
            .lock()
            .unwrap()
            .add(table_id, path.clone(), FRESHNESS, now);
    }

    fn make_due(&self) {
        let mut now = self.now_ms.lock().unwrap();
        *now += FRESHNESS.as_millis() as i64 + 1;
        self.manager.lock().unwrap().tick(*now);
    }

    fn state(&self, table_id: Id) -> Option<TieringState> {
        self.manager.lock().unwrap().state(table_id)
    }

    fn forget_lake(&self, table_id: Id) {
        self.lake.lock().unwrap().remove(&table_id);
    }

    fn drop_table(&self, table_id: Id) {
        self.forget_lake(table_id);
        self.manager.lock().unwrap().remove(table_id);
    }
}

#[async_trait]
impl Coordinator for FakeCoordinator {
    async fn request_table(&self) -> Result<Option<Lease>> {
        let now = *self.now_ms.lock().unwrap();
        Ok(self.manager.lock().unwrap().request_table(now))
    }

    async fn heartbeat(&self, table_id: Id, epoch: u64) -> Result<()> {
        let now = *self.now_ms.lock().unwrap();
        Ok(self
            .manager
            .lock()
            .unwrap()
            .heartbeat(table_id, epoch, now)?)
    }

    async fn finish(&self, table_id: Id, epoch: u64) -> Result<()> {
        let now = *self.now_ms.lock().unwrap();
        Ok(self
            .manager
            .lock()
            .unwrap()
            .finish(table_id, epoch, false, now)?)
    }

    async fn fail(&self, table_id: Id, epoch: u64) -> Result<()> {
        let now = *self.now_ms.lock().unwrap();
        Ok(self.manager.lock().unwrap().fail(table_id, epoch, now)?)
    }

    fn lake_snapshot(&self, table_id: Id) -> Option<LakeSnapshotRow> {
        self.lake.lock().unwrap().get(&table_id).cloned()
    }

    async fn commit_lake_snapshot(&self, table_id: Id, snapshot: LakeSnapshotRow) -> Result<()> {
        let mut lake = self.lake.lock().unwrap();
        if let Some(current) = lake.get(&table_id) {
            for (bucket, offset) in &snapshot.bucket_log_end_offset {
                if current
                    .bucket_log_end_offset
                    .get(bucket)
                    .is_some_and(|c| offset < c)
                {
                    return Err(Error::Other(format!(
                        "lake snapshot {} moves {bucket:?} backwards",
                        snapshot.snapshot_id
                    )));
                }
            }
        }
        lake.insert(table_id, snapshot);
        Ok(())
    }
}

#[derive(Default)]
struct Feed {
    log_start: i64,
    batches: Vec<TieredBatch>,
    snapshot: Option<(Vec<(i64, &'static str)>, i64)>,
}

impl Feed {
    fn high_watermark(&self) -> i64 {
        self.batches
            .last()
            .map_or(self.log_start, |b| b.last_offset() + 1)
    }
}

#[derive(Default)]
struct MemorySource {
    tables: Mutex<BTreeMap<Path, TableInfo>>,
    buckets: Mutex<BTreeMap<Bucket, Feed>>,
}

impl MemorySource {
    fn add_table(&self, table_id: Id, path: &Path, descriptor: &Descriptor) -> Vec<Bucket> {
        let buckets: Vec<Bucket> = (0..descriptor.bucket_count().unwrap())
            .map(|b| Bucket::new(table_id, BucketId(b)))
            .collect();
        self.tables.lock().unwrap().insert(
            path.clone(),
            TableInfo {
                table_id,
                path: path.clone(),
                descriptor: Arc::new(descriptor.clone()),
                buckets: buckets
                    .iter()
                    .map(|b| (*b, None::<PartitionName>))
                    .collect(),
            },
        );
        let mut all = self.buckets.lock().unwrap();
        for bucket in &buckets {
            all.insert(*bucket, Feed::default());
        }
        buckets
    }

    fn append(&self, bucket: Bucket, mut batch: TieredBatch) -> i64 {
        let mut all = self.buckets.lock().unwrap();
        let entry = all.get_mut(&bucket).unwrap();
        batch.base_offset = entry.high_watermark();
        let end = batch.last_offset() + 1;
        entry.batches.push(batch);
        end
    }

    fn set_snapshot(&self, bucket: Bucket, rows: Vec<(i64, &'static str)>, log_offset: i64) {
        self.buckets
            .lock()
            .unwrap()
            .get_mut(&bucket)
            .unwrap()
            .snapshot = Some((rows, log_offset));
    }
}

#[async_trait]
impl LogSource for MemorySource {
    async fn offsets(&self, bucket: Bucket) -> Result<(i64, i64)> {
        let all = self.buckets.lock().unwrap();
        let entry = &all[&bucket];
        Ok((entry.log_start, entry.high_watermark()))
    }

    fn log(
        &self,
        bucket: Bucket,
        from: i64,
        to: i64,
        columns: Option<Vec<usize>>,
    ) -> BoxStream<'static, Result<TieredBatch>> {
        assert!(columns.is_none(), "the tiering worker reads whole rows");
        let all = self.buckets.lock().unwrap();
        let batches: Vec<TieredBatch> = all[&bucket]
            .batches
            .iter()
            .filter(|b| b.base_offset >= from && b.last_offset() < to)
            .cloned()
            .collect();
        Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
    }
}

#[async_trait]
impl BucketSource for MemorySource {
    fn table(&self, path: &Path) -> Result<Option<TableInfo>> {
        Ok(self.tables.lock().unwrap().get(path).cloned())
    }

    async fn snapshot(&self, bucket: Bucket) -> Result<SnapshotRead> {
        let all = self.buckets.lock().unwrap();
        let (rows, log_offset) = all[&bucket]
            .snapshot
            .clone()
            .expect("test sets the snapshot");
        Ok(SnapshotRead {
            log_offset,
            batches: Box::pin(futures::stream::iter([Ok(TieredBatch::snapshot(
                self::rows(&rows),
            ))])),
        })
    }
}

struct Harness {
    _dir: TempDir,
    catalog: Arc<Catalog>,
    coordinator: Arc<FakeCoordinator>,
    source: Arc<MemorySource>,
    worker: Worker<Catalog>,
}

impl Harness {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = format!("file://{}", dir.path().display());
        let catalog = Arc::new(Catalog::connect(&Config::memory(warehouse)).await.unwrap());
        let coordinator = FakeCoordinator::new();
        let source = Arc::new(MemorySource::default());
        let worker = Worker::new(
            catalog.clone(),
            coordinator.clone(),
            source.clone(),
            mink_lake::tiering::Config {
                poll_interval: Duration::from_millis(10),
                heartbeat_interval: Duration::from_millis(5),
            },
        );
        Harness {
            _dir: dir,
            catalog,
            coordinator,
            source,
            worker,
        }
    }

    async fn create(&self, table_id: Id, path: &Path, descriptor: &Descriptor) -> Vec<Bucket> {
        LakeCatalog::create_table(self.catalog.as_ref(), path, descriptor)
            .await
            .unwrap();
        self.coordinator.add(table_id, path);
        self.source.add_table(table_id, path, descriptor)
    }

    async fn round(&self) -> Result<mink_lake::RoundReport> {
        self.coordinator.make_due();
        self.worker
            .run_once()
            .await
            .map(|r| r.expect("a table was due"))
    }

    async fn table(&self, path: &Path) -> Table {
        self.catalog
            .target()
            .load(&Catalog::identifier(path))
            .await
            .unwrap()
    }
}

async fn scan(table: &Table) -> Vec<(i64, String)> {
    let stream = table.scan().build().unwrap().to_arrow().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut out = Vec::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), 2, "user columns only");
        let k = batch
            .column_by_name("k")
            .unwrap()
            .as_primitive::<Int64Type>();
        let v = batch.column_by_name("v").unwrap().as_string::<i32>();
        for i in 0..batch.num_rows() {
            out.push((k.value(i), v.value(i).to_string()));
        }
    }
    out.sort();
    out
}

fn offsets_property(table: &Table) -> BTreeMap<Bucket, i64> {
    let snapshot = table.metadata().current_snapshot().unwrap();
    let json = &snapshot.summary().additional_properties[SNAPSHOT_OFFSETS_PROPERTY];
    let offsets: Vec<mink_lake::BucketOffset> = serde_json::from_str(json).unwrap();
    offsets
        .into_iter()
        .map(|o| (o.bucket, o.log_end_offset))
        .collect()
}

fn summary(table: &Table, key: &str) -> String {
    table
        .metadata()
        .current_snapshot()
        .unwrap()
        .summary()
        .additional_properties
        .get(key)
        .cloned()
        .unwrap_or_default()
}

#[tokio::test]
async fn log_table_rounds_tier_only_what_is_new_and_record_offsets() {
    let h = Harness::new().await;
    let orders = path("db.orders");
    let table_id = Id(7);
    let buckets = h.create(table_id, &orders, &descriptor(false, false)).await;
    h.source
        .append(buckets[0], appends(&[(1, "a"), (2, "b")], 0));
    h.source.append(buckets[1], appends(&[(3, "c")], 0));

    assert!(h.worker.run_once().await.unwrap().is_none());

    let report = h.round().await.unwrap();
    assert_eq!(
        report.tiered,
        BTreeMap::from([(buckets[0], 2), (buckets[1], 1)])
    );
    let snapshot_id = report.snapshot_id.unwrap();
    let lake = h.coordinator.lake_snapshot(table_id).unwrap();
    assert_eq!(lake.snapshot_id, snapshot_id);
    assert_eq!(lake.bucket_log_end_offset, report.tiered);
    assert_eq!(h.coordinator.state(table_id), Some(TieringState::Scheduled));
    let table = h.table(&orders).await;
    assert_eq!(offsets_property(&table), report.tiered);
    assert_eq!(
        scan(&table).await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into()),]
    );

    h.source.append(buckets[0], appends(&[(4, "d")], 0));
    let report = h.round().await.unwrap();
    assert_eq!(report.tiered, BTreeMap::from([(buckets[0], 3)]));
    let lake = h.coordinator.lake_snapshot(table_id).unwrap();
    assert_eq!(
        lake.bucket_log_end_offset,
        BTreeMap::from([(buckets[0], 3), (buckets[1], 1)])
    );
    let table = h.table(&orders).await;
    assert_eq!(offsets_property(&table), lake.bucket_log_end_offset);
    assert_eq!(scan(&table).await.len(), 4);

    let report = h.round().await.unwrap();
    assert_eq!(report.snapshot_id, None);
    assert!(report.tiered.is_empty());
    assert_eq!(h.coordinator.lake_snapshot(table_id).unwrap(), lake);
    assert_eq!(h.coordinator.state(table_id), Some(TieringState::Scheduled));
}

#[tokio::test]
async fn primary_key_tables_bootstrap_from_the_snapshot_then_follow_the_changelog() {
    let h = Harness::new().await;
    let users = path("db.users");
    let table_id = Id(42);
    let buckets = h.create(table_id, &users, &descriptor(true, false)).await;
    for _ in 0..5 {
        h.source
            .append(buckets[0], changes(&[(ChangeType::Insert, 9, "x")], 0));
    }
    h.source
        .set_snapshot(buckets[0], vec![(1, "a2"), (2, "b")], 5);
    h.source.set_snapshot(buckets[1], vec![], 0);

    let report = h.round().await.unwrap();
    assert_eq!(report.tiered, BTreeMap::from([(buckets[0], 5)]));
    let table = h.table(&users).await;
    assert_eq!(scan(&table).await, vec![(1, "a2".into()), (2, "b".into())]);

    h.source.append(
        buckets[0],
        changes(
            &[
                (ChangeType::UpdateBefore, 1, "a2"),
                (ChangeType::UpdateAfter, 1, "a3"),
                (ChangeType::Delete, 2, "b"),
                (ChangeType::Insert, 3, "c"),
            ],
            0,
        ),
    );
    let report = h.round().await.unwrap();
    assert_eq!(report.tiered, BTreeMap::from([(buckets[0], 9)]));
    let table = h.table(&users).await;
    assert_eq!(scan(&table).await, vec![(1, "a3".into()), (3, "c".into())]);
    assert_eq!(summary(&table, "added-equality-deletes"), "2");
}

#[tokio::test]
async fn a_lake_snapshot_never_recorded_is_recovered_before_tiering_more() {
    let h = Harness::new().await;
    let orders = path("db.orders");
    let table_id = Id(3);
    let buckets = h.create(table_id, &orders, &descriptor(false, false)).await;
    h.source.append(buckets[0], appends(&[(1, "a")], 0));
    let first = h.round().await.unwrap();
    let recorded = h.coordinator.lake_snapshot(table_id).unwrap();

    h.coordinator.forget_lake(table_id);
    h.source.append(buckets[0], appends(&[(2, "b")], 0));
    let err = h.round().await.unwrap_err();
    assert!(err.to_string().contains("behind the lake"), "{err}");
    assert_eq!(h.coordinator.lake_snapshot(table_id).unwrap(), recorded);
    assert_eq!(h.coordinator.state(table_id), Some(TieringState::Pending));
    let table = h.table(&orders).await;
    assert_eq!(
        table.metadata().current_snapshot().unwrap().snapshot_id(),
        first.snapshot_id.unwrap()
    );
    let data_dir = fs::read_dir(format!(
        "{}/data",
        table.metadata().location().trim_start_matches("file://")
    ))
    .unwrap()
    .count();
    assert_eq!(data_dir, 1, "the aborted round's file was deleted");

    let report = h.worker.run_once().await.unwrap().unwrap();
    assert_eq!(report.tiered, BTreeMap::from([(buckets[0], 2)]));
    assert_eq!(scan(&h.table(&orders).await).await.len(), 2);
}

#[tokio::test]
async fn auto_compaction_rewrites_small_files_after_the_third_round() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();
    let h = Harness::new().await;
    let events = path("db.events");
    let table_id = Id(5);
    let buckets = h.create(table_id, &events, &descriptor(false, true)).await;

    for round in 0..3 {
        h.source.append(buckets[0], appends(&[(round, "r")], 0));
        let report = h.round().await.unwrap();
        assert!(report.snapshot_id.is_some());
        let table = h.table(&events).await;
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Append
        );
    }
    let table = h.table(&events).await;
    assert_eq!(summary(&table, "total-data-files"), "3");

    h.source.append(buckets[0], appends(&[(3, "r")], 0));
    let report = h.round().await.unwrap();
    let table = h.table(&events).await;
    let current = table.metadata().current_snapshot().unwrap();
    assert_eq!(current.summary().operation, Operation::Replace);
    assert_eq!(report.snapshot_id, Some(current.snapshot_id()));
    assert_eq!(summary(&table, "deleted-data-files"), "3");
    assert_eq!(summary(&table, "added-data-files"), "1");
    assert_eq!(summary(&table, "total-data-files"), "2");
    assert_eq!(summary(&table, "total-records"), "4");
    assert_eq!(offsets_property(&table), BTreeMap::from([(buckets[0], 4)]));
    assert_eq!(
        h.coordinator.lake_snapshot(table_id).unwrap().snapshot_id,
        current.snapshot_id()
    );
    assert_eq!(
        scan(&table).await,
        vec![
            (0, "r".into()),
            (1, "r".into()),
            (2, "r".into()),
            (3, "r".into()),
        ]
    );
    let alive: Vec<_> = table.metadata().snapshots().collect();
    assert_eq!(alive.len(), 5);
}

#[tokio::test]
async fn primary_key_compaction_leaves_files_that_equality_deletes_still_reach() {
    let h = Harness::new().await;
    let users = path("db.users");
    let table_id = Id(6);
    let buckets = h.create(table_id, &users, &descriptor(true, true)).await;
    let b0 = buckets[0];

    h.source
        .append(b0, changes(&[(ChangeType::Insert, 1, "a")], 0));
    h.source.set_snapshot(b0, vec![(1, "a")], 1);
    h.source.set_snapshot(buckets[1], vec![], 0);
    h.round().await.unwrap();
    for v in ["b", "c"] {
        h.source.append(
            b0,
            changes(
                &[
                    (ChangeType::UpdateBefore, 1, "-"),
                    (ChangeType::UpdateAfter, 1, v),
                ],
                0,
            ),
        );
        h.round().await.unwrap();
    }
    for k in 2..5 {
        h.source
            .append(b0, changes(&[(ChangeType::Insert, k, "n")], 0));
        h.round().await.unwrap();
    }
    let table = h.table(&users).await;
    assert_eq!(summary(&table, "total-data-files"), "6");
    assert_eq!(summary(&table, "total-delete-files"), "2");

    h.source
        .append(b0, changes(&[(ChangeType::Insert, 5, "n")], 0));
    h.round().await.unwrap();
    let table = h.table(&users).await;
    let current = table.metadata().current_snapshot().unwrap();
    assert_eq!(current.summary().operation, Operation::Replace);
    assert_eq!(summary(&table, "deleted-data-files"), "3");
    assert_eq!(summary(&table, "total-data-files"), "5");
    assert_eq!(summary(&table, "total-delete-files"), "2");
    assert_eq!(
        scan(&table).await,
        vec![
            (1, "c".into()),
            (2, "n".into()),
            (3, "n".into()),
            (4, "n".into()),
            (5, "n".into()),
        ]
    );
}

#[tokio::test]
async fn lake_source_plans_keyless_tables_whole_and_keyed_tables_per_bucket_sorted() {
    let h = Harness::new().await;

    let orders = path("db.orders");
    let buckets = h.create(Id(7), &orders, &descriptor(false, false)).await;
    h.source
        .append(buckets[0], appends(&[(1, "a"), (2, "b")], 0));
    h.source.append(buckets[1], appends(&[(3, "c")], 0));
    let snapshot_id = h.round().await.unwrap().snapshot_id.unwrap();
    let source = Source::new(h.table(&orders).await);
    let splits = mink_lake::Source::plan(&source, snapshot_id, None)
        .await
        .unwrap();
    assert_eq!(
        splits
            .iter()
            .map(|s| (s.bucket, s.partition.clone(), s.inner.len()))
            .collect::<Vec<_>>(),
        vec![(None, None, 2)]
    );
    let batches: Vec<RecordBatch> = mink_lake::Source::read(
        &source,
        splits[0].clone(),
        ScanOptions {
            projection: Some(vec![1]),
            limit: None,
        },
    )
    .await
    .unwrap()
    .try_collect()
    .await
    .unwrap();
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["v"]);
    let mut values: Vec<&str> = batches
        .iter()
        .flat_map(|b| {
            let v = b.column(0).as_string::<i32>();
            (0..b.num_rows()).map(move |i| v.value(i))
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, ["a", "b", "c"]);

    let planned = h.catalog.plan(&orders, snapshot_id, None).await.unwrap();
    assert_eq!(
        planned
            .iter()
            .map(|s| (s.bucket, s.files, s.rows))
            .collect::<Vec<_>>(),
        vec![(None, 2, Some(3))],
        "keyless tables are not bucketed in the lake"
    );
    let whole: Vec<RecordBatch> = h
        .catalog
        .read(planned.into_iter().next().unwrap(), ScanOptions::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(whole.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    let h = Harness::new().await;
    let users = path("db.users");
    let pk = h.create(Id(8), &users, &descriptor(true, false)).await;
    h.source.append(
        pk[0],
        changes(
            &[
                (ChangeType::Insert, 30, "z"),
                (ChangeType::Insert, 10, "x"),
                (ChangeType::Insert, 20, "y"),
            ],
            0,
        ),
    );
    h.source
        .set_snapshot(pk[0], vec![(30, "z"), (10, "x"), (20, "y")], 3);
    h.source.set_snapshot(pk[1], vec![], 0);
    let snapshot_id = h.round().await.unwrap().snapshot_id.unwrap();
    let split = h
        .catalog
        .plan(&users, snapshot_id, None)
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.bucket == Some(BucketId(0)))
        .unwrap();
    let rows: Vec<RecordBatch> = h
        .catalog
        .read(split, ScanOptions::default())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut keys: Vec<i64> = rows
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, vec![10, 20, 30]);
}

#[tokio::test]
async fn attached_tables_tier_on_top_of_the_existing_snapshot() {
    let h = Harness::new().await;

    let orders = path("db.orders");
    let existing = h.create(Id(20), &orders, &descriptor(false, false)).await;
    h.source
        .append(existing[0], appends(&[(1, "a"), (2, "b")], 0));
    let baseline = h.round().await.unwrap().snapshot_id.unwrap();
    h.coordinator.drop_table(Id(20));
    h.source.tables.lock().unwrap().clear();

    let attach = Descriptor::builder(Schema::builder().build().unwrap())
        .bucket_count(2)
        .options(Options {
            lake: Some(LakeFormat::Iceberg),
            lake_attach: true,
            ..Options::default()
        })
        .build()
        .unwrap();
    let created = LakeCatalog::create_table(h.catalog.as_ref(), &orders, &attach)
        .await
        .unwrap();
    assert_eq!(created.baseline_snapshot_id, Some(baseline));
    let attached = created.descriptor.unwrap();
    assert_eq!(
        attached
            .schema()
            .columns()
            .iter()
            .map(|c| (c.name(), c.data_type().clone()))
            .collect::<Vec<_>>(),
        [("k", DataType::big_int()), ("v", DataType::string())]
    );
    h.coordinator
        .commit_lake_snapshot(
            Id(21),
            LakeSnapshotRow {
                snapshot_id: baseline,
                bucket_log_end_offset: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    h.coordinator.add(Id(21), &orders);
    let buckets = h.source.add_table(Id(21), &orders, &attached);
    h.source.append(buckets[1], appends(&[(3, "c")], 0));

    let report = h.round().await.unwrap();
    assert_eq!(report.tiered, BTreeMap::from([(buckets[1], 1)]));
    let table = h.table(&orders).await;
    assert_eq!(
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id(),
        Some(baseline)
    );
    assert_eq!(
        scan(&table).await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]
    );

    h.coordinator.drop_table(Id(21));
    let users = path("db.users");
    let existing = h.create(Id(22), &users, &descriptor(true, false)).await;
    h.source
        .set_snapshot(existing[0], vec![(1, "a"), (2, "b")], 2);
    h.source.set_snapshot(existing[1], vec![], 0);
    h.source
        .append(existing[0], changes(&[(ChangeType::Insert, 1, "a")], 0));
    let baseline = h.round().await.unwrap().snapshot_id.unwrap();
    h.coordinator.drop_table(Id(22));
    h.source.tables.lock().unwrap().clear();

    let attach = descriptor(true, false)
        .to_builder()
        .options(Options {
            lake: Some(LakeFormat::Iceberg),
            lake_attach: true,
            ..Options::default()
        })
        .build()
        .unwrap();
    let created = LakeCatalog::create_table(h.catalog.as_ref(), &users, &attach)
        .await
        .unwrap();
    assert_eq!(created.baseline_snapshot_id, Some(baseline));
    assert_eq!(created.descriptor.unwrap(), attach);
    h.coordinator
        .commit_lake_snapshot(
            Id(23),
            LakeSnapshotRow {
                snapshot_id: baseline,
                bucket_log_end_offset: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    h.coordinator.add(Id(23), &users);
    let buckets = h.source.add_table(Id(23), &users, &attach);
    h.source.append(
        buckets[0],
        changes(
            &[(ChangeType::Insert, 2, "B"), (ChangeType::Insert, 3, "c")],
            0,
        ),
    );
    let report = h.round().await.unwrap();
    assert_eq!(report.tiered, BTreeMap::from([(buckets[0], 2)]));
    let table = h.table(&users).await;
    assert_eq!(summary(&table, "added-equality-deletes"), "2");
    assert_eq!(
        scan(&table).await,
        vec![(1, "a".into()), (2, "B".into()), (3, "c".into())]
    );
}

#[tokio::test]
async fn a_fenced_round_reports_nothing() {
    let h = Harness::new().await;
    let orders = path("db.orders");
    let table_id = Id(11);
    let buckets = h.create(table_id, &orders, &descriptor(false, false)).await;
    h.source.append(buckets[0], appends(&[(1, "a")], 0));
    h.coordinator.make_due();
    let lease = h.coordinator.request_table().await.unwrap().unwrap();
    h.coordinator.fail(table_id, lease.epoch).await.unwrap();
    let err = h.worker.tier(&lease).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Coordinator(mink_coordinator::Error::TieringFenced { .. })
        ),
        "{err}"
    );
    assert_eq!(h.coordinator.state(table_id), Some(TieringState::Pending));
}

#[tokio::test]
async fn alter_table_evolves_the_iceberg_schema_and_old_batches_widen() {
    let h = Harness::new().await;
    let path = path("db.evolving");
    let before = descriptor(false, false);
    let buckets = h.create(Id(1), &path, &before).await;
    h.source.append(buckets[0], appends(&[(1, "a")], 0));
    h.round().await.unwrap();

    let after = mink_table::alter_table(
        &before,
        &[
            mink_table::Change::add_column("w", DataType::int()),
            mink_table::Change::set("owner", "ann"),
        ],
        Some(LakeFormat::Iceberg),
    )
    .unwrap();
    LakeCatalog::alter_table(h.catalog.as_ref(), &path, &before, &after)
        .await
        .unwrap();
    h.source
        .tables
        .lock()
        .unwrap()
        .get_mut(&path)
        .unwrap()
        .descriptor = Arc::new(after.clone());

    let table = h.table(&path).await;
    let schema = table.metadata().current_schema();
    let names: Vec<&str> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, ["k", "v", "w"]);
    let first = schema.field_by_name("k").unwrap().id;
    assert_eq!(
        schema.field_by_name("v").unwrap().id,
        first + 1,
        "existing ids kept"
    );
    assert_eq!(
        schema.field_by_name("w").unwrap().id,
        first + 2,
        "next after the 2 existing"
    );
    assert_eq!(table.metadata().last_column_id(), first + 2);
    assert_eq!(
        table
            .metadata()
            .properties()
            .get("mink.owner")
            .map(String::as_str),
        Some("ann")
    );
    LakeCatalog::alter_table(h.catalog.as_ref(), &path, &after, &after)
        .await
        .unwrap();

    h.source.append(buckets[0], appends(&[(2, "b")], 0));
    let wide = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("k", ArrowType::Int64, false),
            ArrowField::new("v", ArrowType::Utf8, true),
            ArrowField::new("w", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    h.source.append(
        buckets[0],
        TieredBatch {
            changes: Changes::AppendOnly(1),
            rows: wide,
            base_offset: 0,
            timestamp_ms: 1_700_000_000_000,
        },
    );
    h.round().await.unwrap();

    let table = h.table(&path).await;
    let stream = table.scan().build().unwrap().to_arrow().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut got = Vec::new();
    for batch in &batches {
        let k = batch
            .column_by_name("k")
            .unwrap()
            .as_primitive::<Int64Type>();
        let w = batch
            .column_by_name("w")
            .unwrap()
            .as_primitive::<Int32Type>();
        for i in 0..batch.num_rows() {
            got.push((k.value(i), (!Array::is_null(w, i)).then(|| w.value(i))));
        }
    }
    got.sort();
    assert_eq!(got, vec![(1, None), (2, None), (3, Some(30))]);

    let reshaped = mink_table::alter_table(
        &after,
        &[
            mink_table::Change::RenameColumn {
                name: "v".into(),
                new_name: "value".into(),
            },
            mink_table::Change::ModifyColumn {
                name: "w".into(),
                data_type: DataType::big_int(),
                comment: Some("wide".into()),
            },
            mink_table::Change::DropColumn { name: "k".into() },
        ],
        Some(LakeFormat::Iceberg),
    )
    .unwrap();
    LakeCatalog::alter_table(h.catalog.as_ref(), &path, &after, &reshaped)
        .await
        .unwrap();
    h.source
        .tables
        .lock()
        .unwrap()
        .get_mut(&path)
        .unwrap()
        .descriptor = Arc::new(reshaped.clone());
    let table = h.table(&path).await;
    let schema = table.metadata().current_schema();
    let fields: Vec<(&str, &Type, i32)> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.as_ref(), f.id))
        .collect();
    assert_eq!(
        fields,
        [
            ("value", &Type::Primitive(PrimitiveType::String), first + 1),
            ("w", &Type::Primitive(PrimitiveType::Long), first + 2),
        ]
    );
    assert_eq!(
        schema.field_by_name("w").unwrap().doc.as_deref(),
        Some("wide")
    );
    assert_eq!(table.metadata().last_column_id(), first + 2);

    let narrow = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("value", ArrowType::Utf8, true),
            ArrowField::new("w", ArrowType::Int64, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["d"])),
            Arc::new(Int64Array::from(vec![40])),
        ],
    )
    .unwrap();
    h.source.append(
        buckets[0],
        TieredBatch {
            changes: Changes::AppendOnly(1),
            rows: narrow,
            base_offset: 0,
            timestamp_ms: 1_700_000_000_000,
        },
    );
    h.round().await.unwrap();
    let table = h.table(&path).await;
    let stream = table.scan().build().unwrap().to_arrow().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut got = Vec::new();
    for batch in &batches {
        assert_eq!(batch.num_columns(), 2);
        let value = batch.column_by_name("value").unwrap().as_string::<i32>();
        let w = batch
            .column_by_name("w")
            .unwrap()
            .as_primitive::<Int64Type>();
        for i in 0..batch.num_rows() {
            got.push((
                value.value(i).to_string(),
                (!Array::is_null(w, i)).then(|| w.value(i)),
            ));
        }
    }
    got.sort();
    assert_eq!(
        got,
        vec![
            ("a".into(), None),
            ("b".into(), None),
            ("c".into(), Some(30)),
            ("d".into(), Some(40)),
        ]
    );
}
