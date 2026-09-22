//! Tablet behavior end to end: merge engines, partial updates, auto-increment, recovery and dedup.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use mink_common::ManualClock;
use mink_kv::{Engine, MemoryEngine, Store};
use mink_log::{FetchIsolation, Kv};
use mink_record::{Batch, ChangeType, Codec, Compression, codec};
use mink_table::{
    Aggregate, Bucket, BucketId, ChangelogImage, Column, DeleteBehavior, Descriptor, Id, KvFormat,
    LogFormat, MergeEngine, Options, PrimaryKey, Schema, SchemaId,
};
use mink_tablet::{Config, Error, Fixed, IdRange, Op, Put, RecoverPoint, Sequence, Tablet};
use mink_types::DataType;
use s3stream::{
    CreateStreamOptions, MemoryKvClient, MemoryMetadataManager, MemoryObjectStorage,
    ObjectStorageTrait, ObjectWalConfig, ObjectWalService, OpenStreamOptions, S3StreamBuilder,
    S3StreamEngine, Stream,
};

#[derive(Default)]
struct Counter(AtomicU64);

#[async_trait::async_trait]
impl Sequence for Counter {
    async fn get_and_add(&self, count: u64) -> Result<u64, Error> {
        Ok(self.0.fetch_add(count, Ordering::SeqCst))
    }
}

struct Harness {
    engine: S3StreamEngine,
    snapshots: Arc<Kv>,
    clock: Arc<ManualClock>,
    sequence: Arc<Counter>,
    bucket: Bucket,
    descriptor: Descriptor,
}

impl Harness {
    async fn new(descriptor: Descriptor) -> Self {
        let manager = MemoryMetadataManager::new();
        let data: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
        let wal: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
        let mut wal_config = ObjectWalConfig::defaults();
        wal_config.cluster_id = "mink-tablet-test".into();
        wal_config.node_id = 1;
        wal_config.epoch = 1;
        let engine = S3StreamBuilder::new(s3stream::Config::default())
            .object_storage(data)
            .write_ahead_log(Arc::new(ObjectWalService::new(wal, wal_config)))
            .object_manager(manager.clone())
            .stream_manager(manager.clone())
            .build()
            .await
            .unwrap();
        Harness {
            engine,
            snapshots: Arc::new(Kv::new(MemoryKvClient::new())),
            clock: Arc::new(ManualClock::new(1_000_000)),
            sequence: Arc::new(Counter::default()),
            bucket: Bucket::new(Id(1), BucketId(0)),
            descriptor,
        }
    }

    async fn stream(&self, existing: Option<u64>) -> Arc<dyn Stream> {
        let client = self.engine.stream_client();
        match existing {
            None => client
                .create_and_open_stream(CreateStreamOptions {
                    epoch: 1,
                    ..Default::default()
                })
                .await
                .unwrap(),
            Some(id) => client
                .open_stream(
                    id,
                    OpenStreamOptions {
                        epoch: 2,
                        ..Default::default()
                    },
                )
                .await
                .unwrap(),
        }
    }

    async fn log(&self, stream: Arc<dyn Stream>) -> Arc<mink_log::Tablet> {
        Arc::new(
            mink_log::Tablet::open(
                self.bucket,
                stream,
                self.snapshots.clone(),
                self.clock.clone(),
                mink_log::Config::default(),
            )
            .await
            .unwrap(),
        )
    }

    fn store(&self) -> Box<dyn Store> {
        MemoryEngine
            .open(Path::new("/unused"), mink_kv::Options::default())
            .unwrap()
    }

    async fn tablet(
        &self,
        log: Arc<mink_log::Tablet>,
        store: Box<dyn Store>,
        from: RecoverPoint,
    ) -> Tablet {
        Tablet::open(
            self.bucket,
            &self.descriptor,
            Arc::new(Fixed::single(self.descriptor.schema().clone())),
            self.sequence.clone(),
            log,
            store,
            from,
            Config {
                compression: Compression::None,
                auto_increment_cache: 3,
                ..Config::default()
            },
        )
        .await
        .unwrap()
    }

    async fn fresh(&self) -> Tablet {
        let log = self.log(self.stream(None).await).await;
        let from = RecoverPoint::fresh(&log);
        self.tablet(log, self.store(), from).await
    }
}

fn schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .column(Column::new("n", DataType::int()).unwrap())
        .primary_key(PrimaryKey::new(vec!["k".into()]).unwrap())
        .build()
        .unwrap()
}

fn descriptor(options: Options) -> Descriptor {
    Descriptor::builder(schema())
        .options(options)
        .build()
        .unwrap()
}

fn plain() -> Descriptor {
    descriptor(Options::default())
}

fn auto_increment() -> Descriptor {
    let schema = Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .column(Column::new("n", DataType::int()).unwrap())
        .primary_key(PrimaryKey::new(vec!["k".into()]).unwrap())
        .auto_increment("n")
        .build()
        .unwrap();
    Descriptor::builder(schema).build().unwrap()
}

fn rows(rows: &[(i64, Option<&str>, Option<i32>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::from(schema().fields())),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn key(k: i64) -> Vec<u8> {
    let batch = rows(&[(k, None, None)]);
    let schema = schema();
    let encoder = mink_record::KeyEncoder::new(
        schema.fields(),
        &["k".to_owned()],
        mink_table::Bucketing::Native,
    )
    .unwrap();
    encoder.bind(&batch).unwrap().encode_vec(0).unwrap()
}

type Decoded = (i64, Option<String>, Option<i32>);

fn decode(batch: &RecordBatch) -> Vec<Decoded> {
    let k = batch.column(0).as_primitive::<Int64Type>();
    let v = batch.column(1).as_string::<i32>();
    let n = batch.column(2).as_primitive::<Int32Type>();
    (0..batch.num_rows())
        .map(|i| {
            (
                k.value(i),
                v.is_valid(i).then(|| v.value(i).to_owned()),
                n.is_valid(i).then(|| n.value(i)),
            )
        })
        .collect()
}

fn get(tablet: &Tablet, k: i64) -> Option<Decoded> {
    let value = tablet.lookup(&key(k)).unwrap()?;
    let batch = tablet.to_batch([value]).unwrap();
    decode(&batch).pop()
}

async fn changelog(tablet: &Tablet) -> Vec<(i64, ChangeType, Decoded)> {
    let log = tablet.log();
    let codec: Box<dyn Codec> = codec(LogFormat::Arrow, Compression::None);
    let arrow = tablet.latest().unwrap().arrow.clone();
    let fetched = log
        .read(
            log.log_start_offset(),
            usize::MAX,
            FetchIsolation::LogEnd,
            None,
        )
        .await
        .unwrap();
    let mut out = Vec::new();
    for bytes in fetched.batches {
        let batch = Batch::parse(bytes).unwrap();
        batch.ensure_valid().unwrap();
        let records = batch.records(codec.as_ref(), arrow.clone(), None).unwrap();
        for (i, row) in decode(&records.batch).into_iter().enumerate() {
            out.push((
                batch.header().base_offset + i as i64,
                records.changes.get(i),
                row,
            ));
        }
    }
    out
}

fn row(k: i64, v: Option<&str>, n: Option<i32>) -> Decoded {
    (k, v.map(str::to_owned), n)
}

#[tokio::test]
async fn insert_update_delete_with_full_image() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;

    let info = tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a"), Some(1))])))
        .await
        .unwrap();
    assert_eq!((info.first_offset, info.last_offset), (0, 0));
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a"), Some(1))));
    assert_eq!(tablet.row_count().await, 1);
    assert_eq!(tablet.flushed_log_offset().await, 1);
    assert_eq!(tablet.pending_rows().await, 0);

    let info = tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("b"), None)])))
        .await
        .unwrap();
    assert_eq!((info.first_offset, info.last_offset), (1, 2));
    assert_eq!(get(&tablet, 1), Some(row(1, Some("b"), None)));
    assert_eq!(tablet.row_count().await, 1);

    let info = tablet
        .put(Put::delete(SchemaId(0), rows(&[(1, None, None)])))
        .await
        .unwrap();
    assert_eq!((info.first_offset, info.last_offset), (3, 3));
    assert_eq!(get(&tablet, 1), None);
    assert_eq!(tablet.row_count().await, 0);

    assert_eq!(
        changelog(&tablet).await,
        vec![
            (0, ChangeType::Insert, row(1, Some("a"), Some(1))),
            (1, ChangeType::UpdateBefore, row(1, Some("a"), Some(1))),
            (2, ChangeType::UpdateAfter, row(1, Some("b"), None)),
            (3, ChangeType::Delete, row(1, Some("b"), None)),
        ]
    );
}

#[tokio::test]
async fn snapshot_scan_pins_rows_at_its_log_offset() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(
            SchemaId(0),
            rows(&[(1, Some("a"), None), (2, Some("b"), None)]),
        ))
        .await
        .unwrap();

    let mut scan = tablet.snapshot_scan().await.unwrap();
    assert_eq!(scan.log_offset, 2);

    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("z"), None)])))
        .await
        .unwrap();
    tablet
        .put(Put::delete(SchemaId(0), rows(&[(2, None, None)])))
        .await
        .unwrap();

    let mut values = Vec::new();
    loop {
        let page = scan.rows.next_page(1).unwrap();
        if page.is_empty() {
            break;
        }
        values.extend(page.into_iter().map(|(_, v)| v));
    }
    let pinned = decode(&tablet.to_batch(values).unwrap());
    assert_eq!(
        pinned,
        vec![row(1, Some("a"), None), row(2, Some("b"), None)]
    );

    let tail: Vec<_> = changelog(&tablet)
        .await
        .into_iter()
        .filter(|(offset, _, _)| *offset >= scan.log_offset)
        .collect();
    assert_eq!(
        tail,
        vec![
            (2, ChangeType::UpdateBefore, row(1, Some("a"), None)),
            (3, ChangeType::UpdateAfter, row(1, Some("z"), None)),
            (4, ChangeType::Delete, row(2, Some("b"), None)),
        ]
    );
    assert_eq!(get(&tablet, 1), Some(row(1, Some("z"), None)));
}

#[tokio::test]
async fn one_put_sees_its_own_earlier_rows() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;
    let batch = rows(&[
        (1, Some("a"), None),
        (2, Some("b"), None),
        (1, Some("c"), None),
    ]);
    let put = Put::upsert(SchemaId(0), batch).with_ops(vec![Op::Upsert, Op::Upsert, Op::Upsert]);
    tablet.put(put).await.unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("c"), None)));
    assert_eq!(tablet.row_count().await, 2);
    let changes: Vec<_> = changelog(&tablet).await.into_iter().map(|c| c.1).collect();
    assert_eq!(
        changes,
        [
            ChangeType::Insert,
            ChangeType::Insert,
            ChangeType::UpdateBefore,
            ChangeType::UpdateAfter
        ]
    );
}

#[tokio::test]
async fn deleting_a_missing_key_writes_nothing() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;
    let info = tablet
        .put(Put::delete(SchemaId(0), rows(&[(9, None, None)])))
        .await
        .unwrap();
    assert_eq!(info.batch_count, 0);
    assert_eq!(tablet.log().log_end_offset(), 0);
    assert_eq!(tablet.flushed_log_offset().await, 0);
}

#[tokio::test]
async fn wal_image_writes_only_the_new_row() {
    let harness = Harness::new(descriptor(Options {
        changelog_image: ChangelogImage::Wal,
        ..Options::default()
    }))
    .await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a"), None)])))
        .await
        .unwrap();
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("b"), None)])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("b"), None)));
    assert_eq!(
        changelog(&tablet).await,
        vec![
            (0, ChangeType::UpdateAfter, row(1, Some("a"), None)),
            (1, ChangeType::UpdateAfter, row(1, Some("b"), None)),
        ]
    );
}

#[tokio::test]
async fn first_row_keeps_the_first_and_ignores_deletes() {
    let harness = Harness::new(descriptor(Options {
        merge_engine: Some(MergeEngine::FirstRow),
        ..Options::default()
    }))
    .await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a"), None)])))
        .await
        .unwrap();
    let info = tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("b"), None)])))
        .await
        .unwrap();
    assert_eq!(info.batch_count, 0);
    let info = tablet
        .put(Put::delete(SchemaId(0), rows(&[(1, None, None)])))
        .await
        .unwrap();
    assert_eq!(info.batch_count, 0);
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a"), None)));
    assert_eq!(changelog(&tablet).await.len(), 1);
}

#[tokio::test]
async fn versioned_keeps_the_greater_version() {
    let harness = Harness::new(descriptor(Options {
        merge_engine: Some(MergeEngine::Versioned { column: "n".into() }),
        ..Options::default()
    }))
    .await;
    let tablet = harness.fresh().await;
    let put = |v: &'static str, n: Option<i32>| Put::upsert(SchemaId(0), rows(&[(1, Some(v), n)]));
    tablet.put(put("v5", Some(5))).await.unwrap();
    tablet.put(put("v3", Some(3))).await.unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("v5"), Some(5))));
    tablet.put(put("v5b", Some(5))).await.unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("v5b"), Some(5))));
    tablet.put(put("null", None)).await.unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("v5b"), Some(5))));
    tablet.put(put("v7", Some(7))).await.unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("v7"), Some(7))));
    assert_eq!(changelog(&tablet).await.len(), 5);
}

fn aggregation(delete_behavior: Option<DeleteBehavior>) -> Descriptor {
    let schema = Schema::builder()
        .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
        .column(
            Column::new("v", DataType::string())
                .unwrap()
                .with_aggregate(Aggregate::list_agg()),
        )
        .column(
            Column::new("n", DataType::int())
                .unwrap()
                .with_aggregate(Aggregate::Sum),
        )
        .primary_key(PrimaryKey::new(vec!["k".into()]).unwrap())
        .build()
        .unwrap();
    Descriptor::builder(schema)
        .options(Options {
            merge_engine: Some(MergeEngine::Aggregation),
            delete_behavior,
            ..Options::default()
        })
        .build()
        .unwrap()
}

#[tokio::test]
async fn aggregation_folds_columns_and_ignores_deletes_by_default() {
    let harness = Harness::new(aggregation(None)).await;
    let tablet = harness.fresh().await;
    let put =
        |v: Option<&'static str>, n: Option<i32>| Put::upsert(SchemaId(0), rows(&[(1, v, n)]));
    tablet.put(put(Some("a"), Some(1))).await.unwrap();
    tablet.put(put(Some("b"), Some(2))).await.unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a,b"), Some(3))));
    tablet.put(put(None, Some(4))).await.unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a,b"), Some(7))));

    let info = tablet
        .put(Put::delete(SchemaId(0), rows(&[(1, None, None)])))
        .await
        .unwrap();
    assert_eq!(info.batch_count, 0);
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a,b"), Some(7))));

    let log = changelog(&tablet).await;
    assert_eq!(log.len(), 5);
    assert_eq!(log[1].1, ChangeType::UpdateBefore);
    assert_eq!(
        log[2],
        (2, ChangeType::UpdateAfter, row(1, Some("a,b"), Some(3)))
    );
}

#[tokio::test]
async fn partial_aggregation_folds_only_target_columns() {
    let harness = Harness::new(aggregation(Some(DeleteBehavior::Allow))).await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a"), Some(1))])))
        .await
        .unwrap();
    let only_n = |batch, op| {
        Put::upsert(SchemaId(0), batch)
            .with_ops(vec![op])
            .with_target_columns(vec![0, 2])
    };
    tablet
        .put(only_n(rows(&[(1, Some("ignored"), Some(10))]), Op::Upsert))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a"), Some(11))));

    tablet
        .put(only_n(rows(&[(1, None, None)]), Op::Delete))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a"), None)));
    let only_v = Put::delete(SchemaId(0), rows(&[(1, None, None)])).with_target_columns(vec![0, 1]);
    tablet.put(only_v).await.unwrap();
    assert_eq!(get(&tablet, 1), None);

    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(2, Some("x"), Some(1))])))
        .await
        .unwrap();
    tablet
        .put(Put::delete(SchemaId(0), rows(&[(2, None, None)])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 2), None);
    assert_eq!(tablet.row_count().await, 0);
}

#[tokio::test]
async fn partial_update_merges_and_clears_target_columns() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a"), Some(1))])))
        .await
        .unwrap();

    let only_v = |batch, op| {
        Put::upsert(SchemaId(0), batch)
            .with_ops(vec![op])
            .with_target_columns(vec![0, 1])
    };
    tablet
        .put(only_v(rows(&[(1, Some("b"), Some(99))]), Op::Upsert))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("b"), Some(1))));

    tablet
        .put(only_v(rows(&[(2, Some("x"), Some(99))]), Op::Upsert))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 2), Some(row(2, Some("x"), Some(99))));

    tablet
        .put(only_v(rows(&[(1, None, None)]), Op::Delete))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, None, Some(1))));

    let only_n = Put::delete(SchemaId(0), rows(&[(1, None, None)])).with_target_columns(vec![0, 2]);
    tablet.put(only_n).await.unwrap();
    assert_eq!(get(&tablet, 1), None);
    assert_eq!(tablet.row_count().await, 1);

    let missing_key =
        Put::upsert(SchemaId(0), rows(&[(3, None, None)])).with_target_columns(vec![1]);
    assert!(matches!(
        tablet.put(missing_key).await,
        Err(Error::TargetsMissKey { .. })
    ));
}

#[tokio::test]
async fn delete_behavior_disable_rejects_and_ignore_drops() {
    let harness = Harness::new(descriptor(Options {
        delete_behavior: Some(DeleteBehavior::Disable),
        ..Options::default()
    }))
    .await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a"), None)])))
        .await
        .unwrap();
    assert!(matches!(
        tablet
            .put(Put::delete(SchemaId(0), rows(&[(1, None, None)])))
            .await,
        Err(Error::DeleteDisabled)
    ));
    assert_eq!(tablet.pending_rows().await, 0);
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a"), None)));

    let harness = Harness::new(descriptor(Options {
        delete_behavior: Some(DeleteBehavior::Ignore),
        ..Options::default()
    }))
    .await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a"), None)])))
        .await
        .unwrap();
    let info = tablet
        .put(Put::delete(SchemaId(0), rows(&[(1, None, None)])))
        .await
        .unwrap();
    assert_eq!(info.batch_count, 0);
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a"), None)));
}

#[tokio::test]
async fn duplicate_batch_leaves_state_untouched() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;
    let put = || Put::upsert(SchemaId(0), rows(&[(1, Some("a"), None)])).with_writer(7, 0);
    let first = tablet.put(put()).await.unwrap();
    let again = tablet.put(put()).await.unwrap();
    assert!(again.duplicated);
    assert_eq!(
        (again.first_offset, again.last_offset),
        (first.first_offset, first.last_offset)
    );
    assert_eq!(tablet.log().log_end_offset(), 1);
    assert_eq!(tablet.pending_rows().await, 0);
    assert_eq!(tablet.row_count().await, 1);
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a"), None)));
}

#[tokio::test]
async fn rejects_bad_requests_without_side_effects() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;
    let bad_ops = Put::upsert(SchemaId(0), rows(&[(1, None, None)])).with_ops(vec![]);
    assert!(matches!(
        tablet.put(bad_ops).await,
        Err(Error::OpCount { ops: 0, rows: 1 })
    ));
    let bad_schema = Put::upsert(SchemaId(5), rows(&[(1, None, None)]));
    assert!(matches!(
        tablet.put(bad_schema).await,
        Err(Error::SchemaNotExist(SchemaId(5)))
    ));
    assert_eq!(tablet.log().log_end_offset(), 0);
    assert_eq!(tablet.pending_rows().await, 0);
}

#[tokio::test]
async fn reads_and_batches() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(
            SchemaId(0),
            rows(&[
                (3, Some("c"), None),
                (1, Some("a"), Some(1)),
                (2, None, Some(2)),
            ]),
        ))
        .await
        .unwrap();
    let k1 = key(1);
    let k2 = key(2);
    let k9 = key(9);
    let values = tablet.multi_lookup(&[&k1, &k9, &k2]).unwrap();
    assert!(values[0].is_some() && values[1].is_none() && values[2].is_some());
    let batch = tablet.to_batch(values.into_iter().flatten()).unwrap();
    assert_eq!(
        decode(&batch),
        vec![row(1, Some("a"), Some(1)), row(2, None, Some(2))]
    );
    let scanned = tablet.limit_scan(2).unwrap();
    assert_eq!(scanned.len(), 2);
    assert_eq!(tablet.prefix_lookup(&[]).unwrap().len(), 3);
    tablet.close().await.unwrap();
    assert!(matches!(tablet.lookup(&k1), Err(Error::Closed)));
    assert!(matches!(
        tablet
            .put(Put::upsert(SchemaId(0), rows(&[(1, None, None)])))
            .await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn recovers_from_the_changelog_alone() {
    let harness = Harness::new(plain()).await;
    let stream = harness.stream(None).await;
    let stream_id = stream.stream_id();
    let log = harness.log(stream).await;
    let tablet = harness
        .tablet(Arc::clone(&log), harness.store(), RecoverPoint::fresh(&log))
        .await;
    tablet
        .put(Put::upsert(
            SchemaId(0),
            rows(&[(1, Some("a"), Some(1)), (2, Some("b"), Some(2))]),
        ))
        .await
        .unwrap();
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a2"), None)])))
        .await
        .unwrap();
    tablet
        .put(Put::delete(SchemaId(0), rows(&[(2, None, None)])))
        .await
        .unwrap();
    tablet.close().await.unwrap();
    log.close().await.unwrap();

    let log = harness.log(harness.stream(Some(stream_id)).await).await;
    let from = RecoverPoint::fresh(&log);
    let recovered = harness.tablet(log, harness.store(), from).await;
    assert_eq!(get(&recovered, 1), Some(row(1, Some("a2"), None)));
    assert_eq!(get(&recovered, 2), None);
    assert_eq!(recovered.row_count().await, 1);
    assert_eq!(recovered.flushed_log_offset().await, 5);
    assert_eq!(recovered.pending_rows().await, 0);

    recovered
        .put(Put::upsert(SchemaId(0), rows(&[(3, Some("c"), None)])))
        .await
        .unwrap();
    assert_eq!(recovered.row_count().await, 2);
    assert_eq!(recovered.flushed_log_offset().await, 6);
}

#[tokio::test]
async fn checkpoint_then_replay_only_the_tail() {
    let harness = Harness::new(plain()).await;
    let stream = harness.stream(None).await;
    let stream_id = stream.stream_id();
    let log = harness.log(stream).await;
    let tablet = harness
        .tablet(Arc::clone(&log), harness.store(), RecoverPoint::fresh(&log))
        .await;
    tablet
        .put(Put::upsert(
            SchemaId(0),
            rows(&[(1, Some("a"), None), (2, Some("b"), None)]),
        ))
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = dir.path().join("cp");
    let checkpoint = tablet.checkpoint(&checkpoint_dir).await.unwrap();
    assert_eq!(
        checkpoint.recover_point,
        RecoverPoint {
            log_offset: 2,
            row_count: 2,
            auto_increment: None,
        }
    );
    assert!(checkpoint.files.total_size() > 0);

    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a2"), None)])))
        .await
        .unwrap();
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(3, Some("c"), None)])))
        .await
        .unwrap();
    tablet.close().await.unwrap();
    log.close().await.unwrap();

    let store = MemoryEngine
        .restore(
            &dir.path().join("restored"),
            &checkpoint_dir,
            mink_kv::Options::default(),
        )
        .unwrap();
    let log = harness.log(harness.stream(Some(stream_id)).await).await;
    let recovered = harness.tablet(log, store, checkpoint.recover_point).await;
    assert_eq!(get(&recovered, 1), Some(row(1, Some("a2"), None)));
    assert_eq!(get(&recovered, 2), Some(row(2, Some("b"), None)));
    assert_eq!(get(&recovered, 3), Some(row(3, Some("c"), None)));
    assert_eq!(recovered.row_count().await, 3);
    assert_eq!(recovered.flushed_log_offset().await, 5);
}

#[tokio::test]
async fn older_schema_rows_are_remapped_by_field_id_on_put_lookup_and_recovery() {
    let harness = Harness::new(plain()).await;
    let stream = harness.stream(None).await;
    let stream_id = stream.stream_id();
    let log = harness.log(stream).await;
    let tablet = harness
        .tablet(Arc::clone(&log), harness.store(), RecoverPoint::fresh(&log))
        .await;
    tablet
        .put(Put::upsert(
            SchemaId(0),
            rows(&[(1, Some("a"), Some(1)), (2, Some("b"), Some(2))]),
        ))
        .await
        .unwrap();
    tablet.close().await.unwrap();
    log.close().await.unwrap();

    let evolved = mink_table::alter_table(
        &harness.descriptor,
        &[
            mink_table::Change::RenameColumn {
                name: "v".into(),
                new_name: "value".into(),
            },
            mink_table::Change::ModifyColumn {
                name: "n".into(),
                data_type: DataType::big_int(),
                comment: None,
            },
            mink_table::Change::add_column("extra", DataType::string()),
        ],
        None,
    )
    .unwrap();
    let schemas = Arc::new(Fixed::new(vec![
        harness.descriptor.schema().clone(),
        evolved.schema().clone(),
    ]));
    let open = |log: Arc<mink_log::Tablet>, store: Box<dyn Store>, from: RecoverPoint| {
        let schemas = Arc::clone(&schemas);
        let evolved = evolved.clone();
        let harness = &harness;
        async move {
            Tablet::open(
                harness.bucket,
                &evolved,
                schemas,
                harness.sequence.clone(),
                log,
                store,
                from,
                Config {
                    compression: Compression::None,
                    ..Config::default()
                },
            )
            .await
            .unwrap()
        }
    };
    let log = harness.log(harness.stream(Some(stream_id)).await).await;
    let tablet = open(Arc::clone(&log), harness.store(), RecoverPoint::fresh(&log)).await;

    type Wide = (i64, Option<String>, Option<i64>, Option<String>);
    let latest = tablet.latest().unwrap();
    let read = |tablet: &Tablet, k: i64| -> Option<Wide> {
        let value = tablet.lookup(&key(k)).unwrap()?;
        let batch = tablet.to_batch([value]).unwrap();
        assert_eq!(batch.schema(), latest.arrow);
        let k = batch.column(0).as_primitive::<Int64Type>();
        let v = batch.column(1).as_string::<i32>();
        let n = batch.column(2).as_primitive::<Int64Type>();
        let e = batch.column(3).as_string::<i32>();
        Some((
            k.value(0),
            v.is_valid(0).then(|| v.value(0).to_owned()),
            n.is_valid(0).then(|| n.value(0)),
            e.is_valid(0).then(|| e.value(0).to_owned()),
        ))
    };
    assert_eq!(
        read(&tablet, 1),
        Some((1, Some("a".into()), Some(1), None)),
        "recovered from schema 0 batches"
    );

    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a2"), Some(10))])))
        .await
        .unwrap();
    let wide = RecordBatch::try_new(
        latest.arrow.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(Int64Array::from(vec![i64::from(i32::MAX) + 1])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();
    tablet.put(Put::upsert(SchemaId(1), wide)).await.unwrap();
    tablet
        .put(Put::delete(SchemaId(0), rows(&[(2, None, None)])))
        .await
        .unwrap();
    assert_eq!(
        read(&tablet, 1),
        Some((1, Some("a2".into()), Some(10), None))
    );
    assert_eq!(
        read(&tablet, 3),
        Some((
            3,
            Some("c".into()),
            Some(i64::from(i32::MAX) + 1),
            Some("x".into())
        ))
    );
    assert_eq!(read(&tablet, 2), None);

    let partial =
        Put::upsert(SchemaId(0), rows(&[(1, None, Some(11))])).with_target_columns(vec![0, 2]);
    tablet.put(partial).await.unwrap();
    assert_eq!(
        read(&tablet, 1),
        Some((1, Some("a2".into()), Some(11), None))
    );

    tablet.close().await.unwrap();
    log.close().await.unwrap();
    let log = harness.log(harness.stream(Some(stream_id)).await).await;
    let recovered = open(Arc::clone(&log), harness.store(), RecoverPoint::fresh(&log)).await;
    assert_eq!(
        read(&recovered, 1),
        Some((1, Some("a2".into()), Some(11), None))
    );
    assert_eq!(
        read(&recovered, 3),
        Some((
            3,
            Some("c".into()),
            Some(i64::from(i32::MAX) + 1),
            Some("x".into())
        ))
    );
    assert_eq!(read(&recovered, 2), None);
    assert_eq!(recovered.row_count().await, 2);
}

#[tokio::test]
async fn value_bytes_carry_the_schema_id() {
    let harness = Harness::new(plain()).await;
    let tablet = harness.fresh().await;
    tablet
        .put(Put::upsert(SchemaId(0), rows(&[(1, None, None)])))
        .await
        .unwrap();
    let raw: Bytes = tablet.lookup(&key(1)).unwrap().unwrap();
    assert_eq!(raw.as_ref(), [0x00, 0x00, 0b0000_0110, 0x01]);
    assert_eq!(harness.descriptor.options().kv_format, KvFormat::Compacted);
}

fn without_n(rows: RecordBatch) -> Put {
    Put::upsert(SchemaId(0), rows).with_target_columns(vec![0, 1])
}

#[tokio::test]
async fn auto_increment_fills_inserts_and_keeps_updates() {
    let harness = Harness::new(auto_increment()).await;
    let tablet = harness.fresh().await;

    assert!(matches!(
        tablet
            .put(Put::upsert(SchemaId(0), rows(&[(1, Some("a"), None)])))
            .await,
        Err(Error::AutoIncrementTargets(c)) if c == "n"
    ));
    assert!(matches!(
        tablet
            .put(
                Put::upsert(SchemaId(0), rows(&[(1, Some("a"), Some(7))]))
                    .with_target_columns(vec![0, 1, 2])
            )
            .await,
        Err(Error::AutoIncrementTarget(c)) if c == "n"
    ));
    assert_eq!(harness.sequence.0.load(Ordering::SeqCst), 0);

    tablet
        .put(without_n(rows(&[
            (1, Some("a"), Some(99)),
            (2, Some("b"), None),
        ])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a"), Some(1))));
    assert_eq!(get(&tablet, 2), Some(row(2, Some("b"), Some(2))));

    tablet
        .put(without_n(rows(&[(1, Some("a2"), None)])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 1), Some(row(1, Some("a2"), Some(1))));

    tablet
        .put(without_n(rows(&[
            (3, Some("c"), None),
            (4, Some("d"), None),
        ])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 3), Some(row(3, Some("c"), Some(3))));
    assert_eq!(get(&tablet, 4), Some(row(4, Some("d"), Some(4))));
    assert_eq!(harness.sequence.0.load(Ordering::SeqCst), 6);

    let log = changelog(&tablet).await;
    let inserts: Vec<Option<i32>> = log
        .iter()
        .filter(|(_, change, _)| *change == ChangeType::Insert)
        .map(|(_, _, row)| row.2)
        .collect();
    assert_eq!(inserts, vec![Some(1), Some(2), Some(3), Some(4)]);

    tablet
        .put(Put::delete(SchemaId(0), rows(&[(2, None, None)])).with_target_columns(vec![0, 1]))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 2), Some(row(2, None, Some(2))));
    tablet
        .put(without_n(rows(&[(2, Some("b2"), None)])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 2), Some(row(2, Some("b2"), Some(2))));
    assert_eq!(harness.sequence.0.load(Ordering::SeqCst), 6);
}

#[tokio::test]
async fn auto_increment_range_survives_checkpoint_and_recovery() {
    let harness = Harness::new(auto_increment()).await;
    let stream = harness.stream(None).await;
    let stream_id = stream.stream_id();
    let log = harness.log(stream).await;
    let tablet = harness
        .tablet(Arc::clone(&log), harness.store(), RecoverPoint::fresh(&log))
        .await;
    tablet
        .put(without_n(rows(&[
            (1, Some("a"), None),
            (2, Some("b"), None),
        ])))
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = dir.path().join("cp");
    let checkpoint = tablet.checkpoint(&checkpoint_dir).await.unwrap();
    assert_eq!(
        checkpoint.recover_point.auto_increment,
        Some(IdRange {
            column_id: 2,
            start: 3,
            end: 3,
        })
    );

    tablet
        .put(without_n(rows(&[
            (3, Some("c"), None),
            (4, Some("d"), None),
        ])))
        .await
        .unwrap();
    tablet.close().await.unwrap();
    log.close().await.unwrap();

    let store = MemoryEngine
        .restore(
            &dir.path().join("restored"),
            &checkpoint_dir,
            mink_kv::Options::default(),
        )
        .unwrap();
    let log = harness.log(harness.stream(Some(stream_id)).await).await;
    let tablet = harness
        .tablet(Arc::clone(&log), store, checkpoint.recover_point)
        .await;
    assert_eq!(get(&tablet, 4), Some(row(4, Some("d"), Some(4))));

    let fetched = harness.sequence.0.load(Ordering::SeqCst);
    tablet
        .put(without_n(rows(&[
            (5, Some("e"), None),
            (6, Some("f"), None),
        ])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 5), Some(row(5, Some("e"), Some(5))));
    assert_eq!(get(&tablet, 6), Some(row(6, Some("f"), Some(6))));
    assert_eq!(harness.sequence.0.load(Ordering::SeqCst), fetched);
    tablet
        .put(without_n(rows(&[(7, Some("g"), None)])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 7), Some(row(7, Some("g"), Some(7))));
    tablet.close().await.unwrap();
    log.close().await.unwrap();

    let log = harness.log(harness.stream(Some(stream_id)).await).await;
    let tablet = harness
        .tablet(Arc::clone(&log), harness.store(), RecoverPoint::fresh(&log))
        .await;
    assert_eq!(tablet.row_count().await, 7);
    tablet
        .put(without_n(rows(&[(8, Some("h"), None)])))
        .await
        .unwrap();
    assert_eq!(get(&tablet, 8), Some(row(8, Some("h"), Some(10))));
}
