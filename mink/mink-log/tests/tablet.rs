//! End-to-end log behavior on an in-memory stream: appends, reads, dedup, replay and timestamps.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use mink_common::{Clock, ManualClock};
use mink_log::{Config, Error, FetchIsolation, Kv, Snapshot, SnapshotStore, Tablet};
use mink_record::header::NO_LEADER_EPOCH;
use mink_record::{Batch, ChangeType, Compression, Projection, Spec, build, codec};
use mink_table::{Bucket, BucketId, Id, LogFormat, SchemaId};
use s3stream::{
    CreateStreamOptions, MemoryKvClient, MemoryMetadataManager, MemoryObjectStorage,
    ObjectStorageTrait, ObjectWalConfig, ObjectWalService, OpenStreamOptions, S3StreamBuilder,
    S3StreamEngine, Stream,
};

struct Harness {
    engine: S3StreamEngine,
    snapshots: Arc<Kv>,
    clock: Arc<ManualClock>,
    bucket: Bucket,
}

impl Harness {
    async fn new() -> Self {
        let manager = MemoryMetadataManager::new();
        let data: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(0));
        let wal: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(1));
        let mut wal_config = ObjectWalConfig::defaults();
        wal_config.cluster_id = "mink-log-test".into();
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
            bucket: Bucket::new(Id(1), BucketId(0)),
        }
    }

    async fn create(&self) -> Arc<dyn Stream> {
        self.engine
            .stream_client()
            .create_and_open_stream(CreateStreamOptions {
                epoch: 1,
                ..Default::default()
            })
            .await
            .unwrap()
    }

    async fn reopen(&self, stream_id: u64, epoch: u64) -> Arc<dyn Stream> {
        self.engine
            .stream_client()
            .open_stream(
                stream_id,
                OpenStreamOptions {
                    epoch,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    }

    async fn tablet(&self, stream: Arc<dyn Stream>, config: Config) -> Tablet {
        Tablet::open(
            self.bucket,
            stream,
            self.snapshots.clone(),
            self.clock.clone(),
            config,
        )
        .await
        .unwrap()
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn rows(from: i32, count: i32) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from((from..from + count).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (from..from + count)
                    .map(|i| Some(format!("n{i}")))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn batch(count: i32, writer: Option<(i64, i32)>) -> Bytes {
    let mut spec = Spec::new(SchemaId(1), true);
    if let Some((id, seq)) = writer {
        spec = spec.with_writer(id, seq);
    }
    let codec = codec(LogFormat::Arrow, Compression::None);
    let bytes = build(
        spec,
        &vec![ChangeType::AppendOnly; count as usize],
        &rows(0, count),
        codec.as_ref(),
    )
    .unwrap();
    Bytes::from(bytes)
}

fn headers(info: &mink_log::FetchInfo) -> Vec<(i64, i64, i64)> {
    info.batches
        .iter()
        .map(|b| {
            let h = *Batch::parse(b.clone()).unwrap().header();
            (h.base_offset, h.last_offset(), h.commit_timestamp)
        })
        .collect()
}

#[tokio::test]
async fn offset_for_timestamp_finds_the_first_batch_at_or_after() {
    let h = Harness::new().await;
    let tablet = h.tablet(h.create().await, Config::default()).await;
    assert_eq!(tablet.offset_for_timestamp(0).await.unwrap(), 0);

    tablet.append(batch(3, None)).await.unwrap();
    h.clock.advance(Duration::from_millis(10));
    tablet.append(batch(2, None)).await.unwrap();
    h.clock.advance(Duration::from_millis(10));
    tablet.append(batch(4, None)).await.unwrap();
    tablet.append(batch(1, None)).await.unwrap();

    let lookup = |ts| tablet.offset_for_timestamp(ts);
    assert_eq!(lookup(999_999).await.unwrap(), 0);
    assert_eq!(lookup(1_000_000).await.unwrap(), 0);
    assert_eq!(lookup(1_000_001).await.unwrap(), 3);
    assert_eq!(lookup(1_000_010).await.unwrap(), 3);
    assert_eq!(lookup(1_000_015).await.unwrap(), 5);
    assert_eq!(lookup(1_000_020).await.unwrap(), 5);
    h.clock.advance(Duration::from_millis(10));
    assert_eq!(lookup(1_000_025).await.unwrap(), 10);
    assert!(matches!(
        lookup(1_000_031).await,
        Err(Error::InvalidTimestamp { .. })
    ));
}

#[tokio::test]
async fn append_assigns_offsets_and_timestamps_and_reads_them_back() {
    let h = Harness::new().await;
    let tablet = h.tablet(h.create().await, Config::default()).await;
    assert!(tablet.offsets().is_empty());

    let first = tablet.append(batch(3, None)).await.unwrap();
    assert_eq!((first.first_offset, first.last_offset), (0, 2));
    assert_eq!(first.max_timestamp, 1_000_000);
    assert!(!first.duplicated);

    h.clock.advance(Duration::from_millis(5));
    let mut two = batch(2, None).to_vec();
    two.extend_from_slice(&batch(4, None));
    let second = tablet.append(Bytes::from(two)).await.unwrap();
    assert_eq!((second.first_offset, second.last_offset), (3, 8));
    assert_eq!(second.batch_count, 2);
    assert_eq!(second.max_timestamp, 1_000_005);

    let offsets = tablet.offsets();
    assert_eq!(
        (offsets.log_start, offsets.high_watermark, offsets.log_end),
        (0, 9, 9)
    );

    let all = tablet
        .read(0, usize::MAX, FetchIsolation::HighWatermark, None)
        .await
        .unwrap();
    assert_eq!(
        headers(&all),
        vec![(0, 2, 1_000_000), (3, 4, 1_000_005), (5, 8, 1_000_005)]
    );
    for bytes in &all.batches {
        let parsed = Batch::parse(bytes.clone()).unwrap();
        parsed.ensure_valid().unwrap();
        assert_eq!(parsed.header().leader_epoch, NO_LEADER_EPOCH);
    }

    let tail = tablet
        .read(6, usize::MAX, FetchIsolation::HighWatermark, None)
        .await
        .unwrap();
    assert_eq!(headers(&tail), vec![(5, 8, 1_000_005)]);

    let budget = tablet
        .read(0, 1, FetchIsolation::HighWatermark, None)
        .await
        .unwrap();
    assert_eq!(budget.batches.len(), 1);

    let empty = tablet.append(Bytes::new()).await.unwrap();
    assert_eq!(empty.batch_count, 0);
    assert_eq!(tablet.log_end_offset(), 9);
    h.engine.shutdown().await;
}

#[tokio::test]
async fn commit_timestamp_never_goes_backwards() {
    let h = Harness::new().await;
    let tablet = h.tablet(h.create().await, Config::default()).await;
    tablet.append(batch(1, None)).await.unwrap();
    h.clock.set(10);
    let info = tablet.append(batch(1, None)).await.unwrap();
    assert_eq!(info.max_timestamp, 1_000_000);
    h.engine.shutdown().await;
}

#[tokio::test]
async fn read_bounds_are_enforced() {
    let h = Harness::new().await;
    let tablet = h.tablet(h.create().await, Config::default()).await;
    tablet.append(batch(2, None)).await.unwrap();

    let at_end = tablet
        .read(2, usize::MAX, FetchIsolation::HighWatermark, None)
        .await
        .unwrap();
    assert!(at_end.is_empty());
    assert_eq!((at_end.high_watermark, at_end.log_end_offset), (2, 2));

    assert!(matches!(
        tablet
            .read(3, usize::MAX, FetchIsolation::LogEnd, None)
            .await
            .unwrap_err(),
        Error::OutOfRange {
            offset: 3,
            start: 0,
            end: 2
        }
    ));

    tablet.trim(2).await.unwrap();
    assert_eq!(tablet.log_start_offset(), 2);
    assert!(matches!(
        tablet
            .read(1, usize::MAX, FetchIsolation::HighWatermark, None)
            .await
            .unwrap_err(),
        Error::OutOfRange { offset: 1, .. }
    ));
    h.engine.shutdown().await;
}

#[tokio::test]
async fn projection_is_applied_per_batch() {
    let h = Harness::new().await;
    let tablet = h.tablet(h.create().await, Config::default()).await;
    tablet.append(batch(3, None)).await.unwrap();
    let projection = Projection::new(&schema(), &[1]).unwrap();
    let info = tablet
        .read(
            0,
            usize::MAX,
            FetchIsolation::HighWatermark,
            Some(&projection),
        )
        .await
        .unwrap();
    let projected = Batch::parse(info.batches[0].clone()).unwrap();
    let codec = codec(LogFormat::Arrow, Compression::None);
    let records = projected
        .records(
            codec.as_ref(),
            schema().project(&[1]).map(Arc::new).unwrap(),
            None,
        )
        .unwrap();
    assert_eq!(records.batch, rows(0, 3).project(&[1]).unwrap());
    h.engine.shutdown().await;
}

#[tokio::test]
async fn idempotent_writers_dedup_and_reject_gaps() {
    let h = Harness::new().await;
    let tablet = h.tablet(h.create().await, Config::default()).await;

    assert!(matches!(
        tablet.append(batch(1, Some((7, 4)))).await.unwrap_err(),
        Error::OutOfOrderSequence {
            writer_id: 7,
            incoming: 4,
            current: -1
        }
    ));

    let first = tablet.append(batch(2, Some((7, 0)))).await.unwrap();
    let second = tablet.append(batch(3, Some((7, 1)))).await.unwrap();
    assert_eq!((second.first_offset, second.last_offset), (2, 4));

    let replay = tablet.append(batch(2, Some((7, 0)))).await.unwrap();
    assert!(replay.duplicated);
    assert_eq!(
        (
            replay.first_offset,
            replay.last_offset,
            replay.max_timestamp
        ),
        (first.first_offset, first.last_offset, first.max_timestamp)
    );
    assert_eq!(tablet.log_end_offset(), 5);

    assert!(matches!(
        tablet.append(batch(1, Some((7, 3)))).await.unwrap_err(),
        Error::OutOfOrderSequence {
            incoming: 3,
            current: 1,
            ..
        }
    ));
    assert_eq!(tablet.log_end_offset(), 5);

    let other = tablet.append(batch(1, Some((8, 0)))).await.unwrap();
    assert_eq!(other.first_offset, 5);
    assert_eq!(tablet.writer_count(), 2);

    h.clock.advance(Duration::from_secs(8 * 24 * 60 * 60));
    tablet.remove_expired_writers();
    assert_eq!(tablet.writer_count(), 0);
    let fresh = tablet.append(batch(1, Some((7, 0)))).await.unwrap();
    assert!(!fresh.duplicated);
    h.engine.shutdown().await;
}

#[tokio::test]
async fn writer_state_survives_reopen_via_snapshot_and_replay() {
    let h = Harness::new().await;
    let stream = h.create().await;
    let stream_id = stream.stream_id();
    let tablet = h.tablet(stream, Config::default()).await;

    tablet.append(batch(2, Some((7, 0)))).await.unwrap();
    tablet.append(batch(1, Some((7, 1)))).await.unwrap();
    let snapshot = tablet.snapshot().await.unwrap();
    assert_eq!(snapshot.offset, 3);
    assert_eq!(snapshot.writers.len(), 1);
    assert_eq!(snapshot.writers[0].last_batch_sequence, 1);

    tablet.append(batch(1, Some((7, 2)))).await.unwrap();
    tablet.append(batch(1, Some((9, 0)))).await.unwrap();
    tablet.close().await.unwrap();
    let stored = h.snapshots.load(&h.bucket).await.unwrap().unwrap();
    assert_eq!(stored.offset, 5);
    assert_eq!(stored.writers.len(), 2);

    h.snapshots.store(&h.bucket, &snapshot).await.unwrap();
    let reopened = h
        .tablet(h.reopen(stream_id, 2).await, Config::default())
        .await;
    assert_eq!(reopened.writer_count(), 2);
    assert_eq!(reopened.log_end_offset(), 5);

    let duplicate = reopened.append(batch(1, Some((7, 2)))).await.unwrap();
    assert!(duplicate.duplicated);
    assert_eq!((duplicate.first_offset, duplicate.last_offset), (3, 3));
    assert!(matches!(
        reopened.append(batch(1, Some((9, 2)))).await.unwrap_err(),
        Error::OutOfOrderSequence {
            writer_id: 9,
            incoming: 2,
            current: 0
        }
    ));
    let next = reopened.append(batch(1, Some((7, 3)))).await.unwrap();
    assert_eq!(next.first_offset, 5);
    assert_eq!(next.max_timestamp, h.clock.millis());
    h.engine.shutdown().await;
}

#[tokio::test]
async fn reopen_without_snapshot_replays_from_start() {
    let h = Harness::new().await;
    let stream = h.create().await;
    let stream_id = stream.stream_id();
    let tablet = h.tablet(stream, Config::default()).await;
    tablet.append(batch(1, Some((7, 0)))).await.unwrap();
    h.clock.advance(Duration::from_millis(3));
    tablet.append(batch(1, Some((7, 1)))).await.unwrap();
    tablet.close().await.unwrap();
    h.snapshots.remove(&h.bucket).await.unwrap();

    h.clock.set(1);
    let reopened = h
        .tablet(h.reopen(stream_id, 2).await, Config::default())
        .await;
    assert_eq!(reopened.writer_count(), 1);
    let info = reopened.append(batch(1, Some((7, 2)))).await.unwrap();
    assert_eq!(info.first_offset, 2);
    assert_eq!(info.max_timestamp, 1_000_003);

    reopened.destroy().await.unwrap();
    assert!(h.snapshots.load(&h.bucket).await.unwrap().is_none());
    h.engine.shutdown().await;
}

#[tokio::test]
async fn expired_snapshot_entries_are_dropped_on_open() {
    let h = Harness::new().await;
    let stream = h.create().await;
    let stream_id = stream.stream_id();
    let config = Config {
        writer_expiration: Duration::from_secs(60),
    };
    let tablet = h.tablet(stream, config).await;
    tablet.append(batch(1, Some((7, 0)))).await.unwrap();
    tablet.close().await.unwrap();

    h.clock.advance(Duration::from_secs(120));
    let reopened = h.tablet(h.reopen(stream_id, 2).await, config).await;
    assert_eq!(reopened.writer_count(), 0);
    let _: &dyn SnapshotStore = h.snapshots.as_ref();
    let _: Snapshot = reopened.snapshot().await.unwrap();
    h.engine.shutdown().await;
}

#[tokio::test]
async fn wait_past_returns_on_append_or_timeout() {
    let h = Harness::new().await;
    let tablet = h.tablet(h.create().await, Config::default()).await;
    tablet.append(batch(2, None)).await.unwrap();

    assert_eq!(tablet.wait_past(1, Duration::from_secs(5)).await, 2);
    assert_eq!(tablet.wait_past(2, Duration::from_millis(20)).await, 2);
    let (woken, _) = tokio::join!(tablet.wait_past(2, Duration::from_secs(5)), async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        tablet.append(batch(3, None)).await.unwrap();
    });
    assert_eq!(woken, 5);
}
