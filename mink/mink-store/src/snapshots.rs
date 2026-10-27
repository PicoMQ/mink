use mink_core::{Assembly, IndexedFile, Object, Offsets, Partition, Snapshot};
use sqlx::{PgConnection, PgPool};

use crate::Error;
use crate::files::{self, listed_in, remove_unlisted};
use crate::leases::{Lease, check};
use crate::objects::{record_commit, unreference_through};
use crate::ranges::{check_boundary, lock};

pub struct Snapshots<'a> {
    pool: &'a PgPool,
}

impl<'a> Snapshots<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Snapshots<'a> {
        Snapshots { pool }
    }

    pub async fn commit(
        &self,
        lease: &Lease,
        snapshot: &Snapshot,
        written: &[Object],
        released: &[Object],
        assembled: &[Assembly],
    ) -> Result<(), Error> {
        let topic = lease.require_topic()?;
        if snapshot.topic() != topic {
            return Err(Error::LeaseScope(lease.scope().clone()));
        }

        if snapshot.epoch() != lease.epoch() {
            return Err(Error::Fenced(lease.scope().clone()));
        }

        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;
        let latest = latest(&mut transaction, topic).await?;

        check_id(topic, snapshot, latest.as_ref())?;
        check_offsets(snapshot, latest.as_ref())?;
        for offsets in snapshot.offsets() {
            let range = lock(&mut transaction, offsets.partition()).await?;
            check_boundary(&mut transaction, &range, offsets.start_offset()).await?;
            check_boundary(&mut transaction, &range, offsets.end_offset()).await?;
        }

        insert(&mut transaction, snapshot).await?;
        let latest_id = latest.as_ref().map(Snapshot::id);
        record_commit(&mut transaction, snapshot, latest_id, written, released).await?;

        for assembly in assembled {
            add_assembly(&mut transaction, snapshot, assembly).await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    pub async fn expire(&self, lease: &Lease, through_id: i64) -> Result<u64, Error> {
        let topic = lease.require_topic()?;
        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;

        let latest = latest_id(&mut transaction, topic).await?.unwrap_or(0);
        if through_id >= latest {
            return Err(Error::LatestSnapshot {
                topic: topic.to_owned(),
                id: through_id,
            });
        }

        unreference_through(&mut transaction, topic, through_id).await?;
        let expired = sqlx::query("DELETE FROM snapshot WHERE topic = $1 AND id <= $2")
            .bind(topic)
            .bind(through_id)
            .execute(&mut *transaction)
            .await?;
        remove_unlisted(&mut transaction, topic).await?;

        transaction.commit().await?;
        Ok(expired.rows_affected())
    }

    pub async fn latest(&self, topic: &str) -> Result<Option<Snapshot>, Error> {
        let mut connection = self.pool.acquire().await?;

        latest(&mut connection, topic).await
    }

    pub async fn page(
        &self,
        topic: &str,
        before: Option<i64>,
        limit: i64,
    ) -> Result<Vec<Snapshot>, Error> {
        let rows: Vec<(i64, i64, i64)> = sqlx::query_as(
            "SELECT id, timestamp_ms, epoch FROM snapshot \
             WHERE topic = $1 AND ($2::BIGINT IS NULL OR id < $2) ORDER BY id DESC LIMIT $3",
        )
        .bind(topic)
        .bind(before)
        .bind(limit)
        .fetch_all(self.pool)
        .await?;
        let ids: Vec<i64> = rows.iter().map(|(id, _, _)| *id).collect();

        let offsets: Vec<(i64, i32, i64, i64)> = sqlx::query_as(
            "SELECT snapshot_id, partition, start_offset, end_offset FROM snapshot_offsets \
             WHERE topic = $1 AND snapshot_id = ANY($2) ORDER BY snapshot_id, partition",
        )
        .bind(topic)
        .bind(&ids)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter()
            .map(|(id, timestamp_ms, epoch)| {
                let offsets = offsets
                    .iter()
                    .filter(|(snapshot_id, ..)| *snapshot_id == id)
                    .map(|(_, partition, start, end)| {
                        Ok(Offsets::new(
                            Partition::new(topic, *partition)?,
                            *start,
                            *end,
                        )?)
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                Ok(Snapshot::new(id, timestamp_ms, epoch, offsets)?)
            })
            .collect()
    }

    pub async fn get(&self, topic: &str, id: i64) -> Result<Option<Snapshot>, Error> {
        let page = self.page(topic, Some(id + 1), 1).await?;

        Ok(page.into_iter().find(|snapshot| snapshot.id() == id))
    }

    pub async fn totals(
        &self,
        topic: &str,
        ids: &[i64],
    ) -> Result<Vec<(i64, SnapshotTotals)>, Error> {
        let rows: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
            "WITH wanted AS ( \
                 SELECT snapshot.id, \
                     (SELECT max(earlier.id) FROM snapshot earlier \
                      WHERE earlier.topic = $1 AND earlier.id < snapshot.id) AS previous \
                 FROM snapshot WHERE snapshot.topic = $1 AND snapshot.id = ANY($2) \
             ), live AS ( \
                 SELECT wanted.id AS snapshot_id, wanted.previous, file.partition, \
                     file.start_offset, file.end_offset, file.added_snapshot_id, \
                     file.replaced_snapshot_id, (file.stats->>'records')::BIGINT AS records \
                 FROM wanted \
                 JOIN snapshot_offsets offsets ON offsets.topic = $1 \
                     AND offsets.snapshot_id = wanted.id \
                 JOIN file ON file.topic = $1 AND file.partition = offsets.partition \
                     AND covers(offsets.start_offset, offsets.end_offset, \
                         file.start_offset, file.end_offset) \
                     AND (file.replaced_snapshot_id IS NULL OR file.replaced_snapshot_id > wanted.id) \
                     AND (file.added_snapshot_id IS NULL OR file.added_snapshot_id <= wanted.id) \
             ) \
             SELECT wanted.id, \
                 coalesce((SELECT sum(records) FROM live WHERE live.snapshot_id = wanted.id), 0)::BIGINT, \
                 (SELECT count(*) FROM live WHERE live.snapshot_id = wanted.id AND NOT ( \
                     live.previous IS NOT NULL \
                     AND (live.replaced_snapshot_id IS NULL OR live.replaced_snapshot_id > live.previous) \
                     AND (live.added_snapshot_id IS NULL OR live.added_snapshot_id <= live.previous) \
                     AND EXISTS (SELECT 1 FROM snapshot_offsets earlier \
                         WHERE earlier.topic = $1 AND earlier.snapshot_id = live.previous \
                         AND earlier.partition = live.partition \
                         AND covers(earlier.start_offset, earlier.end_offset, \
                             live.start_offset, live.end_offset)))), \
                 (SELECT count(*) FROM file \
                  WHERE file.topic = $1 AND file.replaced_snapshot_id = wanted.id) \
             FROM wanted",
        )
        .bind(topic)
        .bind(ids)
        .fetch_all(self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(id, records, added_files, replaced_files)| {
                let totals = SnapshotTotals {
                    records: records as u64,
                    added_files: added_files as u64,
                    replaced_files: replaced_files as u64,
                };

                (id, totals)
            })
            .collect())
    }

    pub async fn list(&self, topic: &str) -> Result<Vec<Snapshot>, Error> {
        let mut connection = self.pool.acquire().await?;
        let ids: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM snapshot WHERE topic = $1 ORDER BY id")
                .bind(topic)
                .fetch_all(&mut *connection)
                .await?;

        let mut snapshots = Vec::with_capacity(ids.len());
        for id in ids {
            snapshots.push(load(&mut connection, topic, id).await?);
        }

        Ok(snapshots)
    }

    pub async fn topics(&self) -> Result<Vec<String>, Error> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT snapshot.topic FROM snapshot \
                 JOIN topic ON topic.name = snapshot.topic AND topic.state = 'indexing' \
                 ORDER BY snapshot.topic",
        )
        .fetch_all(self.pool)
        .await?)
    }

    pub async fn files(&self, snapshot: &Snapshot) -> Result<Vec<IndexedFile>, Error> {
        let mut files = Vec::new();

        for offsets in snapshot.offsets() {
            files.extend(listed_in(self.pool, offsets, snapshot.id()).await?);
        }

        Ok(files)
    }
}

async fn add_assembly(
    connection: &mut PgConnection,
    snapshot: &Snapshot,
    assembly: &Assembly,
) -> Result<(), Error> {
    let assembled = assembly.built().indexed().file();
    let covered = snapshot
        .offsets_of(assembled.partition())
        .is_some_and(|offsets| offsets.covers(assembled));
    if !covered {
        return Err(Error::MixedAssembly(assembled.clone()));
    }

    let mut flattened = Vec::new();
    for replaced in assembly.replaced() {
        match files::stored_parts(connection, replaced).await? {
            Some(parts) => flattened.extend(parts),
            None => flattened.push(replaced.range()),
        }

        files::replace(connection, replaced, snapshot.id()).await?;
    }

    if flattened != assembly.parts() {
        return Err(Error::MixedAssembly(assembled.clone()));
    }

    files::insert_assembled(connection, assembly, snapshot.id()).await
}

fn check_id(topic: &str, snapshot: &Snapshot, latest: Option<&Snapshot>) -> Result<(), Error> {
    let expected = Snapshot::next_id(latest.map(Snapshot::id));

    match snapshot.id() == expected {
        true => Ok(()),
        false => Err(Error::SnapshotConflict {
            topic: topic.to_owned(),
            expected,
            given: snapshot.id(),
        }),
    }
}

fn check_offsets(snapshot: &Snapshot, latest: Option<&Snapshot>) -> Result<(), Error> {
    for before in latest.iter().flat_map(|latest| latest.offsets()) {
        let partition = before.partition();
        let after = snapshot
            .offsets_of(partition)
            .ok_or_else(|| Error::MissingPartition(partition.clone()))?;

        if after.end_offset() < before.end_offset() {
            return Err(Error::OffsetsMovedBack(partition.clone()));
        }
    }

    Ok(())
}

async fn insert(connection: &mut PgConnection, snapshot: &Snapshot) -> Result<(), Error> {
    sqlx::query("INSERT INTO snapshot (topic, id, timestamp_ms, epoch) VALUES ($1, $2, $3, $4)")
        .bind(snapshot.topic())
        .bind(snapshot.id())
        .bind(snapshot.timestamp_ms())
        .bind(snapshot.epoch())
        .execute(&mut *connection)
        .await?;

    for offsets in snapshot.offsets() {
        sqlx::query(
            "INSERT INTO snapshot_offsets (topic, snapshot_id, partition, start_offset, end_offset) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(snapshot.topic())
        .bind(snapshot.id())
        .bind(offsets.partition().index())
        .bind(offsets.start_offset())
        .bind(offsets.end_offset())
        .execute(&mut *connection)
        .await?;
    }

    Ok(())
}

pub struct SnapshotTotals {
    records: u64,
    added_files: u64,
    replaced_files: u64,
}

impl SnapshotTotals {
    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn added_files(&self) -> u64 {
        self.added_files
    }

    pub fn replaced_files(&self) -> u64 {
        self.replaced_files
    }
}

async fn latest_id(connection: &mut PgConnection, topic: &str) -> Result<Option<i64>, Error> {
    Ok(
        sqlx::query_scalar("SELECT MAX(id) FROM snapshot WHERE topic = $1")
            .bind(topic)
            .fetch_one(&mut *connection)
            .await?,
    )
}

async fn latest(connection: &mut PgConnection, topic: &str) -> Result<Option<Snapshot>, Error> {
    match latest_id(connection, topic).await? {
        Some(id) => Ok(Some(load(connection, topic, id).await?)),
        None => Ok(None),
    }
}

async fn load(connection: &mut PgConnection, topic: &str, id: i64) -> Result<Snapshot, Error> {
    let (timestamp_ms, epoch): (i64, i64) =
        sqlx::query_as("SELECT timestamp_ms, epoch FROM snapshot WHERE topic = $1 AND id = $2")
            .bind(topic)
            .bind(id)
            .fetch_one(&mut *connection)
            .await?;
    let rows: Vec<(i32, i64, i64)> = sqlx::query_as(
        "SELECT partition, start_offset, end_offset FROM snapshot_offsets \
         WHERE topic = $1 AND snapshot_id = $2 ORDER BY partition",
    )
    .bind(topic)
    .bind(id)
    .fetch_all(&mut *connection)
    .await?;

    let offsets = rows
        .into_iter()
        .map(|(partition, start, end)| {
            Ok(Offsets::new(Partition::new(topic, partition)?, start, end)?)
        })
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(Snapshot::new(id, timestamp_ms, epoch, offsets)?)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mink_core::{DaySketches, ObjectKind, Scope, Topic};
    use proptest::prelude::*;
    use uuid::Uuid;

    use super::*;
    use crate::Store;
    use crate::testing::{self, INDEXER, NEXT_WRITER, TTL, WRITER, file, snapshot};

    type State = Vec<(Snapshot, Vec<IndexedFile>)>;

    async fn state(store: &Store, topic: &Topic) -> State {
        let mut state = Vec::new();

        for snapshot in store.snapshots().list(topic.name()).await.unwrap() {
            let files = store.snapshots().files(&snapshot).await.unwrap();
            state.push((snapshot, files));
        }

        state
    }

    fn assert_rules(state: &State) {
        for pair in state.windows(2) {
            let (before, after) = (&pair[0].0, &pair[1].0);
            assert_eq!(after.id(), before.id() + 1);
            for offsets in before.offsets() {
                let later = after.offsets_of(offsets.partition()).unwrap();
                assert!(later.end_offset() >= offsets.end_offset());
            }
        }

        for (snapshot, files) in state {
            for offsets in snapshot.offsets() {
                let mut next = offsets.start_offset();
                for file in files
                    .iter()
                    .map(IndexedFile::file)
                    .filter(|file| file.partition() == offsets.partition())
                {
                    assert_eq!(file.start_offset(), next, "{snapshot:?}");
                    next = file.end_offset();
                }

                assert_eq!(next, offsets.end_offset(), "{snapshot:?}");
            }
        }
    }

    async fn indexed(store: &Store, topic: &Topic, ranges: &[(i64, i64)]) -> Lease {
        let lease = testing::partition_lease(store, topic, 0, INDEXER).await;
        store
            .indexed_ranges()
            .start(&lease, ranges[0].0)
            .await
            .unwrap();
        let files: Vec<_> = ranges
            .iter()
            .map(|(start, end)| file(topic, 0, *start, *end))
            .collect();
        store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&files),
                &DaySketches::default(),
            )
            .await
            .unwrap();

        lease
    }

    #[tokio::test]
    async fn a_commit_with_the_wrong_id_conflicts() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        testing::started(&store, &topic).await;
        let lease = testing::topic_lease(&store, &topic, WRITER).await;

        let commit = store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 2, &[(0, 0)]),
                &[],
                &[],
                &[],
            )
            .await;

        assert!(
            matches!(
                commit,
                Err(Error::SnapshotConflict {
                    expected: 1,
                    given: 2,
                    ..
                })
            ),
            "{commit:?}"
        );
    }

    #[tokio::test]
    async fn a_commit_under_a_stale_lease_is_fenced() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        testing::started(&store, &topic).await;
        let scope = Scope::Topic(topic.name().to_owned());
        let stale = store
            .leases()
            .acquire(&scope, WRITER, Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        testing::topic_lease(&store, &topic, NEXT_WRITER).await;

        let commit = store
            .snapshots()
            .commit(
                &stale,
                &snapshot(&stale, &topic, 1, &[(0, 0)]),
                &[],
                &[],
                &[],
            )
            .await;

        assert!(matches!(commit, Err(Error::Fenced(_))), "{commit:?}");
        assert!(
            store
                .snapshots()
                .latest(topic.name())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_failed_commit_changes_nothing() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        indexed(&store, &topic, &[(0, 10), (10, 20)]).await;
        let lease = testing::topic_lease(&store, &topic, WRITER).await;
        let metadata = Object::new("v1.metadata.json", ObjectKind::TableMetadata);

        store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 1, &[(0, 10)]),
                std::slice::from_ref(&metadata),
                &[],
                &[],
            )
            .await
            .unwrap();

        let before = state(&store, &topic).await;
        let recorded = store.objects().list(topic.name()).await.unwrap();

        let written_twice = store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 2, &[(0, 20)]),
                &[metadata],
                &[],
                &[],
            )
            .await;

        assert!(
            matches!(written_twice, Err(Error::Sql(_))),
            "{written_twice:?}"
        );
        assert_eq!(state(&store, &topic).await, before);
        assert_eq!(store.objects().list(topic.name()).await.unwrap(), recorded);
    }

    #[tokio::test]
    async fn end_offsets_never_move_back_and_no_partition_is_dropped() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 2).await;
        let indexers = testing::started(&store, &topic).await;

        store
            .files()
            .add(
                &indexers[0],
                &mink_conformance::built(&[file(&topic, 0, 0, 10), file(&topic, 0, 10, 20)]),
                &DaySketches::default(),
            )
            .await
            .unwrap();
        let lease = testing::topic_lease(&store, &topic, WRITER).await;

        store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 1, &[(10, 20), (0, 0)]),
                &[],
                &[],
                &[],
            )
            .await
            .unwrap();

        let shorter = store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 2, &[(10, 10), (0, 0)]),
                &[],
                &[],
                &[],
            )
            .await;
        let missing = store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 2, &[(10, 20)]),
                &[],
                &[],
                &[],
            )
            .await;
        let earlier_start = store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 2, &[(0, 20), (0, 0)]),
                &[],
                &[],
                &[],
            )
            .await;

        assert!(
            matches!(shorter, Err(Error::OffsetsMovedBack(_))),
            "{shorter:?}"
        );
        assert!(
            matches!(missing, Err(Error::MissingPartition(_))),
            "{missing:?}"
        );
        assert!(earlier_start.is_ok(), "{earlier_start:?}");
    }

    #[tokio::test]
    async fn snapshot_offsets_lie_on_file_boundaries_inside_the_indexed_range() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        indexed(&store, &topic, &[(0, 10), (10, 20)]).await;
        let lease = testing::topic_lease(&store, &topic, WRITER).await;

        let inside_a_file = store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 1, &[(5, 20)]),
                &[],
                &[],
                &[],
            )
            .await;
        let past_the_end = store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 1, &[(0, 30)]),
                &[],
                &[],
                &[],
            )
            .await;
        let on_boundaries = store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 1, &[(10, 20)]),
                &[],
                &[],
                &[],
            )
            .await;

        assert!(
            matches!(inside_a_file, Err(Error::NotABoundary { offset: 5, .. })),
            "{inside_a_file:?}"
        );
        assert!(
            matches!(past_the_end, Err(Error::NotABoundary { offset: 30, .. })),
            "{past_the_end:?}"
        );
        assert!(on_boundaries.is_ok(), "{on_boundaries:?}");
    }

    #[tokio::test]
    async fn a_snapshot_contains_exactly_the_files_inside_its_offsets() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let indexer = indexed(&store, &topic, &[(0, 5), (5, 10)]).await;
        let lease = testing::topic_lease(&store, &topic, WRITER).await;
        let first = snapshot(&lease, &topic, 1, &[(0, 10)]);

        store
            .snapshots()
            .commit(&lease, &first, &[], &[], &[])
            .await
            .unwrap();
        store
            .files()
            .add(
                &indexer,
                &mink_conformance::built(&[file(&topic, 0, 10, 15)]),
                &DaySketches::default(),
            )
            .await
            .unwrap();
        let second = snapshot(&lease, &topic, 2, &[(5, 15)]);

        store
            .snapshots()
            .commit(&lease, &second, &[], &[], &[])
            .await
            .unwrap();

        let ranges = |files: Vec<IndexedFile>| -> Vec<_> {
            files
                .into_iter()
                .map(|indexed| (indexed.file().start_offset(), indexed.file().end_offset()))
                .collect()
        };
        assert_eq!(
            ranges(store.snapshots().files(&first).await.unwrap()),
            [(0, 5), (5, 10)]
        );
        assert_eq!(
            ranges(store.snapshots().files(&second).await.unwrap()),
            [(5, 10), (10, 15)]
        );
    }

    #[tokio::test]
    async fn expiry_removes_snapshots_offsets_and_uncovered_files() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let indexer = indexed(&store, &topic, &[(0, 5), (5, 10), (10, 15)]).await;
        let lease = testing::topic_lease(&store, &topic, WRITER).await;

        store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 1, &[(0, 5)]),
                &[],
                &[],
                &[],
            )
            .await
            .unwrap();
        store
            .snapshots()
            .commit(
                &lease,
                &snapshot(&lease, &topic, 2, &[(5, 10)]),
                &[],
                &[],
                &[],
            )
            .await
            .unwrap();
        store.indexed_ranges().trim(&indexer, 5).await.unwrap();
        let trimmed = file(&topic, 0, 0, 5).file().clone();
        let indexed_only = file(&topic, 0, 10, 15).file().clone();

        store.snapshots().expire(&lease, 1).await.unwrap();
        let latest = store.snapshots().expire(&lease, 2).await;

        let ids: Vec<_> = store
            .snapshots()
            .list(topic.name())
            .await
            .unwrap()
            .iter()
            .map(Snapshot::id)
            .collect();
        assert_eq!(ids, [2]);
        assert_eq!(store.files().stats(&trimmed).await.unwrap(), None);
        assert!(store.files().stats(&indexed_only).await.unwrap().is_some());
        assert!(
            matches!(latest, Err(Error::LatestSnapshot { id: 2, .. })),
            "{latest:?}"
        );
    }

    #[derive(Debug, Clone)]
    enum Step {
        Acquire {
            writer: usize,
            expiring: bool,
        },
        Index {
            length: i64,
        },
        Trim {
            file: usize,
        },
        Commit {
            writer: usize,
            id_shift: i64,
            start: usize,
            end: usize,
        },
        Expire {
            writer: usize,
        },
    }

    fn step() -> impl Strategy<Value = Step> {
        let expiring = prop_oneof![4 => Just(false), 1 => Just(true)];
        let id_shift = prop_oneof![8 => Just(0i64), 1 => Just(-1i64), 1 => Just(1i64)];

        prop_oneof![
            1 => (0..2usize, expiring)
                .prop_map(|(writer, expiring)| Step::Acquire { writer, expiring }),
            3 => (1..5i64).prop_map(|length| Step::Index { length }),
            1 => (0..6usize).prop_map(|file| Step::Trim { file }),
            4 => (0..2usize, id_shift, 0..8usize, 0..8usize).prop_map(
                |(writer, id_shift, start, end)| Step::Commit {
                    writer,
                    id_shift,
                    start,
                    end,
                }
            ),
            1 => (0..2usize).prop_map(|writer| Step::Expire { writer }),
        ]
    }

    async fn run(store: &Store, steps: &[Step]) {
        let topic = testing::topic(store, 1).await;
        let indexer = testing::started(store, &topic).await.remove(0);
        let scope = Scope::Topic(topic.name().to_owned());
        let first = store
            .leases()
            .acquire(&scope, Uuid::from_u128(0), TTL)
            .await
            .unwrap();
        let mut leases: [Option<Lease>; 2] = [first, None];
        let mut starts = vec![0];

        for step in steps {
            let before = state(store, &topic).await;
            let result = match step.clone() {
                Step::Acquire { writer, expiring } => {
                    let ttl = if expiring { Duration::ZERO } else { TTL };
                    leases[writer] = store
                        .leases()
                        .acquire(&scope, Uuid::from_u128(writer as u128), ttl)
                        .await
                        .unwrap();
                    continue;
                }
                Step::Index { length } => {
                    let end = *starts.last().unwrap();

                    store
                        .files()
                        .add(
                            &indexer,
                            &mink_conformance::built(&[file(&topic, 0, end, end + length)]),
                            &DaySketches::default(),
                        )
                        .await
                        .unwrap();
                    starts.push(end + length);
                    continue;
                }
                Step::Trim { file } => {
                    let Some(start) = starts.get(file) else {
                        continue;
                    };
                    let trimmed = store.indexed_ranges().trim(&indexer, *start).await;
                    assert!(
                        matches!(trimmed, Ok(_) | Err(Error::NotABoundary { .. })),
                        "{trimmed:?}"
                    );
                    continue;
                }

                Step::Commit {
                    writer,
                    id_shift,
                    start,
                    end,
                } => {
                    let Some(lease) = &leases[writer] else {
                        continue;
                    };
                    let latest = before.last().map(|(snapshot, _)| snapshot.id());
                    let id = Snapshot::next_id(latest) + id_shift;
                    if id < 1 {
                        continue;
                    }

                    let offset = |index: usize| starts.get(index).copied().unwrap_or(index as i64);
                    let (start, end) = (offset(start), offset(end));
                    let range = (start.min(end), start.max(end));

                    store
                        .snapshots()
                        .commit(lease, &snapshot(lease, &topic, id, &[range]), &[], &[], &[])
                        .await
                }
                Step::Expire { writer } => {
                    let Some(lease) = &leases[writer] else {
                        continue;
                    };
                    let Some((oldest, _)) = before.first() else {
                        continue;
                    };
                    store
                        .snapshots()
                        .expire(lease, oldest.id())
                        .await
                        .map(|_| ())
                }
            };

            let after = state(store, &topic).await;
            if result.is_err() {
                assert_eq!(after, before, "{step:?}: {result:?}");
            }

            assert_rules(&after);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        #[test]
        fn commits_keep_the_snapshot_rules(steps in prop::collection::vec(step(), 1..40)) {
            let runtime = tokio::runtime::Runtime::new().unwrap();

            runtime.block_on(async {
                let Some(store) = testing::store().await else {
                    return;
                };

                run(&store, &steps).await;
            });
        }
    }
}
