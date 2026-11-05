use std::collections::BTreeMap;

use mink_core::{File, Offsets, Partition, TieredRange};
use sqlx::{FromRow, PgConnection, PgPool};

use crate::Error;
use crate::leases::{Lease, check};
use crate::ranges::{check_boundary, lock};

macro_rules! tiered_ends {
    () => {
        "SELECT partition, max(end_offset) FROM ( \
         SELECT partition, end_offset FROM tiered_range WHERE topic = $1 \
         UNION ALL SELECT partition, end_offset FROM tiered_offsets WHERE topic = $1 \
         ) AS ends WHERE $2::INTEGER IS NULL OR partition = $2"
    };
}

macro_rules! range_columns {
    () => {
        "SELECT generation, partition, start_offset, end_offset, spec_id, encoded_data_files"
    };
}

pub struct TieredRanges<'a> {
    pool: &'a PgPool,
}

#[derive(FromRow)]
struct RangeRow {
    generation: i32,
    partition: i32,
    start_offset: i64,
    end_offset: i64,
    spec_id: i32,
    encoded_data_files: Vec<String>,
}

impl RangeRow {
    fn range(self, topic: &str) -> Result<TieredRange, Error> {
        let partition = Partition::new(topic, self.partition)?;
        let offsets = Offsets::new(partition, self.start_offset, self.end_offset)?;
        Ok(TieredRange::new(
            offsets,
            self.generation,
            self.spec_id,
            self.encoded_data_files,
        ))
    }
}

impl<'a> TieredRanges<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> TieredRanges<'a> {
        TieredRanges { pool }
    }

    pub async fn add(&self, lease: &Lease, range: &TieredRange) -> Result<(), Error> {
        let partition = lease.require_tier()?;
        let offsets = range.offsets();
        if offsets.partition() != partition {
            return Err(Error::LeaseScope(lease.scope().clone()));
        }
        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;
        let indexed = lock(&mut transaction, partition).await?;
        check_boundary(&mut transaction, &indexed, offsets.end_offset()).await?;

        let start = offsets.start_offset();
        let last = end(&mut transaction, partition).await?;
        let skips_a_gap = last.is_some_and(|end| end < start) && start == indexed.start_offset();
        if let Some(expected) = last.filter(|end| *end != start && !skips_a_gap) {
            return Err(Error::RangeNotAdjacent {
                partition: partition.clone(),
                expected,
                given: start,
            });
        }

        sqlx::query(
            "INSERT INTO tiered_range \
             (topic, generation, partition, start_offset, end_offset, spec_id, encoded_data_files) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(partition.topic())
        .bind(range.generation())
        .bind(partition.index())
        .bind(offsets.start_offset())
        .bind(offsets.end_offset())
        .bind(range.spec_id())
        .bind(range.encoded_data_files())
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        Ok(())
    }

    pub async fn list(&self, topic: &str) -> Result<Vec<TieredRange>, Error> {
        let rows: Vec<RangeRow> = sqlx::query_as(concat!(
            range_columns!(),
            " FROM tiered_range WHERE topic = $1 ORDER BY partition, start_offset"
        ))
        .bind(topic)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(|row| row.range(topic)).collect()
    }

    pub async fn end(&self, partition: &Partition) -> Result<Option<i64>, Error> {
        let mut connection = self.pool.acquire().await?;

        end(&mut connection, partition).await
    }

    pub async fn ends(&self, topic: &str) -> Result<BTreeMap<i32, i64>, Error> {
        let rows: Vec<(i32, i64)> = sqlx::query_as(concat!(tiered_ends!(), " GROUP BY partition"))
            .bind(topic)
            .bind(None::<i32>)
            .fetch_all(self.pool)
            .await?;

        Ok(rows.into_iter().collect())
    }

    pub async fn committed(&self, topic: &str) -> Result<BTreeMap<i32, i64>, Error> {
        let rows: Vec<(i32, i64)> =
            sqlx::query_as("SELECT partition, end_offset FROM tiered_offsets WHERE topic = $1")
                .bind(topic)
                .fetch_all(self.pool)
                .await?;

        Ok(rows.into_iter().collect())
    }

    pub async fn commit(
        &self,
        lease: &Lease,
        generation: i32,
        ends: &BTreeMap<i32, i64>,
    ) -> Result<(), Error> {
        let topic = lease.require_topic()?;

        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;

        for (index, end_offset) in ends {
            let partition = Partition::new(topic, *index)?;
            lock(&mut transaction, &partition).await?;

            let removed: Vec<i64> = sqlx::query_scalar(
                "DELETE FROM tiered_range WHERE topic = $1 AND partition = $2 AND end_offset <= $3 \
                 RETURNING end_offset",
            )
            .bind(topic)
            .bind(index)
            .bind(end_offset)
            .fetch_all(&mut *transaction)
            .await?;
            if !removed.contains(end_offset) {
                return Err(Error::NotATieredEnd {
                    partition,
                    offset: *end_offset,
                });
            }

            set_committed(&mut transaction, &partition, generation, *end_offset).await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    pub async fn discard(
        &self,
        lease: &Lease,
        partition: &Partition,
    ) -> Result<Vec<TieredRange>, Error> {
        let topic = lease.require_topic()?;
        if partition.topic() != topic {
            return Err(Error::LeaseScope(lease.scope().clone()));
        }

        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;
        lock(&mut transaction, partition).await?;

        let rows: Vec<RangeRow> = sqlx::query_as(
            "DELETE FROM tiered_range WHERE topic = $1 AND partition = $2 \
             RETURNING generation, partition, start_offset, end_offset, spec_id, encoded_data_files",
        )
        .bind(topic)
        .bind(partition.index())
        .fetch_all(&mut *transaction)
        .await?;

        transaction.commit().await?;
        rows.into_iter().map(|row| row.range(topic)).collect()
    }
}

async fn end(connection: &mut PgConnection, partition: &Partition) -> Result<Option<i64>, Error> {
    let row: Option<(i32, i64)> = sqlx::query_as(concat!(tiered_ends!(), " GROUP BY partition"))
        .bind(partition.topic())
        .bind(Some(partition.index()))
        .fetch_optional(&mut *connection)
        .await?;

    Ok(row.map(|(_, end)| end))
}

pub(crate) async fn set_committed(
    connection: &mut PgConnection,
    partition: &Partition,
    generation: i32,
    end_offset: i64,
) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO tiered_offsets (topic, generation, partition, end_offset) \
         VALUES ($1, $2, $3, $4) ON CONFLICT (topic, partition) \
         DO UPDATE SET generation = EXCLUDED.generation, end_offset = EXCLUDED.end_offset",
    )
    .bind(partition.topic())
    .bind(generation)
    .bind(partition.index())
    .bind(end_offset)
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub(crate) async fn end_inside(
    connection: &mut PgConnection,
    file: &File,
) -> Result<Option<i64>, Error> {
    let found = sqlx::query_scalar(
        "SELECT end_offset FROM tiered_range WHERE topic = $1 AND partition = $2 \
         AND $3 < end_offset AND end_offset < $4 \
         UNION ALL SELECT end_offset FROM tiered_offsets WHERE topic = $1 AND partition = $2 \
         AND $3 < end_offset AND end_offset < $4 LIMIT 1",
    )
    .bind(file.partition().topic())
    .bind(file.partition().index())
    .bind(file.start_offset())
    .bind(file.end_offset())
    .fetch_optional(&mut *connection)
    .await?;

    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use mink_core::{
        Assembly, BuiltFile, CatalogTable, DaySketches, ExternalTable, File, FileVersion,
        IndexedFile, Offsets, Partition, Scope, Span, Stats, TierSchedule, TieredRange, Topic,
    };
    use uuid::Uuid;

    use crate::testing::{self, TTL, WRITER};
    use crate::{Error, Lease, Store};

    fn range(partition: &Partition, start: i64, end: i64) -> TieredRange {
        let offsets = Offsets::new(partition.clone(), start, end).unwrap();

        TieredRange::new(offsets, 1, 0, vec![format!("{{\"file\":{start}}}")])
    }

    async fn indexed(store: &Store, topic: &Topic, bounds: &[i64]) -> (Lease, Vec<IndexedFile>) {
        let indexer = testing::started(store, topic).await.remove(0);
        let external = ExternalTable::new(
            topic.name(),
            CatalogTable::new("rest", "kafka", topic.name()),
            Uuid::new_v4(),
            &format!("s3://b/{}", topic.name()),
            TierSchedule::default(),
            1,
        )
        .unwrap();
        store
            .external_tables()
            .add(&external, &BTreeMap::new())
            .await
            .unwrap();
        let files: Vec<IndexedFile> = bounds
            .windows(2)
            .map(|pair| testing::file(topic, 0, pair[0], pair[1]))
            .collect();

        store
            .files()
            .add(
                &indexer,
                &mink_conformance::built(&files),
                &DaySketches::default(),
            )
            .await
            .unwrap();
        (indexer, files)
    }

    #[tokio::test]
    async fn tiered_ranges_are_contiguous_and_removed_after_their_commit() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let partition = Partition::new(topic.name(), 0).unwrap();
        indexed(&store, &topic, &[0, 5, 10, 20, 30]).await;
        let tier = store
            .leases()
            .acquire(&Scope::Tier(partition.clone()), Uuid::from_u128(9), TTL)
            .await
            .unwrap()
            .unwrap();
        let writer = testing::topic_lease(&store, &topic, WRITER).await;
        let ranges = store.tiered_ranges();

        ranges.add(&tier, &range(&partition, 5, 10)).await.unwrap();
        ranges.add(&tier, &range(&partition, 10, 20)).await.unwrap();
        let gap = ranges.add(&tier, &range(&partition, 25, 30)).await;
        let under_writer = ranges.add(&writer, &range(&partition, 20, 30)).await;

        assert_eq!(ranges.end(&partition).await.unwrap(), Some(20));
        assert_eq!(ranges.list(topic.name()).await.unwrap().len(), 2);
        assert!(
            matches!(
                gap,
                Err(Error::RangeNotAdjacent {
                    expected: 20,
                    given: 25,
                    ..
                })
            ),
            "{gap:?}"
        );
        assert!(
            matches!(under_writer, Err(Error::LeaseScope(_))),
            "{under_writer:?}"
        );

        ranges
            .commit(&writer, 1, &BTreeMap::from([(0, 10)]))
            .await
            .unwrap();
        let inside = ranges.commit(&writer, 1, &BTreeMap::from([(0, 15)])).await;
        let left = ranges.list(topic.name()).await.unwrap();
        assert_eq!(left, [range(&partition, 10, 20)]);
        assert_eq!(
            ranges.committed(topic.name()).await.unwrap(),
            BTreeMap::from([(0, 10)])
        );
        assert!(
            matches!(inside, Err(Error::NotATieredEnd { offset: 15, .. })),
            "{inside:?}"
        );
    }

    #[tokio::test]
    async fn a_range_skips_a_gap_only_from_the_start_of_the_indexed_range() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let partition = Partition::new(topic.name(), 0).unwrap();
        let (indexer, _) = indexed(&store, &topic, &[0, 10, 20, 30, 40]).await;
        let tier = store
            .leases()
            .acquire(&Scope::Tier(partition.clone()), Uuid::from_u128(9), TTL)
            .await
            .unwrap()
            .unwrap();
        let writer = testing::topic_lease(&store, &topic, WRITER).await;
        let ranges = store.tiered_ranges();
        ranges.add(&tier, &range(&partition, 0, 10)).await.unwrap();
        ranges
            .commit(&writer, 1, &BTreeMap::from([(0, 10)]))
            .await
            .unwrap();
        store.indexed_ranges().trim(&indexer, 20).await.unwrap();

        let past_the_start = ranges.add(&tier, &range(&partition, 30, 40)).await;
        ranges.add(&tier, &range(&partition, 20, 30)).await.unwrap();

        assert!(
            matches!(
                past_the_start,
                Err(Error::RangeNotAdjacent {
                    expected: 10,
                    given: 30,
                    ..
                })
            ),
            "{past_the_start:?}"
        );
        assert_eq!(
            ranges.list(topic.name()).await.unwrap(),
            [range(&partition, 20, 30)]
        );
    }

    #[tokio::test]
    async fn committed_offsets_anchor_the_next_range_once_ranges_are_gone() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let partition = Partition::new(topic.name(), 0).unwrap();
        indexed(&store, &topic, &[0, 10, 20]).await;
        let tier = store
            .leases()
            .acquire(&Scope::Tier(partition.clone()), Uuid::from_u128(9), TTL)
            .await
            .unwrap()
            .unwrap();
        let writer = testing::topic_lease(&store, &topic, WRITER).await;
        let ranges = store.tiered_ranges();

        ranges.add(&tier, &range(&partition, 0, 10)).await.unwrap();
        ranges
            .commit(&writer, 1, &BTreeMap::from([(0, 10)]))
            .await
            .unwrap();
        let gap = ranges.add(&tier, &range(&partition, 15, 20)).await;
        ranges.add(&tier, &range(&partition, 10, 20)).await.unwrap();
        let discarded = ranges.discard(&writer, &partition).await.unwrap();

        assert!(
            matches!(gap, Err(Error::RangeNotAdjacent { expected: 10, .. })),
            "{gap:?}"
        );
        assert_eq!(discarded, [range(&partition, 10, 20)]);
        assert_eq!(ranges.end(&partition).await.unwrap(), Some(10));
    }

    #[tokio::test]
    async fn no_assembly_spans_the_end_of_a_tiered_range() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let partition = Partition::new(topic.name(), 0).unwrap();
        let (_, files) = indexed(&store, &topic, &[0, 10, 20, 30]).await;
        let tier = store
            .leases()
            .acquire(&Scope::Tier(partition.clone()), Uuid::from_u128(9), TTL)
            .await
            .unwrap()
            .unwrap();
        let writer = testing::topic_lease(&store, &topic, WRITER).await;
        let first = testing::snapshot(&writer, &topic, 1, &[(0, 30)]);
        store
            .snapshots()
            .commit(&writer, &first, &[], &[], &[])
            .await
            .unwrap();
        let ranges = mink_conformance::ranges(&[(0, 10), (10, 20)]);
        let assembled = File::assembled(
            partition.clone(),
            files[0].file().time(),
            0,
            FileVersion::V1,
            &ranges,
        )
        .unwrap();
        let spans = ranges.iter().map(|range| Span::new(*range, 0)).collect();
        let built = BuiltFile::new(
            IndexedFile::new(assembled, Stats::new(20, 2000, 0)),
            Bytes::from_static(b"footer"),
            spans,
        )
        .unwrap();
        let replaced = vec![files[0].file().clone(), files[1].file().clone()];
        let assembly = Assembly::new(built, replaced).unwrap();
        let second = testing::snapshot(&writer, &topic, 2, &[(0, 30)]);

        let inside_a_file = store
            .tiered_ranges()
            .add(&tier, &range(&partition, 0, 15))
            .await;
        store
            .tiered_ranges()
            .add(&tier, &range(&partition, 0, 10))
            .await
            .unwrap();
        let spanning = store
            .snapshots()
            .commit(&writer, &second, &[], &[], &[assembly])
            .await;

        assert!(
            matches!(inside_a_file, Err(Error::NotABoundary { offset: 15, .. })),
            "{inside_a_file:?}"
        );
        assert!(
            matches!(spanning, Err(Error::NotABoundary { offset: 10, .. })),
            "{spanning:?}"
        );
    }
}
