use mink_core::{Offsets, Partition};
use sqlx::{PgConnection, PgPool};

use crate::Error;
use crate::files;
use crate::leases::{Lease, check};

pub struct IndexedRanges<'a> {
    pool: &'a PgPool,
}

impl<'a> IndexedRanges<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> IndexedRanges<'a> {
        IndexedRanges { pool }
    }

    pub async fn start(&self, lease: &Lease, offset: i64) -> Result<Offsets, Error> {
        let partition = lease.require_partition()?;
        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;

        sqlx::query(
            "INSERT INTO indexed_range (topic, partition, start_offset, end_offset) \
             VALUES ($1, $2, $3, $3) ON CONFLICT (topic, partition) DO NOTHING",
        )
        .bind(partition.topic())
        .bind(partition.index())
        .bind(offset)
        .execute(&mut *transaction)
        .await?;
        let range = lock(&mut transaction, partition).await?;

        transaction.commit().await?;
        Ok(range)
    }

    pub async fn get(&self, partition: &Partition) -> Result<Option<Offsets>, Error> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            "SELECT start_offset, end_offset FROM indexed_range \
             WHERE topic = $1 AND partition = $2",
        )
        .bind(partition.topic())
        .bind(partition.index())
        .fetch_optional(self.pool)
        .await?;

        row.map(|(start_offset, end_offset)| {
            Ok(Offsets::new(partition.clone(), start_offset, end_offset)?)
        })
        .transpose()
    }

    pub async fn list(&self, topic: &str) -> Result<Vec<Offsets>, Error> {
        let rows: Vec<(i32, i64, i64)> = sqlx::query_as(
            "SELECT partition, start_offset, end_offset FROM indexed_range \
             WHERE topic = $1 ORDER BY partition",
        )
        .bind(topic)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter()
            .map(|(index, start_offset, end_offset)| {
                Ok(Offsets::new(
                    Partition::new(topic, index)?,
                    start_offset,
                    end_offset,
                )?)
            })
            .collect()
    }

    pub async fn trim(&self, lease: &Lease, start_offset: i64) -> Result<Offsets, Error> {
        let partition = lease.require_partition()?;
        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;
        let range = lock(&mut transaction, partition).await?;

        check_boundary(&mut transaction, &range, start_offset).await?;
        set(
            &mut transaction,
            partition,
            start_offset,
            range.end_offset(),
        )
        .await?;

        transaction.commit().await?;
        Ok(Offsets::new(
            partition.clone(),
            start_offset,
            range.end_offset(),
        )?)
    }
}

pub(crate) async fn lock(
    connection: &mut PgConnection,
    partition: &Partition,
) -> Result<Offsets, Error> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT start_offset, end_offset FROM indexed_range \
         WHERE topic = $1 AND partition = $2 FOR UPDATE",
    )
    .bind(partition.topic())
    .bind(partition.index())
    .fetch_optional(&mut *connection)
    .await?;

    let (start_offset, end_offset) =
        row.ok_or_else(|| Error::RangeNotStarted(partition.clone()))?;
    Ok(Offsets::new(partition.clone(), start_offset, end_offset)?)
}

pub(crate) async fn set(
    connection: &mut PgConnection,
    partition: &Partition,
    start_offset: i64,
    end_offset: i64,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE indexed_range SET start_offset = $3, end_offset = $4 \
         WHERE topic = $1 AND partition = $2",
    )
    .bind(partition.topic())
    .bind(partition.index())
    .bind(start_offset)
    .bind(end_offset)
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub(crate) async fn lock_topic(connection: &mut PgConnection, topic: &str) -> Result<(), Error> {
    sqlx::query("SELECT 1 FROM indexed_range WHERE topic = $1 FOR UPDATE")
        .bind(topic)
        .execute(&mut *connection)
        .await?;

    Ok(())
}

pub(crate) async fn check_boundary(
    connection: &mut PgConnection,
    range: &Offsets,
    offset: i64,
) -> Result<(), Error> {
    let at_an_end = offset == range.start_offset() || offset == range.end_offset();
    let file_start = files::starts_at(connection, range, offset).await?;
    let inside_an_assembly =
        files::inside_live_assembly(connection, range.partition(), offset).await?;

    if (at_an_end || file_start) && !inside_an_assembly {
        return Ok(());
    }

    Err(Error::NotABoundary {
        partition: range.partition().clone(),
        offset,
    })
}

#[cfg(test)]
mod tests {
    use mink_core::DaySketches;

    use super::*;
    use crate::testing::{self, INDEXER, file};

    #[tokio::test]
    async fn a_range_starts_once_at_its_first_offset() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let lease = testing::partition_lease(&store, &topic, 0, INDEXER).await;

        let started = store.indexed_ranges().start(&lease, 40).await.unwrap();
        let again = store.indexed_ranges().start(&lease, 90).await.unwrap();

        assert_eq!((started.start_offset(), started.end_offset()), (40, 40));
        assert_eq!(again, started);
    }

    #[tokio::test]
    async fn trimming_moves_the_start_only_forward_to_a_file_boundary() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let lease = testing::partition_lease(&store, &topic, 0, INDEXER).await;
        store.indexed_ranges().start(&lease, 0).await.unwrap();
        store
            .files()
            .add(
                &lease,
                &mink_conformance::built(&[file(&topic, 0, 0, 10), file(&topic, 0, 10, 20)]),
                &DaySketches::default(),
            )
            .await
            .unwrap();

        let inside_a_file = store.indexed_ranges().trim(&lease, 5).await;
        let trimmed = store.indexed_ranges().trim(&lease, 10).await.unwrap();
        let backwards = store.indexed_ranges().trim(&lease, 0).await;

        assert!(
            matches!(inside_a_file, Err(Error::NotABoundary { offset: 5, .. })),
            "{inside_a_file:?}"
        );
        assert_eq!((trimmed.start_offset(), trimmed.end_offset()), (10, 20));
        assert!(
            matches!(backwards, Err(Error::NotABoundary { offset: 0, .. })),
            "{backwards:?}"
        );
    }
}
