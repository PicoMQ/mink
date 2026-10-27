use mink_core::{DaySketches, utc_day};
use sqlx::{PgConnection, PgPool};

use crate::Error;
use crate::leases::{Lease, check};
use crate::{files, ranges};

pub struct Sketches<'a> {
    pool: &'a PgPool,
}

impl<'a> Sketches<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Sketches<'a> {
        Sketches { pool }
    }

    pub async fn list(&self, topic: &str) -> Result<DaySketches, Error> {
        let rows: Vec<(i64, i32, Vec<u8>)> = sqlx::query_as(
            "SELECT day, field_id, sketch FROM sketch WHERE topic = $1 ORDER BY day, field_id",
        )
        .bind(topic)
        .fetch_all(self.pool)
        .await?;

        let mut sketches = DaySketches::default();
        for (day, field_id, sketch) in rows {
            sketches.insert(day, field_id, sketch)?;
        }

        Ok(sketches)
    }

    pub async fn trim(&self, lease: &Lease) -> Result<(), Error> {
        let topic = lease.require_topic()?;
        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;

        ranges::lock_topic(&mut transaction, topic).await?;

        let first_timestamp_ms = files::first_timestamp_ms(&mut transaction, topic).await?;

        if let Some(first_day) = first_timestamp_ms.map(utc_day) {
            sqlx::query("DELETE FROM sketch WHERE topic = $1 AND day < $2")
                .bind(topic)
                .bind(first_day)
                .execute(&mut *transaction)
                .await?;
        }

        transaction.commit().await?;
        Ok(())
    }
}

pub(crate) async fn union(
    connection: &mut PgConnection,
    topic: &str,
    sketches: &DaySketches,
) -> Result<(), Error> {
    for sketch in sketches.iter() {
        let inserted: Option<i32> = sqlx::query_scalar(
            "INSERT INTO sketch (topic, day, field_id, sketch) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (topic, day, field_id) DO NOTHING RETURNING 1",
        )
        .bind(topic)
        .bind(sketch.day())
        .bind(sketch.field_id())
        .bind(sketch.sketch())
        .fetch_optional(&mut *connection)
        .await?;
        if inserted.is_some() {
            continue;
        }

        let stored: Vec<u8> = sqlx::query_scalar(
            "SELECT sketch FROM sketch WHERE topic = $1 AND day = $2 AND field_id = $3 FOR UPDATE",
        )
        .bind(topic)
        .bind(sketch.day())
        .bind(sketch.field_id())
        .fetch_one(&mut *connection)
        .await?;
        let unioned = mink_core::union(&[&stored, sketch.sketch()])?;

        sqlx::query(
            "UPDATE sketch SET sketch = $4 WHERE topic = $1 AND day = $2 AND field_id = $3",
        )
        .bind(topic)
        .bind(sketch.day())
        .bind(sketch.field_id())
        .bind(unioned)
        .execute(&mut *connection)
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use mink_core::Sketch;

    use super::*;
    use crate::testing;

    fn sketch(values: std::ops::Range<u32>) -> DaySketches {
        let mut sketch = Sketch::new();
        for value in values {
            sketch.update(&value.to_le_bytes());
        }

        let mut sketches = DaySketches::default();
        sketches.insert(9, 2, sketch.serialize()).unwrap();
        sketches
    }

    #[tokio::test]
    async fn concurrent_unions_lose_nothing() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let unioned = |sketches: DaySketches| {
            let (store, topic) = (&store, &topic);

            async move {
                let mut transaction = store.pool.begin().await.unwrap();
                union(&mut transaction, topic.name(), &sketches)
                    .await
                    .unwrap();
                transaction.commit().await.unwrap();
            }
        };

        tokio::join!(unioned(sketch(0..400)), unioned(sketch(300..700)));

        let stored = store.sketches().list(topic.name()).await.unwrap();
        let sketch = stored.iter().next().unwrap();
        assert_eq!(mink_core::estimate(sketch.sketch()).unwrap(), 700.0);
    }
}
