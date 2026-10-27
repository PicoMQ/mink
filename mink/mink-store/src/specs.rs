use mink_core::Granularity;
use sqlx::{PgConnection, PgPool};

use crate::Error;

pub struct TimeSpecs<'a> {
    pool: &'a PgPool,
}

impl<'a> TimeSpecs<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> TimeSpecs<'a> {
        TimeSpecs { pool }
    }

    pub async fn record(&self, topic: &str, granularity: Granularity) -> Result<i32, Error> {
        let mut transaction = self.pool.begin().await?;

        crate::topics::lock_for_change(&mut transaction, topic).await?;

        let specs = list(&mut transaction, topic).await?;
        let current = specs.len().checked_sub(1);
        if let Some(current) = current
            && specs[current] == granularity
        {
            return spec_id(current);
        }

        let id = spec_id(specs.len())?;
        insert(&mut transaction, topic, id, granularity).await?;

        transaction.commit().await?;
        Ok(id)
    }

    pub async fn list(&self, topic: &str) -> Result<mink_core::TimeSpecs, Error> {
        let mut connection = self.pool.acquire().await?;

        Ok(mink_core::TimeSpecs::new(
            list(&mut connection, topic).await?,
        )?)
    }
}

pub(crate) async fn insert(
    connection: &mut PgConnection,
    topic: &str,
    id: i32,
    granularity: Granularity,
) -> Result<(), Error> {
    sqlx::query("INSERT INTO time_spec (topic, id, granularity) VALUES ($1, $2, $3)")
        .bind(topic)
        .bind(id)
        .bind(granularity.name())
        .execute(&mut *connection)
        .await?;

    Ok(())
}

async fn list(connection: &mut PgConnection, topic: &str) -> Result<Vec<Granularity>, Error> {
    let names: Vec<String> =
        sqlx::query_scalar("SELECT granularity FROM time_spec WHERE topic = $1 ORDER BY id")
            .bind(topic)
            .fetch_all(&mut *connection)
            .await?;

    names
        .iter()
        .map(|name| {
            Granularity::from_name(name)
                .ok_or_else(|| Error::Corrupt(format!("partition granularity {name}")))
        })
        .collect()
}

fn spec_id(index: usize) -> Result<i32, Error> {
    i32::try_from(index).map_err(|_| Error::OutOfRange(format!("time spec {index}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;

    #[tokio::test]
    async fn a_spec_is_recorded_only_when_the_granularity_changes() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let specs = store.time_specs();

        let same = specs.record(topic.name(), Granularity::None).await.unwrap();
        let changed = specs.record(topic.name(), Granularity::Day).await.unwrap();
        let again = specs.record(topic.name(), Granularity::Day).await.unwrap();
        let back = specs.record(topic.name(), Granularity::None).await.unwrap();

        assert_eq!((same, changed, again, back), (0, 1, 1, 2));
        assert_eq!(
            specs.list(topic.name()).await.unwrap().granularities(),
            [Granularity::None, Granularity::Day, Granularity::None]
        );
    }
}
