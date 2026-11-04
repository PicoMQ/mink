use std::time::Duration;

use mink_core::{ExternalTable, TierSchedule, locations_holding, millis};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::Error;

macro_rules! external_table_select {
    () => {
        "SELECT topic, catalog, namespace, name, uuid, location, tier_after_ms, tier_interval_ms \
         FROM external_table "
    };
}

pub struct ExternalTables<'a> {
    pool: &'a PgPool,
}

#[derive(FromRow)]
struct ExternalTableRow {
    topic: String,
    catalog: String,
    namespace: String,
    name: String,
    uuid: Uuid,
    location: String,
    tier_after_ms: i64,
    tier_interval_ms: i64,
}

impl ExternalTableRow {
    fn table(self) -> Result<ExternalTable, Error> {
        let duration = |ms: i64| Duration::from_millis(u64::try_from(ms).unwrap_or_default());
        let schedule = TierSchedule::new(
            duration(self.tier_after_ms),
            duration(self.tier_interval_ms),
        )?;

        Ok(ExternalTable::new(
            &self.topic,
            &self.catalog,
            &self.namespace,
            &self.name,
            self.uuid,
            &self.location,
            schedule,
        )?)
    }
}

impl<'a> ExternalTables<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> ExternalTables<'a> {
        ExternalTables { pool }
    }

    pub async fn add(&self, table: &ExternalTable) -> Result<(), Error> {
        let added = sqlx::query(
            "INSERT INTO external_table \
             (topic, catalog, namespace, name, uuid, location, tier_after_ms, tier_interval_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(table.topic())
        .bind(table.catalog())
        .bind(table.namespace())
        .bind(table.name())
        .bind(table.uuid())
        .bind(table.location())
        .bind(millis(table.schedule().after()))
        .bind(millis(table.schedule().interval()))
        .execute(self.pool)
        .await;

        let topic = table.topic().to_owned();
        match added {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
                match error.constraint() {
                    Some("external_table_pkey") => Err(Error::AlreadyExternalTable { topic }),
                    _ => Err(Error::ExternalTableTaken { topic }),
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn list(&self) -> Result<Vec<ExternalTable>, Error> {
        let rows: Vec<ExternalTableRow> =
            sqlx::query_as(concat!(external_table_select!(), "ORDER BY topic"))
                .fetch_all(self.pool)
                .await?;

        rows.into_iter().map(ExternalTableRow::table).collect()
    }

    pub async fn get(&self, topic: &str) -> Result<Option<ExternalTable>, Error> {
        let row: Option<ExternalTableRow> =
            sqlx::query_as(concat!(external_table_select!(), "WHERE topic = $1"))
                .bind(topic)
                .fetch_optional(self.pool)
                .await?;

        row.map(ExternalTableRow::table).transpose()
    }

    pub async fn holding(&self, path: &str) -> Result<Option<ExternalTable>, Error> {
        let row: Option<ExternalTableRow> = sqlx::query_as(concat!(
            external_table_select!(),
            "WHERE location = ANY($1) ORDER BY length(location) DESC LIMIT 1"
        ))
        .bind(locations_holding(path))
        .fetch_optional(self.pool)
        .await?;

        row.map(ExternalTableRow::table).transpose()
    }
}

#[cfg(test)]
mod tests {
    use mink_core::{ExternalTable, TierSchedule, Topic};
    use uuid::Uuid;

    use crate::Error;
    use crate::testing;

    fn table(topic: &Topic, name: &str) -> ExternalTable {
        located(topic, name, &format!("s3://b/{name}"))
    }

    fn located(topic: &Topic, name: &str, location: &str) -> ExternalTable {
        ExternalTable::new(
            topic.name(),
            "rest",
            "kafka",
            name,
            Uuid::new_v4(),
            location,
            TierSchedule::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_topic_has_one_external_table() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;

        store
            .external_tables()
            .add(&table(&topic, "orders"))
            .await
            .unwrap();
        let second = store.external_tables().add(&table(&topic, "other")).await;
        let other = testing::topic(&store, 1).await;
        let same_location = store
            .external_tables()
            .add(&located(&other, "elsewhere", "s3://b/orders"))
            .await;

        let stored = store.external_tables().get(topic.name()).await.unwrap();
        assert_eq!(stored.unwrap().name(), "orders");
        let listed = store.external_tables().list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].topic(), topic.name());
        assert!(
            matches!(second, Err(Error::AlreadyExternalTable { .. })),
            "{second:?}"
        );
        assert!(
            matches!(same_location, Err(Error::ExternalTableTaken { .. })),
            "{same_location:?}"
        );
    }

    #[tokio::test]
    async fn a_path_is_held_by_the_external_table_it_falls_under() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let held = table(&topic, topic.name());
        store.external_tables().add(&held).await.unwrap();
        let tables = store.external_tables();

        let data = tables
            .holding(&format!("s3://b/{}/data/0.parquet", topic.name()))
            .await
            .unwrap();
        let beside = tables
            .holding(&format!("s3://b/{}x/data/0.parquet", topic.name()))
            .await
            .unwrap();

        assert_eq!(data, Some(held));
        assert_eq!(beside, None);
    }

    #[tokio::test]
    async fn two_topics_never_share_an_external_table() {
        let Some(store) = testing::store().await else {
            return;
        };
        let first = testing::topic(&store, 1).await;
        let second = testing::topic(&store, 1).await;

        store
            .external_tables()
            .add(&table(&first, "orders"))
            .await
            .unwrap();
        let shared = store.external_tables().add(&table(&second, "orders")).await;

        assert!(
            matches!(shared, Err(Error::ExternalTableTaken { .. })),
            "{shared:?}"
        );
    }
}
