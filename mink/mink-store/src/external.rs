use std::collections::BTreeMap;
use std::time::Duration;

use mink_core::{CatalogTable, ExternalLocation, ExternalTable, TierSchedule, millis};
use sqlx::{FromRow, PgConnection, PgPool};
use uuid::Uuid;

use crate::leases::{Lease, check};
use crate::{Error, tiered};

macro_rules! external_table_select {
    () => {
        "SELECT topic, catalog, namespace, name, uuid, location, tier_after_ms, tier_interval_ms, \
         generation FROM external_table "
    };
}

pub struct ExternalTables<'a> {
    pool: &'a PgPool,
}

pub struct ListedLocation {
    location: String,
    uuid: Uuid,
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
    generation: i32,
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
            CatalogTable::new(&self.catalog, &self.namespace, &self.name),
            self.uuid,
            ExternalLocation::parse(&self.location)?,
            schedule,
            self.generation,
        )?)
    }
}

impl<'a> ExternalTables<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> ExternalTables<'a> {
        ExternalTables { pool }
    }

    pub async fn attach(
        &self,
        topic: &str,
        table: CatalogTable,
        uuid: Uuid,
        location: ExternalLocation,
        schedule: TierSchedule,
        end_offsets: &BTreeMap<i32, i64>,
    ) -> Result<ExternalTable, Error> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("LOCK TABLE listed_location IN SHARE ROW EXCLUSIVE MODE")
            .execute(&mut *transaction)
            .await?;
        if overlaps_another_table(&mut transaction, &location, Some(uuid)).await? {
            return Err(Error::OverlappingLocation {
                location: location.path().to_owned(),
            });
        }
        let generation: Option<i32> = sqlx::query_scalar(
            "UPDATE topic SET external_generation = external_generation + 1 WHERE name = $1 \
             RETURNING external_generation",
        )
        .bind(topic)
        .fetch_optional(&mut *transaction)
        .await?;
        let generation = generation.ok_or_else(|| Error::NoTable {
            topic: topic.to_owned(),
        })?;
        let external = ExternalTable::new(topic, table, uuid, location, schedule, generation)?;

        let catalog_table = external.table();
        let inserted = sqlx::query(
            "INSERT INTO external_table \
             (topic, catalog, namespace, name, uuid, location, tier_after_ms, tier_interval_ms, \
             generation) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(topic)
        .bind(catalog_table.catalog())
        .bind(catalog_table.namespace())
        .bind(catalog_table.name())
        .bind(external.uuid())
        .bind(external.location().path())
        .bind(millis(external.schedule().after()))
        .bind(millis(external.schedule().interval()))
        .bind(generation)
        .execute(&mut *transaction)
        .await;
        match inserted {
            Ok(_) => {}
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
                let topic = topic.to_owned();
                return match error.constraint() {
                    Some("external_table_pkey") => Err(Error::AlreadyExternalTable { topic }),
                    _ => Err(Error::ExternalTableTaken { topic }),
                };
            }
            Err(error) => return Err(error.into()),
        }

        sqlx::query(
            "INSERT INTO listed_location (topic, generation, uuid, location) VALUES ($1, $2, $3, $4)",
        )
        .bind(topic)
        .bind(generation)
        .bind(external.uuid())
        .bind(external.location().path())
        .execute(&mut *transaction)
        .await?;
        tiered::start_at(&mut transaction, &external, end_offsets).await?;

        transaction.commit().await?;
        Ok(external)
    }

    pub async fn check_location(&self, location: &ExternalLocation) -> Result<(), Error> {
        let mut connection = self.pool.acquire().await?;
        match overlaps_another_table(&mut connection, location, None).await? {
            true => Err(Error::OverlappingLocation {
                location: location.path().to_owned(),
            }),
            false => Ok(()),
        }
    }

    pub async fn detach(&self, topic: &str) -> Result<(), Error> {
        let removed = sqlx::query("DELETE FROM external_table WHERE topic = $1")
            .bind(topic)
            .execute(self.pool)
            .await?;

        match removed.rows_affected() {
            0 => Err(Error::NoExternalTable {
                topic: topic.to_owned(),
            }),
            _ => Ok(()),
        }
    }

    pub async fn list(&self) -> Result<Vec<ExternalTable>, Error> {
        let rows: Vec<ExternalTableRow> =
            sqlx::query_as(concat!(external_table_select!(), "ORDER BY topic"))
                .fetch_all(self.pool)
                .await?;

        rows.into_iter().map(ExternalTableRow::table).collect()
    }

    pub async fn require(&self, topic: &str) -> Result<ExternalTable, Error> {
        self.get(topic)
            .await?
            .ok_or_else(|| Error::NoExternalTable {
                topic: topic.to_owned(),
            })
    }

    pub async fn get(&self, topic: &str) -> Result<Option<ExternalTable>, Error> {
        let row: Option<ExternalTableRow> =
            sqlx::query_as(concat!(external_table_select!(), "WHERE topic = $1"))
                .bind(topic)
                .fetch_optional(self.pool)
                .await?;

        row.map(ExternalTableRow::table).transpose()
    }

    pub async fn listed(&self, path: &str) -> Result<Option<ListedLocation>, Error> {
        let row: Option<(String, Uuid)> = sqlx::query_as(
            "SELECT location, uuid FROM listed_location WHERE location = ANY($1) \
             ORDER BY length(location) DESC LIMIT 1",
        )
        .bind(ExternalLocation::holding(path))
        .fetch_optional(self.pool)
        .await?;

        Ok(row.map(|(location, uuid)| ListedLocation { location, uuid }))
    }

    pub async fn forget_unlisted(&self, lease: &Lease, generations: &[i32]) -> Result<(), Error> {
        let topic = lease.require_writer()?;

        let mut transaction = self.pool.begin().await?;
        check(&mut transaction, lease).await?;
        sqlx::query(
            "DELETE FROM listed_location l WHERE l.topic = $1 AND NOT l.generation = ANY($2) \
             AND NOT EXISTS (SELECT 1 FROM external_table e \
                 WHERE e.topic = l.topic AND e.generation = l.generation)",
        )
        .bind(topic)
        .bind(generations)
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        Ok(())
    }
}

impl ListedLocation {
    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

async fn overlaps_another_table(
    connection: &mut PgConnection,
    location: &ExternalLocation,
    uuid: Option<Uuid>,
) -> Result<bool, Error> {
    let location = location.path();
    let overlapping: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM listed_location WHERE uuid IS DISTINCT FROM $3 \
         AND (location = $1 OR location = ANY($2) OR starts_with(location, $1 || '/')) LIMIT 1",
    )
    .bind(location)
    .bind(ExternalLocation::holding(location))
    .bind(uuid)
    .fetch_optional(&mut *connection)
    .await?;

    Ok(overlapping.is_some())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mink_core::{CatalogTable, ExternalLocation, ExternalTable, TierSchedule, Topic};
    use uuid::Uuid;

    use super::ExternalTables;
    use crate::Error;
    use crate::testing::{self, WRITER};

    async fn attach(
        tables: &ExternalTables<'_>,
        topic: &Topic,
        name: &str,
        location: &str,
        uuid: Uuid,
    ) -> Result<ExternalTable, Error> {
        tables
            .attach(
                topic.name(),
                CatalogTable::new("rest", "kafka", name),
                uuid,
                ExternalLocation::parse(location).unwrap(),
                TierSchedule::default(),
                &BTreeMap::new(),
            )
            .await
    }

    #[tokio::test]
    async fn a_topic_has_one_external_table() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let other = testing::topic(&store, 1).await;
        let tables = store.external_tables();

        let first = attach(&tables, &topic, "orders", "s3://b/t/orders", Uuid::new_v4())
            .await
            .unwrap();
        let second = attach(&tables, &topic, "other", "s3://b/t/other", Uuid::new_v4()).await;
        let same_location = attach(
            &tables,
            &other,
            "elsewhere",
            "s3://b/t/orders",
            Uuid::new_v4(),
        )
        .await;

        let stored = tables.get(topic.name()).await.unwrap();
        assert_eq!(stored, Some(first.clone()));
        assert_eq!(first.generation(), 1);
        let listed = tables.list().await.unwrap();
        assert_eq!(listed, [first]);
        assert!(
            matches!(second, Err(Error::AlreadyExternalTable { .. })),
            "{second:?}"
        );
        assert!(
            matches!(same_location, Err(Error::OverlappingLocation { .. })),
            "{same_location:?}"
        );
    }

    #[tokio::test]
    async fn a_path_is_listed_until_no_snapshot_lists_its_location() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let writer = testing::topic_lease(&store, &topic, WRITER).await;
        let tables = store.external_tables();
        let location = format!("s3://b/t/{}", topic.name());
        attach(&tables, &topic, topic.name(), &location, Uuid::new_v4())
            .await
            .unwrap();
        let path = format!("{location}/data/0.parquet");
        let listed = async || {
            tables
                .listed(&path)
                .await
                .unwrap()
                .map(|listed| listed.uuid())
        };

        let attached = listed().await;
        let beside = tables
            .listed(&format!("{location}x/data/0.parquet"))
            .await
            .unwrap();
        tables.detach(topic.name()).await.unwrap();
        let detached = listed().await;
        tables.forget_unlisted(&writer, &[1]).await.unwrap();
        let retained = listed().await;
        tables.forget_unlisted(&writer, &[]).await.unwrap();
        let forgotten = listed().await;

        assert!(attached.is_some());
        assert!(beside.is_none());
        assert_eq!(detached, attached);
        assert_eq!(retained, attached);
        assert_eq!(forgotten, None);
    }

    #[tokio::test]
    async fn two_topics_never_share_an_external_table() {
        let Some(store) = testing::store().await else {
            return;
        };
        let first = testing::topic(&store, 1).await;
        let second = testing::topic(&store, 1).await;
        let tables = store.external_tables();

        attach(&tables, &first, "orders", "s3://b/t/orders", Uuid::new_v4())
            .await
            .unwrap();
        let shared = attach(
            &tables,
            &second,
            "orders",
            "s3://b/t/elsewhere",
            Uuid::new_v4(),
        )
        .await;

        assert!(
            matches!(shared, Err(Error::ExternalTableTaken { .. })),
            "{shared:?}"
        );
    }

    #[tokio::test]
    async fn a_table_attaches_with_offsets_at_boundaries_and_detaches_with_its_rows() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        testing::started(&store, &topic).await;
        let tables = store.external_tables();
        let uuid = Uuid::new_v4();
        let attach = async |end_offset| {
            tables
                .attach(
                    topic.name(),
                    CatalogTable::new("rest", "kafka", topic.name()),
                    uuid,
                    ExternalLocation::parse("s3://b/t/orders").unwrap(),
                    TierSchedule::default(),
                    &BTreeMap::from([(0, end_offset)]),
                )
                .await
        };

        let beyond = attach(5).await;
        let attached = attach(0).await.unwrap();
        let settled = store.tiered_ranges().settled(topic.name()).await.unwrap();
        tables.detach(topic.name()).await.unwrap();
        let again = tables.detach(topic.name()).await;
        let reattached = attach(0).await.unwrap();

        assert!(
            matches!(beyond, Err(Error::EndOffsetBeyondIndex { offset: 5, .. })),
            "{beyond:?}"
        );
        assert_eq!(attached.generation(), 1);
        assert_eq!(settled, BTreeMap::from([(0, 0)]));
        assert!(
            matches!(again, Err(Error::NoExternalTable { .. })),
            "{again:?}"
        );
        assert_eq!(reattached.generation(), 2);
    }

    #[tokio::test]
    async fn another_table_cannot_overlap_a_listed_location_but_the_same_table_returns() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let other = testing::topic(&store, 1).await;
        let name = topic.name().to_owned();
        let tables = store.external_tables();
        let location = format!("s3://b/t/{name}");
        let first = attach(&tables, &topic, &name, &location, Uuid::new_v4())
            .await
            .unwrap();
        tables.detach(&name).await.unwrap();
        let elsewhere = async |location: &str| {
            attach(&tables, &other, "elsewhere", location, Uuid::new_v4()).await
        };

        let inside = elsewhere(&format!("{location}/inner")).await;
        let around = elsewhere("s3://b/t").await;
        let returned = attach(&tables, &topic, &name, &location, first.uuid()).await;
        let beside = elsewhere(&format!("{location}x")).await;

        assert!(
            matches!(inside, Err(Error::OverlappingLocation { .. })),
            "{inside:?}"
        );
        assert!(
            matches!(around, Err(Error::OverlappingLocation { .. })),
            "{around:?}"
        );
        assert_eq!(returned.unwrap().generation(), 2);
        assert!(beside.is_ok(), "{beside:?}");
    }
}
