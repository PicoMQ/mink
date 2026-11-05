use std::collections::BTreeMap;
use std::time::Duration;

use mink_core::{CatalogTable, ExternalTable, Partition, TierSchedule, locations_holding, millis};
use sqlx::{FromRow, PgConnection, PgPool};
use uuid::Uuid;

use crate::Error;
use crate::leases::{Lease, check};
use crate::ranges::{check_boundary, lock};
use crate::tiered::set_committed;

macro_rules! external_table_select {
    () => {
        "SELECT topic, catalog, namespace, name, uuid, location, tier_after_ms, tier_interval_ms, \
         generation FROM external_table "
    };
}

pub struct ExternalTables<'a> {
    pool: &'a PgPool,
}

pub struct ServedLocation {
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
            &self.location,
            schedule,
            self.generation,
        )?)
    }
}

impl<'a> ExternalTables<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> ExternalTables<'a> {
        ExternalTables { pool }
    }

    pub async fn add(
        &self,
        table: &ExternalTable,
        end_offsets: &BTreeMap<i32, i64>,
    ) -> Result<(), Error> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("LOCK TABLE external_table IN SHARE ROW EXCLUSIVE MODE")
            .execute(&mut *transaction)
            .await?;
        if overlaps_another_table(&mut transaction, table).await? {
            return Err(Error::OverlappingLocation {
                location: table.location().to_owned(),
            });
        }

        let catalog_table = table.table();
        let added = sqlx::query(
            "INSERT INTO external_table \
             (topic, catalog, namespace, name, uuid, location, tier_after_ms, tier_interval_ms, \
             generation) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(table.topic())
        .bind(catalog_table.catalog())
        .bind(catalog_table.namespace())
        .bind(catalog_table.name())
        .bind(table.uuid())
        .bind(table.location())
        .bind(millis(table.schedule().after()))
        .bind(millis(table.schedule().interval()))
        .bind(table.generation())
        .execute(&mut *transaction)
        .await;

        let topic = table.topic().to_owned();
        match added {
            Ok(_) => {}
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
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
        .bind(&topic)
        .bind(table.generation())
        .bind(table.uuid())
        .bind(table.location())
        .execute(&mut *transaction)
        .await?;

        for (index, end_offset) in end_offsets {
            let partition = Partition::new(&topic, *index)?;
            let indexed = lock(&mut transaction, &partition).await?;
            if *end_offset > indexed.end_offset() {
                return Err(Error::EndOffsetBeyondIndex {
                    partition,
                    offset: *end_offset,
                    indexed: indexed.end_offset(),
                });
            }
            if *end_offset > indexed.start_offset() {
                check_boundary(&mut transaction, &indexed, *end_offset).await?;
            }

            set_committed(
                &mut transaction,
                &partition,
                table.generation(),
                *end_offset,
            )
            .await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    pub async fn remove(&self, topic: &str) -> Result<(), Error> {
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

    pub async fn serving(&self, path: &str) -> Result<Option<ServedLocation>, Error> {
        let row: Option<(String, Uuid)> = sqlx::query_as(
            "SELECT location, uuid FROM listed_location WHERE location = ANY($1) \
             ORDER BY length(location) DESC LIMIT 1",
        )
        .bind(locations_holding(path))
        .fetch_optional(self.pool)
        .await?;

        Ok(row.map(|(location, uuid)| ServedLocation { location, uuid }))
    }

    pub async fn forget_unlisted(&self, lease: &Lease, generations: &[i32]) -> Result<(), Error> {
        let topic = lease.require_topic()?;

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

impl ServedLocation {
    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

async fn overlaps_another_table(
    connection: &mut PgConnection,
    table: &ExternalTable,
) -> Result<bool, Error> {
    let location = table.location();
    let overlapping: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM listed_location WHERE uuid <> $3 \
         AND (location = $1 OR location = ANY($2) OR starts_with(location, $1 || '/')) LIMIT 1",
    )
    .bind(location)
    .bind(locations_holding(location))
    .bind(table.uuid())
    .fetch_optional(&mut *connection)
    .await?;

    Ok(overlapping.is_some())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mink_core::{CatalogTable, ExternalTable, TierSchedule, Topic};
    use uuid::Uuid;

    use crate::Error;
    use crate::testing::{self, WRITER};

    fn table(topic: &Topic, name: &str) -> ExternalTable {
        located(topic, name, &format!("s3://b/{name}"))
    }

    fn located(topic: &Topic, name: &str, location: &str) -> ExternalTable {
        ExternalTable::new(
            topic.name(),
            CatalogTable::new("rest", "kafka", name),
            Uuid::new_v4(),
            location,
            TierSchedule::default(),
            1,
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
            .add(&table(&topic, "orders"), &BTreeMap::new())
            .await
            .unwrap();
        let second = store
            .external_tables()
            .add(&table(&topic, "other"), &BTreeMap::new())
            .await;
        let other = testing::topic(&store, 1).await;
        let same_location = store
            .external_tables()
            .add(
                &located(&other, "elsewhere", "s3://b/orders"),
                &BTreeMap::new(),
            )
            .await;

        let stored = store.external_tables().get(topic.name()).await.unwrap();
        assert_eq!(stored.unwrap().table().name(), "orders");
        let listed = store.external_tables().list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].topic(), topic.name());
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
    async fn a_path_is_served_until_no_snapshot_lists_its_location() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let writer = testing::topic_lease(&store, &topic, WRITER).await;
        let tables = store.external_tables();
        tables
            .add(&table(&topic, topic.name()), &BTreeMap::new())
            .await
            .unwrap();
        let path = format!("s3://b/{}/data/0.parquet", topic.name());
        let served = async || {
            tables
                .serving(&path)
                .await
                .unwrap()
                .map(|served| served.uuid())
        };

        let attached = served().await;
        let beside = tables
            .serving(&format!("s3://b/{}x/data/0.parquet", topic.name()))
            .await
            .unwrap();
        tables.remove(topic.name()).await.unwrap();
        let detached = served().await;
        tables.forget_unlisted(&writer, &[1]).await.unwrap();
        let listed = served().await;
        tables.forget_unlisted(&writer, &[]).await.unwrap();
        let unlisted = served().await;

        assert!(attached.is_some());
        assert!(beside.is_none());
        assert_eq!(detached, attached);
        assert_eq!(listed, attached);
        assert_eq!(unlisted, None);
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
            .add(&table(&first, "orders"), &BTreeMap::new())
            .await
            .unwrap();
        let shared = store
            .external_tables()
            .add(
                &located(&second, "orders", "s3://b/elsewhere"),
                &BTreeMap::new(),
            )
            .await;

        assert!(
            matches!(shared, Err(Error::ExternalTableTaken { .. })),
            "{shared:?}"
        );
    }

    #[tokio::test]
    async fn a_table_registers_with_offsets_at_boundaries_and_detaches_with_its_rows() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        testing::started(&store, &topic).await;
        let tables = store.external_tables();
        let beyond = tables
            .add(&table(&topic, topic.name()), &BTreeMap::from([(0, 5)]))
            .await;
        tables
            .add(&table(&topic, topic.name()), &BTreeMap::from([(0, 0)]))
            .await
            .unwrap();
        let committed = store.tiered_ranges().committed(topic.name()).await.unwrap();
        let generations = [
            store
                .topics()
                .next_external_generation(topic.name())
                .await
                .unwrap(),
            store
                .topics()
                .next_external_generation(topic.name())
                .await
                .unwrap(),
        ];

        tables.remove(topic.name()).await.unwrap();
        let again = tables.remove(topic.name()).await;

        assert!(
            matches!(beyond, Err(Error::EndOffsetBeyondIndex { offset: 5, .. })),
            "{beyond:?}"
        );
        assert_eq!(committed, BTreeMap::from([(0, 0)]));
        assert_eq!(generations, [1, 2]);
        assert!(tables.get(topic.name()).await.unwrap().is_none());
        assert!(
            store
                .tiered_ranges()
                .committed(topic.name())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            matches!(again, Err(Error::NoExternalTable { .. })),
            "{again:?}"
        );
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
        let first = table(&topic, &name);
        tables.add(&first, &BTreeMap::new()).await.unwrap();
        tables.remove(&name).await.unwrap();
        let add = async |location: &str| {
            tables
                .add(&located(&other, "elsewhere", location), &BTreeMap::new())
                .await
        };

        let inside = add(&format!("s3://b/{name}/inner")).await;
        let around = add("s3://b").await;
        let returned = ExternalTable::new(
            topic.name(),
            CatalogTable::new("rest", "kafka", &name),
            first.uuid(),
            first.location(),
            TierSchedule::default(),
            2,
        )
        .unwrap();
        tables.add(&returned, &BTreeMap::new()).await.unwrap();
        let beside = add(&format!("s3://b/{name}x")).await;

        assert!(
            matches!(inside, Err(Error::OverlappingLocation { .. })),
            "{inside:?}"
        );
        assert!(
            matches!(around, Err(Error::OverlappingLocation { .. })),
            "{around:?}"
        );
        assert!(beside.is_ok(), "{beside:?}");
    }
}
