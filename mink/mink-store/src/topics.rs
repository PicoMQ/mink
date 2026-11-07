use std::time::Duration;

use mink_core::{Granularity, TableState, Topic, millis};
use sqlx::{FromRow, PgConnection, PgPool};
use uuid::Uuid;

use crate::Error;
use crate::specs;

pub struct Topics<'a> {
    pool: &'a PgPool,
}

#[derive(FromRow)]
struct TableRow {
    name: String,
    uuid: Uuid,
    partition_count: i32,
    state: String,
}

impl<'a> Topics<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Topics<'a> {
        Topics { pool }
    }

    pub async fn add(&self, topic: &Topic, granularity: Granularity) -> Result<(), Error> {
        let mut transaction = self.pool.begin().await?;

        let added = sqlx::query(
            "INSERT INTO topic (name, uuid, partition_count, state) VALUES ($1, $2, $3, 'indexing') \
             ON CONFLICT DO NOTHING",
        )
        .bind(topic.name())
        .bind(topic.uuid())
        .bind(topic.partition_count())
        .execute(&mut *transaction)
        .await?;

        if added.rows_affected() == 0 {
            return Err(Error::AlreadyTable {
                topic: topic.name().to_owned(),
            });
        }

        specs::insert(&mut transaction, topic.name(), 0, granularity).await?;

        transaction.commit().await?;
        Ok(())
    }

    pub async fn refresh(&self, name: &str, found: Option<&Topic>) -> Result<TableState, Error> {
        let no_table = || Error::NoTable {
            topic: name.to_owned(),
        };
        let (stored, state) = self.table(name).await?.ok_or_else(no_table)?;

        let next = match found {
            _ if state == TableState::Removing => return Ok(TableState::Removing),
            None => TableState::Missing,
            Some(found) if found.uuid() != stored.uuid() => TableState::Replaced,
            Some(found) if found.partition_count() < stored.partition_count() => {
                return Err(Error::FewerPartitions {
                    topic: name.to_owned(),
                    registered: stored.partition_count(),
                    given: found.partition_count(),
                });
            }
            Some(_) => TableState::Indexing,
        };
        let partition_count = match found {
            Some(found) if next == TableState::Indexing => found.partition_count(),
            _ => stored.partition_count(),
        };
        if next == state && partition_count == stored.partition_count() {
            return Ok(next);
        }

        let updated = sqlx::query(
            "UPDATE topic SET partition_count = $2, state = $3 \
             WHERE name = $1 AND state <> 'removing'",
        )
        .bind(name)
        .bind(partition_count)
        .bind(next.name())
        .execute(self.pool)
        .await?;

        match updated.rows_affected() {
            0 => Ok(self.table(name).await?.ok_or_else(no_table)?.1),
            _ => Ok(next),
        }
    }

    pub async fn remove(&self, name: &str) -> Result<(), Error> {
        let marked = sqlx::query(
            "UPDATE topic SET state = 'removing', removing_at_ms = coalesce(removing_at_ms, now_ms()) \
             WHERE name = $1",
        )
        .bind(name)
        .execute(self.pool)
        .await?;

        if marked.rows_affected() == 0 {
            return Err(Error::NoTable {
                topic: name.to_owned(),
            });
        }
        Ok(())
    }

    pub async fn removable(&self, after: Duration) -> Result<Vec<String>, Error> {
        let names = sqlx::query_scalar(
            "SELECT name FROM topic WHERE state = 'removing' AND removing_at_ms <= now_ms() - $1 \
             AND NOT EXISTS ( \
                 SELECT 1 FROM lease WHERE lease.topic = topic.name AND expires_at_ms >= now_ms() \
             ) \
             ORDER BY name",
        )
        .bind(millis(after))
        .fetch_all(self.pool)
        .await?;

        Ok(names)
    }

    pub async fn delete(&self, name: &str) -> Result<(), Error> {
        sqlx::query("DELETE FROM topic WHERE name = $1 AND state = 'removing'")
            .bind(name)
            .execute(self.pool)
            .await?;

        Ok(())
    }

    pub async fn get(&self, name: &str) -> Result<Option<Topic>, Error> {
        let table = self.table(name).await?;

        Ok(table
            .filter(|(_, state)| *state == TableState::Indexing)
            .map(|(topic, _)| topic))
    }

    pub async fn table(&self, name: &str) -> Result<Option<(Topic, TableState)>, Error> {
        let row: Option<TableRow> =
            sqlx::query_as("SELECT name, uuid, partition_count, state FROM topic WHERE name = $1")
                .bind(name)
                .fetch_optional(self.pool)
                .await?;

        row.map(TableRow::table).transpose()
    }

    pub async fn list(&self) -> Result<Vec<(Topic, TableState)>, Error> {
        let rows: Vec<TableRow> =
            sqlx::query_as("SELECT name, uuid, partition_count, state FROM topic ORDER BY name")
                .fetch_all(self.pool)
                .await?;

        rows.into_iter().map(TableRow::table).collect()
    }

    pub async fn page(
        &self,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<(Topic, TableState)>, Error> {
        let rows: Vec<TableRow> = sqlx::query_as(
            "SELECT name, uuid, partition_count, state FROM topic \
             WHERE ($1::TEXT IS NULL OR starts_with(name, $1)) \
             AND ($2::TEXT IS NULL OR name > $2) ORDER BY name LIMIT $3",
        )
        .bind(prefix)
        .bind(after)
        .bind(limit)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(TableRow::table).collect()
    }

    pub async fn summaries(&self, names: &[String]) -> Result<Vec<TableSummary>, Error> {
        let rows: Vec<SummaryRow> = sqlx::query_as(
            "SELECT topic.name, \
                 (SELECT granularity FROM time_spec WHERE time_spec.topic = topic.name \
                  ORDER BY id DESC LIMIT 1) AS granularity, \
                 latest.id AS snapshot_id, latest.timestamp_ms AS snapshot_at_ms, \
                 sums.records, sums.bytes, \
                 (SELECT coalesce(max(id), 0) FROM table_schema \
                  WHERE table_schema.topic = topic.name) AS schema_id \
             FROM topic LEFT JOIN LATERAL ( \
                 SELECT id, timestamp_ms FROM snapshot WHERE snapshot.topic = topic.name \
                 ORDER BY id DESC LIMIT 1 \
             ) latest ON true \
             CROSS JOIN LATERAL ( \
                 SELECT coalesce(sum((stats->>'records')::BIGINT), 0)::BIGINT AS records, \
                     coalesce(sum((stats->>'bytes')::BIGINT), 0)::BIGINT AS bytes \
                 FROM file WHERE file.topic = topic.name AND replaced_snapshot_id IS NULL \
             ) sums \
             WHERE topic.name = ANY($1) ORDER BY topic.name",
        )
        .bind(names)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(SummaryRow::summary).collect()
    }

    pub async fn totals(&self) -> Result<Totals, Error> {
        let (tables, records, bytes, collectable): (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM topic), sums.records, sums.bytes, \
                 (SELECT count(*) FROM object WHERE unreferenced_at_ms IS NOT NULL) \
             FROM ( \
                 SELECT coalesce(sum((stats->>'records')::BIGINT), 0)::BIGINT AS records, \
                     coalesce(sum((stats->>'bytes')::BIGINT), 0)::BIGINT AS bytes \
                 FROM file WHERE replaced_snapshot_id IS NULL \
             ) sums",
        )
        .fetch_one(self.pool)
        .await?;

        Ok(Totals {
            tables: tables as u64,
            records: records as u64,
            bytes: bytes as u64,
            collectable: collectable as u64,
        })
    }

    pub async fn indexing(&self) -> Result<Vec<Topic>, Error> {
        let rows: Vec<TableRow> = sqlx::query_as(
            "SELECT name, uuid, partition_count, state FROM topic \
             WHERE state = 'indexing' ORDER BY name",
        )
        .fetch_all(self.pool)
        .await?;

        rows.into_iter().map(|row| Ok(row.table()?.0)).collect()
    }
}

pub struct TableSummary {
    name: String,
    granularity: Granularity,
    snapshot: Option<(i64, i64)>,
    records: u64,
    bytes: u64,
    schema_id: i32,
}

pub struct Totals {
    tables: u64,
    records: u64,
    bytes: u64,
    collectable: u64,
}

#[derive(FromRow)]
struct SummaryRow {
    name: String,
    granularity: Option<String>,
    snapshot_id: Option<i64>,
    snapshot_at_ms: Option<i64>,
    records: i64,
    bytes: i64,
    schema_id: i32,
}

impl SummaryRow {
    fn summary(self) -> Result<TableSummary, Error> {
        let granularity = self.granularity.unwrap_or_default();
        let granularity = Granularity::from_name(&granularity)
            .ok_or_else(|| Error::Corrupt(format!("granularity {granularity}")))?;

        Ok(TableSummary {
            name: self.name,
            granularity,
            snapshot: self.snapshot_id.zip(self.snapshot_at_ms),
            records: self.records as u64,
            bytes: self.bytes as u64,
            schema_id: self.schema_id,
        })
    }
}

impl TableSummary {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn granularity(&self) -> Granularity {
        self.granularity
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot.map(|(id, _)| id)
    }

    pub fn snapshot_at_ms(&self) -> Option<i64> {
        self.snapshot.map(|(_, at_ms)| at_ms)
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }
}

impl Totals {
    pub fn tables(&self) -> u64 {
        self.tables
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn collectable(&self) -> u64 {
        self.collectable
    }
}

impl TableRow {
    fn table(self) -> Result<(Topic, TableState), Error> {
        let state = TableState::from_name(&self.state)
            .ok_or_else(|| Error::Corrupt(format!("table state {}", self.state)))?;

        Ok((
            Topic::new(self.name, self.uuid, self.partition_count)?,
            state,
        ))
    }
}

pub(crate) async fn lock_for_change(
    connection: &mut PgConnection,
    topic: &str,
) -> Result<(), Error> {
    let state: Option<String> =
        sqlx::query_scalar("SELECT state FROM topic WHERE name = $1 FOR UPDATE")
            .bind(topic)
            .fetch_optional(&mut *connection)
            .await?;

    match state.as_deref() {
        None => Err(Error::NoTable {
            topic: topic.to_owned(),
        }),
        Some("removing") => Err(Error::TableRemoving {
            topic: topic.to_owned(),
        }),
        Some(_) => Ok(()),
    }
}

pub(crate) async fn lock(connection: &mut PgConnection, topic: &str) -> Result<(), Error> {
    sqlx::query("SELECT 1 FROM topic WHERE name = $1 FOR UPDATE")
        .bind(topic)
        .execute(&mut *connection)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;

    #[tokio::test]
    async fn a_table_is_added_once() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 2).await;

        let again = store.topics().add(&topic, Granularity::Day).await;

        assert!(
            matches!(again, Err(Error::AlreadyTable { .. })),
            "{again:?}"
        );
        assert_eq!(store.topics().get(topic.name()).await.unwrap(), Some(topic));
    }

    #[tokio::test]
    async fn refreshing_follows_the_topic_in_kafka() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topics = store.topics();
        let topic = testing::topic(&store, 3).await;
        let more = Topic::new(topic.name(), topic.uuid(), 4).unwrap();
        let fewer = Topic::new(topic.name(), topic.uuid(), 2).unwrap();
        let recreated = Topic::new(topic.name(), Uuid::new_v4(), 4).unwrap();

        let grown = topics.refresh(topic.name(), Some(&more)).await.unwrap();
        let shrunk = topics.refresh(topic.name(), Some(&fewer)).await;
        let replaced = topics
            .refresh(topic.name(), Some(&recreated))
            .await
            .unwrap();
        let replaced_table = topics.table(topic.name()).await.unwrap();
        let missing = topics.refresh(topic.name(), None).await.unwrap();
        let back = topics.refresh(topic.name(), Some(&more)).await.unwrap();

        assert_eq!(grown, TableState::Indexing);
        assert!(
            matches!(
                shrunk,
                Err(Error::FewerPartitions {
                    registered: 4,
                    given: 2,
                    ..
                })
            ),
            "{shrunk:?}"
        );
        assert_eq!(replaced, TableState::Replaced);
        assert_eq!(replaced_table, Some((more.clone(), TableState::Replaced)));
        assert_eq!(missing, TableState::Missing);
        assert_eq!(back, TableState::Indexing);
        assert_eq!(topics.get(topic.name()).await.unwrap(), Some(more));
    }

    #[tokio::test]
    async fn a_removed_table_is_deleted_only_after_its_wait() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topics = store.topics();
        let topic = testing::topic(&store, 1).await;

        topics.remove(topic.name()).await.unwrap();
        let refreshed = topics.refresh(topic.name(), Some(&topic)).await.unwrap();
        let hidden = topics.get(topic.name()).await.unwrap();
        let early = topics.removable(Duration::from_secs(60)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let due = topics.removable(Duration::from_millis(5)).await.unwrap();
        topics.delete(topic.name()).await.unwrap();
        let unknown = topics.remove(topic.name()).await;

        assert_eq!(refreshed, TableState::Removing);
        assert_eq!(hidden, None);
        assert!(!early.contains(&topic.name().to_owned()));
        assert!(due.contains(&topic.name().to_owned()));
        assert_eq!(topics.table(topic.name()).await.unwrap(), None);
        assert!(matches!(unknown, Err(Error::NoTable { .. })), "{unknown:?}");
    }
}
