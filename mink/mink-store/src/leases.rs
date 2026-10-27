use std::time::Duration;

use mink_core::{Partition, Scope, millis};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    scope: Scope,
    holder: Uuid,
    epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHolder {
    scope: Scope,
    holder: Uuid,
    epoch: i64,
    expires_at_ms: i64,
}

pub struct Leases<'a> {
    pool: &'a PgPool,
}

impl Lease {
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    pub(crate) fn require_partition(&self) -> Result<&Partition, Error> {
        match &self.scope {
            Scope::Partition(partition) => Ok(partition),
            Scope::Topic(_) => Err(Error::LeaseScope(self.scope.clone())),
        }
    }

    pub(crate) fn require_topic(&self) -> Result<&str, Error> {
        match &self.scope {
            Scope::Topic(topic) => Ok(topic),
            Scope::Partition(_) => Err(Error::LeaseScope(self.scope.clone())),
        }
    }
}

impl LeaseHolder {
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn holder(&self) -> Uuid {
        self.holder
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    pub fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }
}

impl<'a> Leases<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Leases<'a> {
        Leases { pool }
    }

    pub async fn holders(&self, topic: &str) -> Result<Vec<LeaseHolder>, Error> {
        self.page(Some(topic), None, None, i64::MAX).await
    }

    pub async fn page(
        &self,
        topic: Option<&str>,
        holder: Option<Uuid>,
        after: Option<(&str, Option<i32>)>,
        limit: i64,
    ) -> Result<Vec<LeaseHolder>, Error> {
        let rows: Vec<(String, Option<i32>, Uuid, i64, i64)> = sqlx::query_as(
            "SELECT topic, partition, holder, epoch, expires_at_ms FROM lease \
             WHERE expires_at_ms > now_ms() AND ($1::TEXT IS NULL OR topic = $1) \
             AND ($2::UUID IS NULL OR holder = $2) \
             AND ($3::TEXT IS NULL OR (topic, coalesce(partition, -1)) > ($3, coalesce($4, -1))) \
             ORDER BY topic, coalesce(partition, -1) LIMIT $5",
        )
        .bind(topic)
        .bind(holder)
        .bind(after.map(|(topic, _)| topic))
        .bind(after.and_then(|(_, partition)| partition))
        .bind(limit)
        .fetch_all(self.pool)
        .await?;

        rows.into_iter()
            .map(|(topic, partition, holder, epoch, expires_at_ms)| {
                let scope = match partition {
                    Some(index) => Scope::Partition(Partition::new(&topic, index)?),
                    None => Scope::Topic(topic),
                };

                Ok(LeaseHolder {
                    scope,
                    holder,
                    epoch,
                    expires_at_ms,
                })
            })
            .collect()
    }

    pub async fn acquire(
        &self,
        scope: &Scope,
        holder: Uuid,
        ttl: Duration,
    ) -> Result<Option<Lease>, Error> {
        let epoch: Option<i64> = sqlx::query_scalar(
            "INSERT INTO lease (topic, partition, holder, epoch, expires_at_ms) \
             SELECT $1, $2, $3, 1, now_ms() + $4 \
             WHERE EXISTS (SELECT 1 FROM topic WHERE name = $1 AND state = 'indexing') \
             ON CONFLICT (topic, partition) DO UPDATE SET \
                 holder = EXCLUDED.holder, \
                 epoch = CASE WHEN lease.holder = EXCLUDED.holder AND lease.expires_at_ms >= now_ms() \
                     THEN lease.epoch ELSE lease.epoch + 1 END, \
                 expires_at_ms = EXCLUDED.expires_at_ms \
             WHERE lease.holder = EXCLUDED.holder OR lease.expires_at_ms < now_ms() \
             RETURNING epoch",
        )
        .bind(scope.topic())
        .bind(scope.partition_index())
        .bind(holder)
        .bind(millis(ttl))
        .fetch_optional(self.pool)
        .await?;

        Ok(epoch.map(|epoch| Lease {
            scope: scope.clone(),
            holder,
            epoch,
        }))
    }

    pub async fn renew(&self, lease: &Lease, ttl: Duration) -> Result<(), Error> {
        let renewed = sqlx::query(
            "UPDATE lease SET expires_at_ms = now_ms() + $5 \
             WHERE topic = $1 AND partition IS NOT DISTINCT FROM $2 \
             AND holder = $3 AND epoch = $4 AND expires_at_ms >= now_ms() \
             AND EXISTS (SELECT 1 FROM topic WHERE name = $1 AND state = 'indexing')",
        )
        .bind(lease.scope.topic())
        .bind(lease.scope.partition_index())
        .bind(lease.holder)
        .bind(lease.epoch)
        .bind(millis(ttl))
        .execute(self.pool)
        .await?;

        match renewed.rows_affected() {
            0 => Err(Error::Fenced(lease.scope.clone())),
            _ => Ok(()),
        }
    }

    pub async fn release(&self, lease: &Lease) -> Result<(), Error> {
        let released = sqlx::query(
            "UPDATE lease SET expires_at_ms = 0 \
             WHERE topic = $1 AND partition IS NOT DISTINCT FROM $2 \
             AND holder = $3 AND epoch = $4 AND expires_at_ms >= now_ms()",
        )
        .bind(lease.scope.topic())
        .bind(lease.scope.partition_index())
        .bind(lease.holder)
        .bind(lease.epoch)
        .execute(self.pool)
        .await?;

        match released.rows_affected() {
            0 => Err(Error::Fenced(lease.scope.clone())),
            _ => Ok(()),
        }
    }
}

pub(crate) async fn check(connection: &mut PgConnection, lease: &Lease) -> Result<(), Error> {
    let held: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM lease WHERE topic = $1 AND partition IS NOT DISTINCT FROM $2 \
         AND holder = $3 AND epoch = $4 AND expires_at_ms >= now_ms() \
         AND EXISTS (SELECT 1 FROM topic WHERE name = $1 AND state = 'indexing') \
         FOR UPDATE OF lease",
    )
    .bind(lease.scope.topic())
    .bind(lease.scope.partition_index())
    .bind(lease.holder)
    .bind(lease.epoch)
    .fetch_optional(&mut *connection)
    .await?;

    match held {
        Some(_) => Ok(()),
        None => Err(Error::Fenced(lease.scope.clone())),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testing::{self, TTL};

    const FIRST: Uuid = Uuid::from_u128(1);
    const SECOND: Uuid = Uuid::from_u128(2);

    #[tokio::test]
    async fn only_one_holder_acquires_a_lease() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let partition = Scope::Partition(Partition::new(topic.name(), 0).unwrap());
        let whole = Scope::Topic(topic.name().to_owned());

        let first = store
            .leases()
            .acquire(&partition, FIRST, TTL)
            .await
            .unwrap();
        let second = store
            .leases()
            .acquire(&partition, SECOND, TTL)
            .await
            .unwrap();
        let again = store
            .leases()
            .acquire(&partition, FIRST, TTL)
            .await
            .unwrap();
        let other_scope = store.leases().acquire(&whole, SECOND, TTL).await.unwrap();

        assert_eq!(first.as_ref().map(Lease::epoch), Some(1));
        assert_eq!(second, None);
        assert_eq!(again, first);
        assert_eq!(other_scope.map(|lease| lease.epoch()), Some(1));
    }

    #[tokio::test]
    async fn an_expired_lease_moves_to_a_new_holder_with_a_higher_epoch() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let scope = Scope::Topic(topic.name().to_owned());

        store
            .leases()
            .acquire(&scope, FIRST, Duration::ZERO)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let taken = store.leases().acquire(&scope, SECOND, TTL).await.unwrap();

        assert_eq!(taken.map(|lease| lease.epoch()), Some(2));
    }

    #[tokio::test]
    async fn a_lease_re_taken_after_expiry_fences_the_old_one() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let scope = Scope::Partition(Partition::new(topic.name(), 0).unwrap());
        let stalled = store
            .leases()
            .acquire(&scope, FIRST, Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let retaken = store
            .leases()
            .acquire(&scope, FIRST, TTL)
            .await
            .unwrap()
            .unwrap();
        let mut connection = store.pool.acquire().await.unwrap();
        let stale = check(&mut connection, &stalled).await;

        assert_eq!(retaken.epoch(), stalled.epoch() + 1);
        assert!(matches!(stale, Err(Error::Fenced(_))), "{stale:?}");
    }

    #[tokio::test]
    async fn a_stale_lease_cannot_renew_or_release() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 1).await;
        let scope = Scope::Partition(Partition::new(topic.name(), 0).unwrap());
        let stale = store
            .leases()
            .acquire(&scope, FIRST, Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let current = store
            .leases()
            .acquire(&scope, SECOND, TTL)
            .await
            .unwrap()
            .unwrap();

        let renewed = store.leases().renew(&stale, TTL).await;
        let released = store.leases().release(&stale).await;

        assert!(matches!(renewed, Err(Error::Fenced(_))), "{renewed:?}");
        assert!(matches!(released, Err(Error::Fenced(_))), "{released:?}");
        assert!(store.leases().renew(&current, TTL).await.is_ok());
        assert!(store.leases().release(&current).await.is_ok());
    }

    #[tokio::test]
    async fn a_removing_table_has_no_leases() {
        let Some(store) = testing::store().await else {
            return;
        };
        let topic = testing::topic(&store, 2).await;
        let (held, free) = (
            Scope::Partition(Partition::new(topic.name(), 0).unwrap()),
            Scope::Partition(Partition::new(topic.name(), 1).unwrap()),
        );
        let lease = store
            .leases()
            .acquire(&held, FIRST, TTL)
            .await
            .unwrap()
            .unwrap();

        store.topics().remove(topic.name()).await.unwrap();

        let renewed = store.leases().renew(&lease, TTL).await;
        let acquired = store.leases().acquire(&free, FIRST, TTL).await.unwrap();
        let granularity = store
            .time_specs()
            .record(topic.name(), mink_core::Granularity::Day)
            .await;
        let blocked = store.topics().removable(Duration::ZERO).await.unwrap();
        store.leases().release(&lease).await.unwrap();
        let due = store.topics().removable(Duration::ZERO).await.unwrap();

        assert!(matches!(renewed, Err(Error::Fenced(_))), "{renewed:?}");
        assert_eq!(acquired, None);
        assert!(
            matches!(granularity, Err(Error::TableRemoving { .. })),
            "{granularity:?}"
        );
        assert!(!blocked.contains(&topic.name().to_owned()));
        assert!(due.contains(&topic.name().to_owned()));
    }
}
