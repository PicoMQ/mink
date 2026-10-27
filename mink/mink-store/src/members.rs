use std::time::Duration;

use mink_core::{Member, Role, millis};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::Error;

pub struct Members<'a> {
    pool: &'a PgPool,
}

#[derive(FromRow)]
struct ListedRow {
    #[sqlx(flatten)]
    member: MemberRow,
    expires_at_ms: i64,
}

#[derive(FromRow)]
struct MemberRow {
    id: Uuid,
    roles: Vec<String>,
    address: Option<String>,
    admin_address: Option<String>,
}

impl<'a> Members<'a> {
    pub(crate) fn new(pool: &'a PgPool) -> Members<'a> {
        Members { pool }
    }

    pub async fn register(&self, member: &Member, ttl: Duration) -> Result<(), Error> {
        let mut transaction = self.pool.begin().await?;

        sqlx::query("DELETE FROM member WHERE expires_at_ms <= now_ms()")
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO member (id, roles, address, admin_address, expires_at_ms) \
             VALUES ($1, $2, $3, $4, now_ms() + $5) \
             ON CONFLICT (id) DO UPDATE SET roles = EXCLUDED.roles, address = EXCLUDED.address, \
             admin_address = EXCLUDED.admin_address, expires_at_ms = EXCLUDED.expires_at_ms",
        )
        .bind(member.id())
        .bind(member.roles().iter().map(Role::name).collect::<Vec<_>>())
        .bind(member.address())
        .bind(member.admin_address())
        .bind(millis(ttl))
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;
        Ok(())
    }

    pub async fn remove(&self, id: Uuid) -> Result<(), Error> {
        sqlx::query("DELETE FROM member WHERE id = $1")
            .bind(id)
            .execute(self.pool)
            .await?;

        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<(Member, i64)>, Error> {
        let rows: Vec<ListedRow> = sqlx::query_as(
            "SELECT id, roles, address, admin_address, expires_at_ms FROM member \
             WHERE expires_at_ms > now_ms() ORDER BY id",
        )
        .fetch_all(self.pool)
        .await?;

        rows.into_iter()
            .map(|row| Ok((member(row.member)?, row.expires_at_ms)))
            .collect()
    }

    pub async fn live(&self) -> Result<Vec<Member>, Error> {
        let listed = self.list().await?;

        Ok(listed.into_iter().map(|(member, _)| member).collect())
    }
}

fn member(row: MemberRow) -> Result<Member, Error> {
    let roles = row
        .roles
        .iter()
        .map(|name| Role::from_name(name).ok_or_else(|| Error::Corrupt(format!("role {name}"))))
        .collect::<Result<_, _>>()?;

    Ok(Member::new(row.id, roles, row.address, row.admin_address))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;

    fn new_member(roles: &[Role]) -> Member {
        Member::new(
            Uuid::new_v4(),
            roles.to_vec(),
            Some("http://127.0.0.1:9100".to_owned()),
            Some("http://127.0.0.1:9180".to_owned()),
        )
    }

    async fn live_ids(members: &Members<'_>, role: Role) -> Vec<Uuid> {
        let live = members.live().await.unwrap();

        live.iter()
            .filter(|member| member.has(role))
            .map(Member::id)
            .collect()
    }

    async fn rows(members: &Members<'_>, id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM member WHERE id = $1")
            .bind(id)
            .fetch_one(members.pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn members_are_live_until_their_row_expires_or_is_removed() {
        let Some(store) = testing::store().await else {
            return;
        };
        let members = store.members();
        let (indexer, server, short) = (
            new_member(&[Role::Indexer]),
            new_member(&[Role::S3, Role::Indexer]),
            new_member(&[Role::S3]),
        );
        let minute = Duration::from_secs(60);

        members.register(&indexer, minute).await.unwrap();
        members.register(&server, minute).await.unwrap();
        members
            .register(&short, Duration::from_millis(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let s3_before = live_ids(&members, Role::S3).await;

        members.register(&indexer, minute).await.unwrap();
        let expired_rows = rows(&members, short.id()).await;

        members.remove(server.id()).await.unwrap();
        let indexers_after = live_ids(&members, Role::Indexer).await;

        members.register(&short, minute).await.unwrap();
        let s3_again = live_ids(&members, Role::S3).await;

        assert!(s3_before.contains(&server.id()) && !s3_before.contains(&short.id()));
        assert_eq!(expired_rows, 0);
        assert!(indexers_after.contains(&indexer.id()) && !indexers_after.contains(&server.id()));
        assert!(s3_again.contains(&short.id()));

        members.remove(indexer.id()).await.unwrap();
        members.remove(short.id()).await.unwrap();
    }
}
