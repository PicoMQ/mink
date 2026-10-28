use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

const KEPT_SECONDS: u64 = 3600;

pub async fn fresh_schema(url: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_secs();
    let schema = format!("test_{now}_{}", Uuid::new_v4().simple());
    let pool = PgPool::connect(url).await.unwrap();

    drop_old_schemas(&pool, now).await;
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{schema}")
}

async fn drop_old_schemas(pool: &PgPool, now: u64) {
    let schemas: Vec<String> =
        sqlx::query_scalar("SELECT schema_name::TEXT FROM information_schema.schemata WHERE schema_name LIKE 'test\\_%'")
            .fetch_all(pool)
            .await
            .unwrap();

    for schema in schemas {
        let created = schema
            .split('_')
            .nth(1)
            .and_then(|seconds| seconds.parse::<u64>().ok());

        if created.is_none_or(|created| created + KEPT_SECONDS < now) {
            let dropped = sqlx::query(AssertSqlSafe(format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE"
            )))
            .execute(pool)
            .await;
            if let Err(error) = dropped {
                eprintln!("schema {schema} was not dropped: {error}");
            }
        }
    }
}
