//! The store contract against a live Postgres when one is configured.

use std::env;

use mink_sql::PgStore;
use mink_sql::store::contract_suite;
use sqlx::PgPool;

#[tokio::test]
async fn postgres_contract() {
    let Ok(url) = env::var("MINK_PG_URL") else {
        eprintln!("MINK_PG_URL not set; skipping postgres contract test");
        return;
    };

    let admin = PgPool::connect(&url).await.expect("connect for cleanup");
    sqlx::query("DROP TABLE IF EXISTS meta_log, meta_snapshot, meta_lease")
        .execute(&admin)
        .await
        .expect("drop tables");
    admin.close().await;

    let store = PgStore::connect(&url).await.expect("connect + migrate");
    contract_suite(&store).await;
}
