use mink_store::Store;
use sqlx::PgPool;
use uuid::Uuid;

async fn postgres() -> Option<String> {
    let url = mink_conformance::service("TEST_POSTGRES_URL")?;

    Some(mink_conformance::fresh_schema(&url).await)
}

async fn connected(url: &str) -> PgPool {
    Store::connect(url).await.unwrap().close().await;

    PgPool::connect(url).await.unwrap()
}

async fn topic(pool: &PgPool) -> String {
    let name = format!("t{}", Uuid::new_v4().simple());

    sqlx::query(
        "INSERT INTO topic (name, uuid, partition_count, state) VALUES ($1, $2, 1, 'indexing')",
    )
    .bind(&name)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO time_spec (topic, id, granularity) VALUES ($1, 0, 'hour')")
        .bind(&name)
        .execute(pool)
        .await
        .unwrap();

    name
}

async fn insert_file(
    pool: &PgPool,
    topic: &str,
    start: i64,
    end: i64,
    bucket: Option<i32>,
    schema_id: i32,
) -> bool {
    sqlx::query(
        "INSERT INTO file \
         (topic, partition, start_offset, end_offset, spec_id, bucket, schema_id, version, stats, \
         footer) VALUES ($1, 0, $2, $3, 0, $4, $5, 1, '{}', '\\x00')",
    )
    .bind(topic)
    .bind(start)
    .bind(end)
    .bind(bucket)
    .bind(schema_id)
    .execute(pool)
    .await
    .is_ok()
}

#[tokio::test]
async fn connect_creates_every_table() {
    let Some(url) = postgres().await else { return };
    let pool = connected(&url).await;

    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::TEXT FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name <> '_sqlx_migrations' \
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        tables,
        [
            "field",
            "file",
            "indexed_range",
            "lease",
            "member",
            "object",
            "registry_schema",
            "sketch",
            "snapshot",
            "snapshot_offsets",
            "table_schema",
            "time_spec",
            "topic",
            "topic_schema"
        ]
    );
}

#[tokio::test]
async fn connect_twice_applies_migrations_once() {
    let Some(url) = postgres().await else { return };

    Store::connect(&url).await.unwrap().close().await;
    Store::connect(&url).await.unwrap().close().await;

    let pool = PgPool::connect(&url).await.unwrap();
    let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(applied as usize, sqlx::migrate!().iter().count());
}

#[tokio::test]
async fn a_file_range_must_be_non_empty_and_non_negative() {
    let Some(url) = postgres().await else { return };
    let pool = connected(&url).await;
    let topic = topic(&pool).await;

    assert!(!insert_file(&pool, &topic, 5, 5, Some(5), 0).await);
    assert!(!insert_file(&pool, &topic, 10, 5, Some(5), 0).await);
    assert!(!insert_file(&pool, &topic, -1, 5, Some(5), 0).await);
    assert!(!insert_file(&pool, &topic, 0, 5, Some(5), -1).await);
}

#[tokio::test]
async fn the_file_key_is_the_full_identity() {
    let Some(url) = postgres().await else { return };
    let pool = connected(&url).await;
    let topic = topic(&pool).await;

    assert!(insert_file(&pool, &topic, 0, 10, Some(5), 0).await);
    assert!(insert_file(&pool, &topic, 0, 20, Some(5), 0).await);
    assert!(insert_file(&pool, &topic, 0, 10, Some(5), 1).await);
    assert!(insert_file(&pool, &topic, 0, 10, Some(6), 0).await);
    assert!(insert_file(&pool, &topic, 0, 10, None, 0).await);
    assert!(!insert_file(&pool, &topic, 0, 20, Some(5), 0).await);
    assert!(!insert_file(&pool, &topic, 0, 10, None, 0).await);
}

#[tokio::test]
async fn offsets_must_be_a_non_negative_ordered_range() {
    let Some(url) = postgres().await else { return };
    let pool = connected(&url).await;
    let topic = topic(&pool).await;
    sqlx::query("INSERT INTO snapshot (topic, id, timestamp_ms, epoch) VALUES ($1, 1, 0, 1)")
        .bind(&topic)
        .execute(&pool)
        .await
        .unwrap();

    let offsets = |partition: i32, start: i64, end: i64| {
        sqlx::query(
            "INSERT INTO snapshot_offsets (topic, snapshot_id, partition, start_offset, end_offset) \
             VALUES ($1, 1, $2, $3, $4)",
        )
        .bind(topic.clone())
        .bind(partition)
        .bind(start)
        .bind(end)
        .execute(&pool)
    };

    assert!(offsets(0, 5, 5).await.is_ok());
    assert!(offsets(1, 6, 5).await.is_err());
    assert!(offsets(2, -1, 5).await.is_err());
    assert!(offsets(-1, 0, 5).await.is_err());
}

#[tokio::test]
async fn a_partition_or_a_topic_has_at_most_one_lease() {
    let Some(url) = postgres().await else { return };
    let pool = connected(&url).await;
    let topic = topic(&pool).await;

    let (first, second) = (Uuid::from_u128(1), Uuid::from_u128(2));
    let lease = |partition: Option<i32>, holder: Uuid| {
        sqlx::query(
            "INSERT INTO lease (topic, partition, holder, epoch, expires_at_ms) \
             VALUES ($1, $2, $3, 1, 0)",
        )
        .bind(topic.clone())
        .bind(partition)
        .bind(holder)
        .execute(&pool)
    };

    assert!(lease(Some(0), first).await.is_ok());
    assert!(lease(Some(0), second).await.is_err());
    assert!(lease(None, first).await.is_ok());
    assert!(lease(None, second).await.is_err());
}

#[tokio::test]
async fn deleting_a_topic_deletes_what_references_it() {
    let Some(url) = postgres().await else { return };
    let pool = connected(&url).await;
    let topic = topic(&pool).await;
    assert!(insert_file(&pool, &topic, 0, 10, Some(5), 0).await);
    for statement in [
        "INSERT INTO indexed_range (topic, partition, start_offset, end_offset) VALUES ($1, 0, 0, 10)",
        "INSERT INTO snapshot (topic, id, timestamp_ms, epoch) VALUES ($1, 1, 0, 1)",
        "INSERT INTO snapshot_offsets (topic, snapshot_id, partition, start_offset, end_offset) \
         VALUES ($1, 1, 0, 0, 10)",
        "INSERT INTO lease (topic, partition, holder, epoch, expires_at_ms) \
         VALUES ($1, 0, gen_random_uuid(), 1, 0)",
        "INSERT INTO lease (topic, partition, holder, epoch, expires_at_ms) \
         VALUES ($1, NULL, gen_random_uuid(), 1, 0)",
        "INSERT INTO object (topic, path, kind) VALUES ($1, 'x', 'manifest')",
        "INSERT INTO sketch (topic, day, field_id, sketch) VALUES ($1, 0, 2, '\\x00')",
    ] {
        sqlx::query(statement)
            .bind(&topic)
            .execute(&pool)
            .await
            .unwrap();
    }

    sqlx::query("DELETE FROM topic WHERE name = $1")
        .bind(&topic)
        .execute(&pool)
        .await
        .unwrap();

    let remaining: Vec<i64> = sqlx::query_scalar(
        "SELECT count(*) FROM indexed_range WHERE topic = $1 \
         UNION ALL SELECT count(*) FROM file WHERE topic = $1 \
         UNION ALL SELECT count(*) FROM snapshot WHERE topic = $1 \
         UNION ALL SELECT count(*) FROM snapshot_offsets WHERE topic = $1 \
         UNION ALL SELECT count(*) FROM lease WHERE topic = $1 \
         UNION ALL SELECT count(*) FROM object WHERE topic = $1 \
         UNION ALL SELECT count(*) FROM time_spec WHERE topic = $1 \
         UNION ALL SELECT count(*) FROM sketch WHERE topic = $1",
    )
    .bind(&topic)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(remaining, [0; 8]);
}
