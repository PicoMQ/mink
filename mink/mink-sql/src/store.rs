//! The store interface for the log, snapshots, lease and heartbeats, implemented once over any SQL dialect.

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{
    AssertSqlSafe, ColumnIndex, Database, Decode, Encode, Error, Executor, IntoArguments, Pool,
    Postgres, Row, Sqlite, Type,
};

pub(crate) const DEFAULT_LEASE_TTL_MS: i64 = 10_000;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sql: {0}")]
    Sql(#[from] Error),
    #[error("corrupt store: {0}")]
    Corrupt(String),
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn append(&self, idx: u64, payload: &[u8]) -> Result<bool, StoreError>;

    async fn last_idx(&self) -> Result<u64, StoreError>;

    async fn fetch_after(&self, after: u64, limit: u32) -> Result<Vec<(u64, Vec<u8>)>, StoreError>;

    async fn load_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError>;

    async fn snapshot_idx(&self) -> Result<Option<u64>, StoreError>;

    async fn store_snapshot(&self, applied_idx: u64, payload: &[u8]) -> Result<(), StoreError>;

    async fn truncate_log(&self, up_to: u64) -> Result<(), StoreError>;

    async fn acquire_lease(
        &self,
        holder: &str,
        prev_epoch: Option<u64>,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Option<u64>, StoreError>;

    async fn release_lease(&self, holder: &str, epoch: u64) -> Result<(), StoreError>;

    async fn heartbeat_node(
        &self,
        node_id: i32,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<(), StoreError>;

    async fn expire_node(&self, node_id: i32) -> Result<(), StoreError>;

    async fn live_nodes(&self, now_ms: i64) -> Result<Vec<i32>, StoreError>;
}

fn to_i64(value: u64, what: &str) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::Corrupt(format!("{what} {value} exceeds i64")))
}

fn to_u64(value: i64, what: &str) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Corrupt(format!("{what} {value} is negative")))
}

fn insert_outcome(result: Result<(), Error>) -> Result<bool, StoreError> {
    match result {
        Ok(()) => Ok(true),
        Err(Error::Database(e)) if e.is_unique_violation() => Ok(false),
        Err(e) => Err(e.into()),
    }
}

pub trait Dialect: Database {
    const MIGRATIONS: &'static [&'static str];
    // SQLite in WAL mode can fail a deferred write transaction without honoring the busy timeout.
    const MIGRATE_IN_TRANSACTION: bool;

    fn placeholder(n: usize) -> String;
}

impl Dialect for Sqlite {
    const MIGRATIONS: &'static [&'static str] = &[
        "CREATE TABLE IF NOT EXISTS meta_log (\
             idx INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
        "CREATE TABLE IF NOT EXISTS meta_snapshot (\
             id INTEGER PRIMARY KEY, applied_idx INTEGER NOT NULL, payload BLOB NOT NULL)",
        "CREATE TABLE IF NOT EXISTS meta_lease (\
             id INTEGER PRIMARY KEY, holder TEXT NOT NULL, \
             epoch INTEGER NOT NULL, expires_at_ms INTEGER NOT NULL)",
        // Seeded so acquire is always one CAS UPDATE with no INSERT race.
        "INSERT OR IGNORE INTO meta_lease (id, holder, epoch, expires_at_ms) VALUES (0, '', 0, 0)",
        "CREATE TABLE IF NOT EXISTS meta_node (\
             node_id INTEGER PRIMARY KEY, expires_at_ms INTEGER NOT NULL)",
    ];

    const MIGRATE_IN_TRANSACTION: bool = false;

    fn placeholder(_: usize) -> String {
        "?".to_owned()
    }
}

impl Dialect for Postgres {
    const MIGRATIONS: &'static [&'static str] = &[
        // Concurrent CREATE TABLE IF NOT EXISTS races in Postgres; the advisory lock serializes first boot.
        "SELECT pg_advisory_xact_lock(x'7069636f'::int8)",
        "CREATE TABLE IF NOT EXISTS meta_log (\
             idx BIGINT PRIMARY KEY, payload BYTEA NOT NULL)",
        "CREATE TABLE IF NOT EXISTS meta_snapshot (\
             id BIGINT PRIMARY KEY, applied_idx BIGINT NOT NULL, payload BYTEA NOT NULL)",
        "CREATE TABLE IF NOT EXISTS meta_lease (\
             id BIGINT PRIMARY KEY, holder TEXT NOT NULL, \
             epoch BIGINT NOT NULL, expires_at_ms BIGINT NOT NULL)",
        "INSERT INTO meta_lease (id, holder, epoch, expires_at_ms) \
         VALUES (0, '', 0, 0) ON CONFLICT (id) DO NOTHING",
        "CREATE TABLE IF NOT EXISTS meta_node (\
             node_id INTEGER PRIMARY KEY, expires_at_ms BIGINT NOT NULL)",
    ];

    const MIGRATE_IN_TRANSACTION: bool = true;

    fn placeholder(n: usize) -> String {
        format!("${n}")
    }
}

// Templates are literals; only the placeholder syntax is rewritten.
fn sql<DB: Dialect>(template: &str) -> AssertSqlSafe<String> {
    let mut out = String::with_capacity(template.len());
    for (n, part) in template.split('?').enumerate() {
        if n > 0 {
            out.push_str(&DB::placeholder(n));
        }
        out.push_str(part);
    }

    AssertSqlSafe(out)
}

pub struct SqlStore<DB: Database> {
    pool: Pool<DB>,
}

pub type SqliteStore = SqlStore<Sqlite>;
pub type PgStore = SqlStore<Postgres>;

impl SqliteStore {
    pub async fn memory() -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new().in_memory(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;

        Self::migrated(pool).await
    }

    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        Self::migrated(pool).await
    }
}

impl PgStore {
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;

        Self::migrated(pool).await
    }
}

impl<DB> SqlStore<DB>
where
    DB: Dialect,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    async fn migrated(pool: Pool<DB>) -> Result<Self, StoreError> {
        if DB::MIGRATE_IN_TRANSACTION {
            let mut tx = pool.begin().await?;
            for statement in DB::MIGRATIONS {
                sqlx::query(*statement).execute(&mut *tx).await?;
            }
            tx.commit().await?;
        } else {
            for statement in DB::MIGRATIONS {
                sqlx::query(*statement).execute(&pool).await?;
            }
        }

        Ok(Self { pool })
    }
}

#[async_trait]
impl<DB> Store for SqlStore<DB>
where
    DB: Dialect,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    for<'q> i64: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
    for<'q> i32: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'q> Vec<u8>: Decode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'c> &'c str: ColumnIndex<DB::Row>,
{
    async fn append(&self, idx: u64, payload: &[u8]) -> Result<bool, StoreError> {
        let idx = to_i64(idx, "log idx")?;
        let query = sql::<DB>("INSERT INTO meta_log (idx, payload) VALUES (?, ?)");
        let result = sqlx::query(query)
            .bind(idx)
            .bind(payload)
            .execute(&self.pool)
            .await
            .map(|_| ());

        insert_outcome(result)
    }

    async fn last_idx(&self) -> Result<u64, StoreError> {
        let row = sqlx::query(
            "SELECT COALESCE((SELECT MAX(idx) FROM meta_log), 0) AS log_idx, \
                    COALESCE((SELECT applied_idx FROM meta_snapshot WHERE id = 0), 0) AS snap_idx",
        )
        .fetch_one(&self.pool)
        .await?;
        let log_idx: i64 = row.get("log_idx");
        let snap_idx: i64 = row.get("snap_idx");

        to_u64(log_idx.max(snap_idx), "last idx")
    }

    async fn fetch_after(&self, after: u64, limit: u32) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let after = to_i64(after, "after idx")?;
        let query =
            sql::<DB>("SELECT idx, payload FROM meta_log WHERE idx > ? ORDER BY idx ASC LIMIT ?");
        let rows = sqlx::query(query)
            .bind(after)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|row| Ok((to_u64(row.get("idx"), "log idx")?, row.get("payload"))))
            .collect()
    }

    async fn load_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let row = sqlx::query("SELECT applied_idx, payload FROM meta_snapshot WHERE id = 0")
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| {
            Ok((
                to_u64(row.get("applied_idx"), "snapshot idx")?,
                row.get("payload"),
            ))
        })
        .transpose()
    }

    async fn snapshot_idx(&self) -> Result<Option<u64>, StoreError> {
        let row = sqlx::query("SELECT applied_idx FROM meta_snapshot WHERE id = 0")
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| to_u64(row.get("applied_idx"), "snapshot idx"))
            .transpose()
    }

    async fn store_snapshot(&self, applied_idx: u64, payload: &[u8]) -> Result<(), StoreError> {
        let applied_idx = to_i64(applied_idx, "snapshot idx")?;
        let query = sql::<DB>(
            "INSERT INTO meta_snapshot (id, applied_idx, payload) VALUES (0, ?, ?) \
             ON CONFLICT (id) DO UPDATE SET \
                 applied_idx = excluded.applied_idx, payload = excluded.payload \
             WHERE excluded.applied_idx > meta_snapshot.applied_idx",
        );
        sqlx::query(query)
            .bind(applied_idx)
            .bind(payload)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn truncate_log(&self, up_to: u64) -> Result<(), StoreError> {
        let up_to = to_i64(up_to, "truncate idx")?;
        let query = sql::<DB>("DELETE FROM meta_log WHERE idx <= ?");
        sqlx::query(query).bind(up_to).execute(&self.pool).await?;

        Ok(())
    }

    async fn acquire_lease(
        &self,
        holder: &str,
        prev_epoch: Option<u64>,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Option<u64>, StoreError> {
        match prev_epoch {
            Some(epoch) => {
                let query = sql::<DB>(
                    "UPDATE meta_lease SET expires_at_ms = ? \
                     WHERE id = 0 AND holder = ? AND epoch = ? AND expires_at_ms >= ? \
                     RETURNING epoch",
                );
                let row = sqlx::query(query)
                    .bind(now_ms + ttl_ms)
                    .bind(holder)
                    .bind(to_i64(epoch, "lease epoch")?)
                    .bind(now_ms)
                    .fetch_optional(&self.pool)
                    .await?;

                Ok(row.map(|_| epoch))
            }
            None => {
                let query = sql::<DB>(
                    "UPDATE meta_lease SET holder = ?, epoch = epoch + 1, expires_at_ms = ? \
                     WHERE id = 0 AND (expires_at_ms < ? OR holder = ?) \
                     RETURNING epoch",
                );
                let row = sqlx::query(query)
                    .bind(holder)
                    .bind(now_ms + ttl_ms)
                    .bind(now_ms)
                    .bind(holder)
                    .fetch_optional(&self.pool)
                    .await?;

                row.map(|row| to_u64(row.get("epoch"), "lease epoch"))
                    .transpose()
            }
        }
    }

    async fn release_lease(&self, holder: &str, epoch: u64) -> Result<(), StoreError> {
        let epoch = to_i64(epoch, "lease epoch")?;
        let query = sql::<DB>(
            "UPDATE meta_lease SET expires_at_ms = 0 WHERE id = 0 AND holder = ? AND epoch = ?",
        );
        sqlx::query(query)
            .bind(holder)
            .bind(epoch)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn heartbeat_node(
        &self,
        node_id: i32,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<(), StoreError> {
        let query = sql::<DB>(
            "INSERT INTO meta_node (node_id, expires_at_ms) VALUES (?, ?) \
             ON CONFLICT (node_id) DO UPDATE SET expires_at_ms = excluded.expires_at_ms",
        );
        sqlx::query(query)
            .bind(node_id)
            .bind(now_ms + ttl_ms)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn expire_node(&self, node_id: i32) -> Result<(), StoreError> {
        let query = sql::<DB>("UPDATE meta_node SET expires_at_ms = 0 WHERE node_id = ?");
        sqlx::query(query).bind(node_id).execute(&self.pool).await?;

        Ok(())
    }

    async fn live_nodes(&self, now_ms: i64) -> Result<Vec<i32>, StoreError> {
        let query =
            sql::<DB>("SELECT node_id FROM meta_node WHERE expires_at_ms >= ? ORDER BY node_id");
        let rows = sqlx::query(query)
            .bind(now_ms)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|row| row.get::<i32, _>("node_id"))
            .collect())
    }
}

#[doc(hidden)]
pub async fn contract_suite(store: &dyn Store) {
    assert_eq!(store.last_idx().await.unwrap(), 0);
    assert_eq!(store.fetch_after(0, 100).await.unwrap(), vec![]);
    assert_eq!(store.load_snapshot().await.unwrap(), None);
    assert_eq!(store.snapshot_idx().await.unwrap(), None);

    assert!(store.append(1, b"one").await.unwrap());
    assert!(store.append(2, &[0u8, 255, 7, 0]).await.unwrap());
    assert!(store.append(3, b"").await.unwrap());
    assert_eq!(store.last_idx().await.unwrap(), 3);

    assert!(!store.append(2, b"usurper").await.unwrap());
    let rows = store.fetch_after(0, 100).await.unwrap();
    assert_eq!(
        rows,
        vec![
            (1, b"one".to_vec()),
            (2, vec![0u8, 255, 7, 0]),
            (3, Vec::new()),
        ]
    );

    assert_eq!(
        store.fetch_after(1, 1).await.unwrap(),
        vec![(2, vec![0u8, 255, 7, 0])]
    );
    assert_eq!(store.fetch_after(3, 100).await.unwrap(), vec![]);

    store.store_snapshot(2, b"snap-v1").await.unwrap();
    store.store_snapshot(3, b"snap-v2").await.unwrap();
    assert_eq!(
        store.load_snapshot().await.unwrap(),
        Some((3, b"snap-v2".to_vec()))
    );
    store.store_snapshot(2, b"stale").await.unwrap();
    assert_eq!(
        store.load_snapshot().await.unwrap(),
        Some((3, b"snap-v2".to_vec()))
    );
    assert_eq!(store.snapshot_idx().await.unwrap(), Some(3));
    store.truncate_log(3).await.unwrap();
    assert_eq!(store.fetch_after(0, 100).await.unwrap(), vec![]);
    assert_eq!(
        store.last_idx().await.unwrap(),
        3,
        "snapshot idx survives truncation"
    );
    assert!(store.append(4, b"post-truncate").await.unwrap());
    assert_eq!(store.last_idx().await.unwrap(), 4);

    let now = 1_000;
    let ttl = 100;
    let e1 = store
        .acquire_lease("a", None, now, ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(e1 >= 1);
    assert_eq!(
        store.acquire_lease("b", None, now + 50, ttl).await.unwrap(),
        None
    );
    assert_eq!(
        store
            .acquire_lease("a", Some(e1), now + 50, ttl)
            .await
            .unwrap(),
        Some(e1)
    );
    assert_eq!(
        store
            .acquire_lease("b", None, now + 120, ttl)
            .await
            .unwrap(),
        None
    );
    let e2 = store
        .acquire_lease("b", None, now + 200, ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(e2 > e1);
    assert_eq!(
        store
            .acquire_lease("a", Some(e1), now + 210, ttl)
            .await
            .unwrap(),
        None
    );
    let e3 = store
        .acquire_lease("b", None, now + 220, ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(e3 > e2);
    store.release_lease("b", e2).await.unwrap();
    assert_eq!(
        store
            .acquire_lease("a", None, now + 230, ttl)
            .await
            .unwrap(),
        None
    );
    store.release_lease("b", e3).await.unwrap();
    let e4 = store
        .acquire_lease("a", None, now + 230, ttl)
        .await
        .unwrap()
        .unwrap();
    assert!(e4 > e3);

    assert_eq!(store.live_nodes(now).await.unwrap(), Vec::<i32>::new());
    store.heartbeat_node(2, now, ttl).await.unwrap();
    store.heartbeat_node(1, now, ttl).await.unwrap();
    assert_eq!(store.live_nodes(now + 100).await.unwrap(), vec![1, 2]);
    assert_eq!(
        store.live_nodes(now + 101).await.unwrap(),
        Vec::<i32>::new()
    );
    store.heartbeat_node(1, now + 100, ttl).await.unwrap();
    assert_eq!(store.live_nodes(now + 150).await.unwrap(), vec![1]);
    store.expire_node(1).await.unwrap();
    assert_eq!(
        store.live_nodes(now + 150).await.unwrap(),
        Vec::<i32>::new()
    );
    store.heartbeat_node(1, now + 150, ttl).await.unwrap();
    assert_eq!(store.live_nodes(now + 150).await.unwrap(), vec![1]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_memory_contract() {
        let store = SqliteStore::memory().await.unwrap();
        contract_suite(&store).await;
    }

    #[tokio::test]
    async fn sqlite_file_contract_and_reopen_durability() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        {
            let store = SqliteStore::open(&path).await.unwrap();
            contract_suite(&store).await;
        }
        let store = SqliteStore::open(&path).await.unwrap();
        assert_eq!(store.last_idx().await.unwrap(), 4);
        assert_eq!(
            store.fetch_after(3, 10).await.unwrap(),
            vec![(4, b"post-truncate".to_vec())]
        );
        assert_eq!(
            store.load_snapshot().await.unwrap(),
            Some((3, b"snap-v2".to_vec()))
        );
    }

    #[tokio::test]
    async fn sqlite_append_race_single_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("race.db");
        let a = SqliteStore::open(&path).await.unwrap();
        let b = SqliteStore::open(&path).await.unwrap();
        let mut wins = 0;
        for idx in 1..=20u64 {
            let (pa, pb) = (format!("a-{idx}"), format!("b-{idx}"));
            let (ra, rb) = tokio::join!(a.append(idx, pa.as_bytes()), b.append(idx, pb.as_bytes()));
            let (ra, rb) = (ra.unwrap(), rb.unwrap());
            assert!(ra ^ rb, "exactly one writer must win idx {idx}");
            wins += u64::from(ra);
        }
        let rows = a.fetch_after(0, 100).await.unwrap();
        assert_eq!(rows.len(), 20);
        for (idx, payload) in rows {
            let text = String::from_utf8(payload).unwrap();
            assert!(text == format!("a-{idx}") || text == format!("b-{idx}"));
        }
        assert!(wins <= 20);
    }
}
