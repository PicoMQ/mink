mod files;
mod leases;
mod members;
mod objects;
mod ranges;
mod registry;
mod schemas;
mod sketches;
mod snapshots;
mod specs;
mod stats;
#[cfg(test)]
mod testing;
mod topics;

use mink_core::{File, Partition, Scope};
use sqlx::PgPool;
use sqlx::migrate::MigrateError;

pub use files::{Files, StoredFile};
pub use leases::{Lease, LeaseHolder, Leases};
pub use members::Members;
pub use objects::Objects;
pub use ranges::IndexedRanges;
pub use registry::{ListedSchema, Registry};
pub use schemas::Schemas;
pub use sketches::Sketches;
pub use snapshots::{SnapshotTotals, Snapshots};
pub use specs::TimeSpecs;
pub use stats::{parse_stats, stats_json};
pub use topics::{TableSummary, Topics, Totals};

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Sql(sqlx::Error),
    #[error("postgres is unreachable: {0}")]
    Unavailable(sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] MigrateError),
    #[error(transparent)]
    Core(#[from] mink_core::Error),
    #[error("a stored row is invalid: {0}")]
    Corrupt(String),
    #[error("{0} does not fit its column")]
    OutOfRange(String),
    #[error("topic {topic} is already a table")]
    AlreadyTable { topic: String },
    #[error("topic {topic} is not a table")]
    NoTable { topic: String },
    #[error("table {topic} is being removed")]
    TableRemoving { topic: String },
    #[error("topic {topic} cannot go from {registered} partitions to {given}")]
    FewerPartitions {
        topic: String,
        registered: i32,
        given: i32,
    },
    #[error("snapshot {given} of {topic} conflicts: the next id is {expected}")]
    SnapshotConflict {
        topic: String,
        expected: i64,
        given: i64,
    },
    #[error("the lease on {0:?} is not held by this holder and epoch")]
    Fenced(Scope),
    #[error("a lease on {0:?} cannot do this")]
    LeaseScope(Scope),
    #[error("the indexed range of {0:?} has not been started")]
    RangeNotStarted(Partition),
    #[error("{0:?} does not extend its partition's indexed range")]
    NotAdjacent(File),
    #[error("{0:?} overlaps another file of its partition")]
    OverlappingFile(File),
    #[error("{0:?} is not a file of the leased partition")]
    FileOutsideLease(File),
    #[error("offset {offset} is not a file boundary in the indexed range of {partition:?}")]
    NotABoundary { partition: Partition, offset: i64 },
    #[error("the new snapshot has no offsets for {0:?}, which the latest snapshot has")]
    MissingPartition(Partition),
    #[error("the end offset of {0:?} would move backwards")]
    OffsetsMovedBack(Partition),
    #[error("snapshot {id} of {topic} is the latest and cannot expire")]
    LatestSnapshot { topic: String, id: i64 },
    #[error(
        "a schema of {topic} was merged over table schema {based_on}, but the latest is {latest}"
    )]
    SchemaConflict {
        topic: String,
        based_on: i32,
        latest: i32,
    },
    #[error("{0:?} cannot take part in this assembly")]
    MixedAssembly(File),
    #[error("{0:?} has no stored footer")]
    MissingFooter(File),
    #[error("a schema of {topic} merged over table schema {based_on} cannot be schema {id}")]
    UnexpectedSchemaId {
        topic: String,
        based_on: i32,
        id: i32,
    },
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Error {
        let unreachable = match &error {
            sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => true,
            sqlx::Error::Database(database) => database
                .code()
                .is_some_and(|code| code.starts_with("08") || code.starts_with("57P")),
            _ => false,
        };

        match unreachable {
            true => Error::Unavailable(error),
            false => Error::Sql(error),
        }
    }
}

impl Store {
    pub async fn connect(url: &str) -> Result<Store, Error> {
        let pool = PgPool::connect(url).await?;

        sqlx::migrate!().run(&pool).await?;

        Ok(Store { pool })
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }

    pub fn members(&self) -> Members<'_> {
        Members::new(&self.pool)
    }

    pub fn topics(&self) -> Topics<'_> {
        Topics::new(&self.pool)
    }

    pub fn snapshots(&self) -> Snapshots<'_> {
        Snapshots::new(&self.pool)
    }

    pub fn files(&self) -> Files<'_> {
        Files::new(&self.pool)
    }

    pub fn indexed_ranges(&self) -> IndexedRanges<'_> {
        IndexedRanges::new(&self.pool)
    }

    pub fn leases(&self) -> Leases<'_> {
        Leases::new(&self.pool)
    }

    pub fn objects(&self) -> Objects<'_> {
        Objects::new(&self.pool)
    }

    pub fn registry(&self) -> Registry<'_> {
        Registry::new(&self.pool)
    }

    pub fn schemas(&self) -> Schemas<'_> {
        Schemas::new(&self.pool)
    }

    pub fn time_specs(&self) -> TimeSpecs<'_> {
        TimeSpecs::new(&self.pool)
    }

    pub fn sketches(&self) -> Sketches<'_> {
        Sketches::new(&self.pool)
    }
}
