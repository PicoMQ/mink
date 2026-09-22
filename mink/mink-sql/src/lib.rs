//! SQL-backed metadata sink: an append-only command log with snapshots in SQLite or Postgres, plus
//! the coordinator lease and node heartbeats kept in the same database.

mod heartbeat;
mod lease;
mod sink;
pub mod store;
mod worker;

pub use heartbeat::{Heartbeat, HeartbeatConfig};
pub use lease::{Lease, LeaseConfig};
pub use sink::{Config, Error, Sink};
pub use store::{Dialect, PgStore, SqlStore, SqliteStore, Store, StoreError};
