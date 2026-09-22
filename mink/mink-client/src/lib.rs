//! The client library: cluster discovery with leader following, table handles, writers, scans, tails, lookups
//! and catalog administration over the Flight protocol.

mod admin;
mod cluster;
mod connection;
mod error;
mod lookup;
mod scan;
mod table;
mod tail;
mod write;

pub use admin::Admin;
pub use cluster::{Cluster, MAX_REDIRECTS};
pub use connection::{Connection, Session};
pub use error::Error;
pub use lookup::Lookup;
pub use mink_protocol as proto;
pub use scan::{Batch, Snapshot};
pub use table::Table;
pub use tail::Tail;
pub use write::{Append, Routed, Upsert};
