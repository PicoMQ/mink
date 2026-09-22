//! SQL over Mink tables with DataFusion: the cluster's databases as a catalog, each table read as
//! its lake snapshot plus the log tail, pinned per statement.

mod catalog;
mod config;
mod engine;
mod error;
mod exec;
mod keys;
mod log;
mod predicate;
mod provider;

pub use catalog::Catalog;
pub use config::Config;
pub use engine::Engine;
pub use error::{Error, Result};
pub use provider::MinkTable;
