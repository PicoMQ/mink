//! Flight server knobs: compression, default read sizes, leader wait, and extra entries to describe.

use std::collections::BTreeMap;
use std::time::Duration;

use mink_record::Compression;

#[derive(Debug, Clone)]
pub struct Config {
    pub compression: Compression,
    pub default_scan_bytes: usize,
    pub default_snapshot_rows: usize,
    pub leader_wait: Duration,
    pub describe: BTreeMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            compression: Compression::default(),
            default_scan_bytes: 16 * 1024 * 1024,
            default_snapshot_rows: 4096,
            leader_wait: Duration::from_secs(30),
            describe: BTreeMap::new(),
        }
    }
}
