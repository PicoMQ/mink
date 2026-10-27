use std::sync::atomic::AtomicU64;

use crate::histogram::Histogram;

#[derive(Default)]
pub(crate) struct Instruments {
    pub(crate) indexed_records: [AtomicU64; 2],
    pub(crate) indexed_bytes: [AtomicU64; 2],
    pub(crate) files_written: [AtomicU64; 2],
    pub(crate) kafka_requests: [[AtomicU64; 2]; 3],
    pub(crate) kafka_fetched_bytes: AtomicU64,
    pub(crate) commits: [AtomicU64; 2],
    pub(crate) commit_duration: Histogram,
    pub(crate) assemblies: AtomicU64,
    pub(crate) snapshots_expired: AtomicU64,
    pub(crate) objects_collected: AtomicU64,
    pub(crate) s3_requests: [[[AtomicU64; 3]; 2]; 2],
    pub(crate) s3_sent_bytes: AtomicU64,
    pub(crate) s3_forwarded: [AtomicU64; 2],
    pub(crate) builds: [AtomicU64; 2],
    pub(crate) build_duration: Histogram,
    pub(crate) catalog_requests: [[AtomicU64; 3]; 5],
    pub(crate) tails: AtomicU64,
    pub(crate) tail_duration: Histogram,
    pub(crate) registry_requests: [AtomicU64; 3],
    pub(crate) worker_failures: [AtomicU64; 2],
    pub(crate) admin_requests: [AtomicU64; 3],
    pub(crate) leases: [AtomicU64; 2],
    pub(crate) cache_bytes: AtomicU64,
}
