use std::sync::atomic::AtomicU64;

use crate::histogram::Histogram;
use crate::labels::{
    Answer, Forward, Method, Object, Outcome, Request, Route, Scope, Source, Status, Worker,
};

#[derive(Default)]
pub(crate) struct Instruments {
    pub(crate) indexed_records: [AtomicU64; Source::ALL.len()],
    pub(crate) indexed_bytes: [AtomicU64; Source::ALL.len()],
    pub(crate) files_written: [AtomicU64; Source::ALL.len()],
    pub(crate) kafka_requests: [[AtomicU64; Outcome::ALL.len()]; Request::ALL.len()],
    pub(crate) kafka_fetched_bytes: AtomicU64,
    pub(crate) commits: [AtomicU64; Outcome::ALL.len()],
    pub(crate) commit_duration: Histogram,
    pub(crate) assemblies: AtomicU64,
    pub(crate) snapshots_expired: AtomicU64,
    pub(crate) objects_collected: AtomicU64,
    pub(crate) s3_requests:
        [[[AtomicU64; Status::ALL.len()]; Object::ALL.len()]; Method::ALL.len()],
    pub(crate) s3_sent_bytes: AtomicU64,
    pub(crate) s3_forwarded: [AtomicU64; Forward::ALL.len()],
    pub(crate) builds: [AtomicU64; Outcome::ALL.len()],
    pub(crate) build_duration: Histogram,
    pub(crate) catalog_requests: [[AtomicU64; Status::ALL.len()]; Route::ALL.len()],
    pub(crate) tails: AtomicU64,
    pub(crate) tail_duration: Histogram,
    pub(crate) registry_requests: [AtomicU64; Answer::ALL.len()],
    pub(crate) worker_failures: [AtomicU64; Worker::ALL.len()],
    pub(crate) admin_requests: [AtomicU64; Status::ALL.len()],
    pub(crate) leases: [AtomicU64; Scope::ALL.len()],
    pub(crate) cache_bytes: AtomicU64,
    pub(crate) tier_gaps: AtomicU64,
    pub(crate) tier_skipped_offsets: AtomicU64,
    pub(crate) tier_lag_seconds: AtomicU64,
}
