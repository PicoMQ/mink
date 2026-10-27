mod histogram;
mod instruments;
mod labels;
mod text;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use instruments::Instruments;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};

pub use labels::{
    Answer, Forward, Method, Object, Outcome, Request, Route, Scope, Source, Status, Worker,
};

#[derive(Clone)]
pub struct Metrics {
    instruments: Arc<Instruments>,
    recorder: Arc<PrometheusRecorder>,
    handle: PrometheusHandle,
}

impl Default for Metrics {
    fn default() -> Metrics {
        Metrics::new()
    }
}

impl Metrics {
    pub fn new() -> Metrics {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        Metrics {
            instruments: Arc::default(),
            recorder: Arc::new(recorder),
            handle,
        }
    }

    pub fn render(&self) -> String {
        let mut text = text::render(&self.instruments);

        text.push_str(&self.handle.render());
        text
    }

    pub fn with_recorder<T>(&self, work: impl FnOnce() -> T) -> T {
        metrics::with_local_recorder(self.recorder.as_ref(), work)
    }

    pub fn upkeep(&self) {
        self.handle.run_upkeep();
    }

    pub fn indexed(&self, source: Source, files: u64, records: u64, bytes: u64) {
        let slot = source as usize;

        add(&self.instruments.files_written[slot], files);
        add(&self.instruments.indexed_records[slot], records);
        add(&self.instruments.indexed_bytes[slot], bytes);
    }

    pub fn kafka_request(&self, request: Request, outcome: Outcome) {
        add(
            &self.instruments.kafka_requests[request as usize][outcome as usize],
            1,
        );
    }

    pub fn fetched(&self, bytes: u64) {
        add(&self.instruments.kafka_fetched_bytes, bytes);
    }

    pub fn commit(&self, outcome: Outcome, elapsed: Duration) {
        add(&self.instruments.commits[outcome as usize], 1);
        self.instruments.commit_duration.observe(elapsed);
    }

    pub fn assembled(&self, count: u64) {
        add(&self.instruments.assemblies, count);
    }

    pub fn expired(&self, count: u64) {
        add(&self.instruments.snapshots_expired, count);
    }

    pub fn collected(&self, count: u64) {
        add(&self.instruments.objects_collected, count);
    }

    pub fn s3_request(&self, method: Method, object: Object, status: u16) {
        let status = Status::of(status) as usize;

        add(
            &self.instruments.s3_requests[method as usize][object as usize][status],
            1,
        );
    }

    pub fn sent(&self, bytes: u64) {
        add(&self.instruments.s3_sent_bytes, bytes);
    }

    pub fn forwarded(&self, forward: Forward) {
        add(&self.instruments.s3_forwarded[forward as usize], 1);
    }

    pub fn cache_size(&self, bytes: u64) {
        self.instruments.cache_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn build(&self, outcome: Outcome, elapsed: Duration) {
        add(&self.instruments.builds[outcome as usize], 1);
        self.instruments.build_duration.observe(elapsed);
    }

    pub fn catalog_request(&self, route: Route, status: u16) {
        let status = Status::of(status) as usize;

        add(
            &self.instruments.catalog_requests[route as usize][status],
            1,
        );
    }

    pub fn tail(&self, elapsed: Duration) {
        add(&self.instruments.tails, 1);
        self.instruments.tail_duration.observe(elapsed);
    }

    pub fn registry_request(&self, answer: Answer) {
        add(&self.instruments.registry_requests[answer as usize], 1);
    }

    pub fn worker_failed(&self, worker: Worker) {
        add(&self.instruments.worker_failures[worker as usize], 1);
    }

    pub fn admin_request(&self, status: u16) {
        add(
            &self.instruments.admin_requests[Status::of(status) as usize],
            1,
        );
    }

    pub fn leases(&self, scope: Scope, count: u64) {
        self.instruments.leases[scope as usize].store(count, Ordering::Relaxed);
    }
}

impl fmt::Debug for Metrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Metrics").finish_non_exhaustive()
    }
}

fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}
