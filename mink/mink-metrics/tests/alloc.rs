use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use mink_metrics::{
    Answer, Forward, Method, Metrics, Object, Outcome, Request, Route, Scope, Source, Worker,
};

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const EVENTS: u64 = 100_000;

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

#[test]
fn recording_never_allocates() {
    let metrics = Metrics::new();

    let before = allocations();
    for event in 0..EVENTS {
        let elapsed = Duration::from_micros(event % 20_000_000);
        metrics.indexed(Source::Live, 1, 10, 100);
        metrics.kafka_request(Request::Fetch, Outcome::Ok);
        metrics.fetched(100);
        metrics.commit(Outcome::Ok, elapsed);
        metrics.assembled(1);
        metrics.expired(1);
        metrics.collected(1);
        metrics.s3_request(Method::Get, Object::Data, 206);
        metrics.sent(100);
        metrics.forwarded(Forward::Ok);
        metrics.cache_size(event * 10);
        metrics.build(Outcome::Error, elapsed);
        metrics.catalog_request(Route::Load, 200);
        metrics.tail(elapsed);
        metrics.registry_request(Answer::Missing);
        metrics.worker_failed(Worker::Writer);
        metrics.admin_request(404);
        metrics.leases(Scope::Partition, event);
    }
    let recorded = allocations() - before;

    let text = metrics.render();
    assert_eq!(recorded, 0);
    assert!(text.contains(r#"mink_indexed_records_total{source="live"} 1000000"#));
    assert!(
        text.contains(r#"mink_s3_requests_total{method="get",object="data",status="2xx"} 100000"#)
    );
    assert!(text.contains("mink_commit_duration_seconds_count 100000"));
    assert!(text.contains(r#"mink_admin_requests_total{status="4xx"} 100000"#));
    assert!(text.contains(r#"mink_leases{scope="partition"} 99999"#));
}
