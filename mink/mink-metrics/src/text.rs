use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::histogram::Histogram;
use crate::instruments::Instruments;
use crate::labels::{
    Answer, Forward, Method, Object, Outcome, Request, Route, Scope, Source, Status, Worker,
};

pub(crate) fn render(instruments: &Instruments) -> String {
    let mut out = String::new();

    for (name, counters) in [
        ("mink_indexed_records_total", &instruments.indexed_records),
        ("mink_indexed_bytes_total", &instruments.indexed_bytes),
        ("mink_files_written_total", &instruments.files_written),
    ] {
        family(&mut out, name, "counter");
        for source in Source::ALL {
            let labels = format!(r#"source="{}""#, source.label());
            sample(&mut out, name, &labels, &counters[source as usize]);
        }
    }

    family(&mut out, "mink_kafka_requests_total", "counter");
    for request in Request::ALL {
        for outcome in Outcome::ALL {
            let labels = format!(
                r#"request="{}",result="{}""#,
                request.label(),
                outcome.label()
            );
            let counter = &instruments.kafka_requests[request as usize][outcome as usize];
            sample(&mut out, "mink_kafka_requests_total", &labels, counter);
        }
    }

    counter(
        &mut out,
        "mink_kafka_fetched_bytes_total",
        &instruments.kafka_fetched_bytes,
    );

    outcomes(&mut out, "mink_commits_total", &instruments.commits);
    histogram(
        &mut out,
        "mink_commit_duration_seconds",
        &instruments.commit_duration,
    );

    counter(&mut out, "mink_assemblies_total", &instruments.assemblies);
    counter(
        &mut out,
        "mink_snapshots_expired_total",
        &instruments.snapshots_expired,
    );
    counter(
        &mut out,
        "mink_objects_collected_total",
        &instruments.objects_collected,
    );

    family(&mut out, "mink_s3_requests_total", "counter");
    for method in Method::ALL {
        for object in Object::ALL {
            for status in Status::ALL {
                let labels = format!(
                    r#"method="{}",object="{}",status="{}""#,
                    method.label(),
                    object.label(),
                    status.label()
                );
                let counter =
                    &instruments.s3_requests[method as usize][object as usize][status as usize];
                sample(&mut out, "mink_s3_requests_total", &labels, counter);
            }
        }
    }

    counter(
        &mut out,
        "mink_s3_sent_bytes_total",
        &instruments.s3_sent_bytes,
    );

    family(&mut out, "mink_s3_forwarded_total", "counter");
    for forward in Forward::ALL {
        let labels = format!(r#"result="{}""#, forward.label());
        let counter = &instruments.s3_forwarded[forward as usize];
        sample(&mut out, "mink_s3_forwarded_total", &labels, counter);
    }

    outcomes(&mut out, "mink_builds_total", &instruments.builds);
    histogram(
        &mut out,
        "mink_build_duration_seconds",
        &instruments.build_duration,
    );

    family(&mut out, "mink_catalog_requests_total", "counter");
    for route in Route::ALL {
        for status in Status::ALL {
            let labels = format!(r#"route="{}",status="{}""#, route.label(), status.label());
            let counter = &instruments.catalog_requests[route as usize][status as usize];
            sample(&mut out, "mink_catalog_requests_total", &labels, counter);
        }
    }

    counter(&mut out, "mink_tails_total", &instruments.tails);
    histogram(
        &mut out,
        "mink_tail_duration_seconds",
        &instruments.tail_duration,
    );

    family(&mut out, "mink_registry_requests_total", "counter");
    for answer in Answer::ALL {
        let labels = format!(r#"result="{}""#, answer.label());
        let counter = &instruments.registry_requests[answer as usize];
        sample(&mut out, "mink_registry_requests_total", &labels, counter);
    }

    family(&mut out, "mink_worker_failures_total", "counter");
    for worker in Worker::ALL {
        let labels = format!(r#"worker="{}""#, worker.label());
        let counter = &instruments.worker_failures[worker as usize];
        sample(&mut out, "mink_worker_failures_total", &labels, counter);
    }

    family(&mut out, "mink_admin_requests_total", "counter");
    for status in Status::ALL {
        let labels = format!(r#"status="{}""#, status.label());
        let counter = &instruments.admin_requests[status as usize];
        sample(&mut out, "mink_admin_requests_total", &labels, counter);
    }

    family(&mut out, "mink_leases", "gauge");
    for scope in Scope::ALL {
        let labels = format!(r#"scope="{}""#, scope.label());
        sample(
            &mut out,
            "mink_leases",
            &labels,
            &instruments.leases[scope as usize],
        );
    }

    gauge(&mut out, "mink_cache_bytes", &instruments.cache_bytes);

    out
}

fn counter(out: &mut String, name: &str, value: &AtomicU64) {
    family(out, name, "counter");
    sample(out, name, "", value);
}

fn gauge(out: &mut String, name: &str, value: &AtomicU64) {
    family(out, name, "gauge");
    sample(out, name, "", value);
}

fn outcomes(out: &mut String, name: &str, counters: &[AtomicU64; 2]) {
    family(out, name, "counter");

    for outcome in Outcome::ALL {
        let labels = format!(r#"result="{}""#, outcome.label());
        sample(out, name, &labels, &counters[outcome as usize]);
    }
}

fn histogram(out: &mut String, name: &str, histogram: &Histogram) {
    family(out, name, "histogram");

    let mut count = 0;
    for (bound, cumulative) in histogram.cumulative() {
        let le = match bound {
            Some(micros) => format!("{}", micros as f64 / 1_000_000.0),
            None => "+Inf".to_owned(),
        };
        let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}");
        count = cumulative;
    }

    let _ = writeln!(out, "{name}_sum {}", histogram.sum_seconds());
    let _ = writeln!(out, "{name}_count {count}");
}

fn family(out: &mut String, name: &str, kind: &str) {
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn sample(out: &mut String, name: &str, labels: &str, value: &AtomicU64) {
    let value = value.load(Ordering::Relaxed);

    if labels.is_empty() {
        let _ = writeln!(out, "{name} {value}");
    } else {
        let _ = writeln!(out, "{name}{{{labels}}} {value}");
    }
}
