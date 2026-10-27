use std::time::Duration;

use mink_metrics::{Metrics, Outcome, Request, Source};

const EXPOSITION: &str = include_str!("exposition.txt");

fn series(text: &str) -> Vec<&str> {
    text.lines()
        .filter_map(|line| line.rsplit_once(' ').map(|(series, _)| series))
        .collect()
}

#[test]
fn the_text_lists_every_series_from_the_start() {
    let metrics = Metrics::new();

    let fresh = metrics.render();
    metrics.indexed(Source::Backfill, 2, 20, 200);
    metrics.kafka_request(Request::Metadata, Outcome::Error);
    metrics.commit(Outcome::Ok, Duration::from_millis(30));
    let worked = metrics.render();

    assert_eq!(fresh, EXPOSITION);
    assert_eq!(series(&worked), series(&fresh));
    assert!(worked.contains(r#"mink_indexed_records_total{source="backfill"} 20"#));
    assert!(worked.contains(r#"mink_commit_duration_seconds_bucket{le="0.05"} 1"#));
}
