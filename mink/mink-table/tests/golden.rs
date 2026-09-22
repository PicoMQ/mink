//! Checks bucketing and hashing against fixtures produced by the JVM implementation.

use std::fs;
use std::path::Path;

use mink_common::murmur;
use mink_table::{BucketId, Bucketing};
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    key: String,
    murmur: i32,
    buckets: Vec<Expected>,
}

#[derive(Deserialize)]
struct Expected {
    count: u32,
    native: u32,
    paimon: u32,
    iceberg: u32,
}

// `fixtures/bucketing.json` comes from the Java bucketing functions; regenerate it from Java only.
fn cases() -> Vec<Case> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bucketing.json");
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn fixture_covers_every_tail_length_and_a_high_bit_byte() {
    let cases = cases();
    assert!(cases.len() >= 30);
    let lengths: Vec<usize> = cases.iter().map(|case| case.key.len() / 2).collect();
    for tail in 0..4 {
        assert!(lengths.iter().any(|len| len % 4 == tail && *len >= 4));
    }
    assert!(cases.iter().any(|case| case.key == "80"));
}

#[test]
fn buckets_match_java() {
    for case in cases() {
        let key = hex::decode(&case.key).unwrap();
        for expected in &case.buckets {
            let check = |bucketing: Bucketing, want: u32| {
                let got = bucketing.bucket(&key, expected.count).unwrap();
                assert_eq!(
                    got,
                    BucketId(want),
                    "{bucketing:?} key={} count={}",
                    case.key,
                    expected.count
                );
            };
            check(Bucketing::Native, expected.native);
            check(Bucketing::Paimon, expected.paimon);
            check(Bucketing::Iceberg, expected.iceberg);
        }
    }
}

#[test]
fn native_murmur_matches_fixtures() {
    for case in cases() {
        let key = hex::decode(&case.key).unwrap();
        assert_eq!(murmur::native32(&key), case.murmur, "key={}", case.key);
    }
}
