//! Snapshot encode and decode at scale, and apply throughput.

use std::time::Instant;

use mink_metadata::{Command, State, apply};

const NODE_A: i32 = 1;
const NODE_B: i32 = 2;
const EPOCH: i64 = 1;

fn build(total: u64, open_every: u64) -> State {
    let mut state = State::new();
    for node_id in [NODE_A, NODE_B] {
        apply(
            &mut state,
            &Command::RegisterNode {
                node_id,
                node_epoch: EPOCH,
                http_address: String::new(),
                slots: 1,
                protocol_addresses: Default::default(),
            },
        )
        .unwrap();
    }
    for i in 0..total {
        apply(
            &mut state,
            &Command::CreateStream {
                node_id: NODE_A,
                node_epoch: EPOCH,
            },
        )
        .unwrap();
        if i % open_every == 0 {
            let node_id = if (i / open_every).is_multiple_of(2) {
                NODE_A
            } else {
                NODE_B
            };
            apply(
                &mut state,
                &Command::OpenStream {
                    node_id,
                    node_epoch: EPOCH,
                    stream_id: i,
                    epoch: 1,
                },
            )
            .unwrap();
        }
    }
    state
}

fn run_gate(total: u64) {
    let open_every = 10;
    let opened = total / open_every;

    let started = Instant::now();
    let state = build(total, open_every);
    let build_elapsed = started.elapsed();
    println!("create {total} streams (+{opened} opens): {build_elapsed:?}");
    assert_eq!(state.streams.len() as u64, total);

    let started = Instant::now();
    let forks: Vec<State> = (0..1_000).map(|_| state.clone()).collect();
    let fork_elapsed = started.elapsed() / 1_000;
    println!("state fork (avg of 1000): {fork_elapsed:?}");
    drop(forks);
    assert!(
        fork_elapsed.as_micros() < 1_000,
        "fork must be O(1) (~ns), took {fork_elapsed:?} at {total} streams"
    );

    let started = Instant::now();
    for i in (0..total).step_by((total / 10_000).max(1) as usize) {
        assert!(state.get_stream(i).is_some());
    }
    println!("10k point lookups: {:?}", started.elapsed());

    let started = Instant::now();
    let node_b = state.get_opening_streams(NODE_B);
    let opening_elapsed = started.elapsed();
    println!(
        "get_opening_streams({} streams): {opening_elapsed:?}",
        node_b.len()
    );
    assert_eq!(node_b.len() as u64, opened / 2);

    let started = Instant::now();
    let mut fork = state.clone();
    apply(
        &mut fork,
        &Command::CreateStream {
            node_id: NODE_A,
            node_epoch: EPOCH,
        },
    )
    .unwrap();
    println!(
        "single apply on {total}-stream state: {:?}",
        started.elapsed()
    );
    assert_eq!(
        state.streams.len() as u64,
        total,
        "original untouched (persistent maps)"
    );

    let started = Instant::now();
    let encoded = mink_metadata::snapshot::encode(&state);
    let encode_elapsed = started.elapsed();
    let bytes_per_stream = encoded.len() as u64 / total;
    println!(
        "snapshot encode: {encode_elapsed:?}, {} bytes total, {bytes_per_stream} B/stream",
        encoded.len()
    );
    assert!(
        bytes_per_stream <= 64,
        "compact rows: expected <= 64 B/stream in the snapshot"
    );
    let started = Instant::now();
    let decoded = mink_metadata::snapshot::decode(&encoded).unwrap();
    println!(
        "snapshot decode (incl. index rebuild): {:?}",
        started.elapsed()
    );
    assert_eq!(decoded, state);
}

#[test]
fn hundred_k_streams_gate() {
    run_gate(100_000);
}

#[test]
#[ignore = "run explicitly: cargo test --release -p mink-metadata --test scale -- --ignored --nocapture"]
fn million_streams_gate() {
    run_gate(1_000_000);
}
