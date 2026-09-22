//! Plans bucket leadership moves that drain dead nodes and even out load across live ones.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use mink_metadata::State;
use mink_table::Bucket;

use crate::assign::{elect, leader_load, orphaned};

const THRESHOLD: f64 = 1.10;
const MARGIN: f64 = 0.9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Move {
    pub bucket: Bucket,
    pub from: i32,
    pub to: i32,
}

pub fn limits(leaders: usize, live_nodes: usize) -> (usize, usize) {
    let average = leaders as f64 / live_nodes as f64;
    let adjusted = (THRESHOLD - 1.0) * MARGIN;
    let upper = (average * (1.0 + adjusted)).ceil() as usize;
    let lower = (average * (1.0 - adjusted)).floor() as usize;

    (lower, upper)
}

pub fn plan(state: &State, live: &BTreeSet<i32>) -> Vec<Move> {
    if live.is_empty() {
        return Vec::new();
    }
    let mut leaders: BTreeMap<Bucket, i32> = state
        .catalog
        .buckets
        .iter()
        .map(|(bucket, row)| (*bucket, row.leader))
        .collect();
    let mut load = leader_load(state, live);
    let mut moves = Vec::new();

    for (bucket, from) in orphaned(state, live) {
        let to = least_loaded(&load);
        *load.get_mut(&to).expect("live") += 1;
        leaders.insert(bucket, to);
        moves.push(Move { bucket, from, to });
    }

    let (lower, upper) = limits(leaders.len(), live.len());
    loop {
        let from = most_loaded(&load);
        let to = least_loaded(&load);
        let (heavy, light) = (load[&from], load[&to]);
        if heavy <= upper && light >= lower || heavy - light <= 1 {
            break;
        }
        let bucket = leaders
            .iter()
            .find(|(_, leader)| **leader == from)
            .map(|(bucket, _)| *bucket)
            .expect("a loaded node leads a bucket");
        leaders.insert(bucket, to);
        *load.get_mut(&from).expect("live") -= 1;
        *load.get_mut(&to).expect("live") += 1;
        moves.push(Move { bucket, from, to });
    }

    moves
}

fn least_loaded(load: &BTreeMap<i32, usize>) -> i32 {
    elect(load).expect("live nodes")
}

fn most_loaded(load: &BTreeMap<i32, usize>) -> i32 {
    *load
        .iter()
        .max_by_key(|(n, c)| (**c, Reverse(**n)))
        .map(|(n, _)| n)
        .expect("live nodes")
}

#[cfg(test)]
mod tests {
    use mink_metadata::BucketRow;
    use mink_table::{BucketId, Id};

    use super::*;

    fn state(leaders: &[i32]) -> State {
        let mut state = State::default();
        for (i, leader) in leaders.iter().enumerate() {
            state.catalog.buckets.insert(
                Bucket::new(Id(1), BucketId(i as u32)),
                BucketRow {
                    stream_id: i as u64,
                    leader: *leader,
                    leader_epoch: 0,
                    coordinator_epoch: 0,
                },
            );
        }
        state
    }

    #[test]
    fn limits_are_fixed() {
        assert_eq!(limits(10, 4), (2, 3));
        assert_eq!(limits(0, 3), (0, 0));
    }

    #[test]
    fn balanced_clusters_need_no_moves() {
        let live = BTreeSet::from([1, 2, 3]);
        assert!(plan(&state(&[1, 2, 3, 1, 2, 3]), &live).is_empty());
        assert!(plan(&state(&[1, 2, 3, 1]), &live).is_empty());
    }

    #[test]
    fn dead_nodes_are_drained_first_then_the_rest_evens_out() {
        let live = BTreeSet::from([1, 2]);
        let moves = plan(&state(&[9, 9, 1, 1, 1, 1]), &live);
        let mut load = BTreeMap::from([(1, 4), (2, 0)]);
        for m in &moves {
            assert!(live.contains(&m.to));
            if live.contains(&m.from) {
                *load.get_mut(&m.from).unwrap() -= 1;
            }
            *load.get_mut(&m.to).unwrap() += 1;
        }
        assert_eq!(load, BTreeMap::from([(1, 4), (2, 2)]));
        assert_eq!(moves.len(), 2);
        assert!(moves.iter().all(|m| m.from == 9 && m.to == 2));
    }

    #[test]
    fn overloaded_nodes_give_to_underloaded_ones() {
        let live = BTreeSet::from([1, 2, 3]);
        let moves = plan(&state(&[1, 1, 1, 1, 1, 1, 1, 2, 3]), &live);
        let mut load = BTreeMap::from([(1, 7), (2, 1), (3, 1)]);
        for m in &moves {
            *load.get_mut(&m.from).unwrap() -= 1;
            *load.get_mut(&m.to).unwrap() += 1;
        }
        let (lower, upper) = limits(9, 3);
        assert!(
            load.values().all(|n| *n >= lower && *n <= upper),
            "{load:?}"
        );
        assert_eq!(load.values().sum::<usize>(), 9);
    }

    #[test]
    fn no_live_nodes_means_no_plan() {
        assert!(plan(&state(&[1, 1]), &BTreeSet::new()).is_empty());
    }
}
