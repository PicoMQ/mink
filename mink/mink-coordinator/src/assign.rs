//! Round-robin bucket assignment, leader load counting, least-loaded election and orphan detection.

use std::collections::{BTreeMap, BTreeSet};

use mink_metadata::State;
use mink_table::{Bucket, BucketId};

pub fn assign(bucket_count: u32, nodes: &[i32], start: usize) -> BTreeMap<BucketId, i32> {
    assert!(!nodes.is_empty(), "assigning buckets to no nodes");
    (0..bucket_count)
        .map(|i| {
            let node = nodes[(i as usize + start) % nodes.len()];
            (BucketId(i), node)
        })
        .collect()
}

pub fn leader_load(state: &State, live: &BTreeSet<i32>) -> BTreeMap<i32, usize> {
    let mut load: BTreeMap<i32, usize> = live.iter().map(|node| (*node, 0)).collect();
    for row in state.catalog.buckets.values() {
        if let Some(count) = load.get_mut(&row.leader) {
            *count += 1;
        }
    }

    load
}

pub fn elect(load: &BTreeMap<i32, usize>) -> Option<i32> {
    load.iter().min_by_key(|(n, c)| (**c, **n)).map(|(n, _)| *n)
}

pub fn orphaned(state: &State, live: &BTreeSet<i32>) -> Vec<(Bucket, i32)> {
    state
        .catalog
        .buckets
        .iter()
        .filter(|(_, row)| !live.contains(&row.leader))
        .map(|(bucket, row)| (*bucket, row.leader))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_from_the_start_index() {
        let assigned = assign(5, &[10, 20, 30], 1);
        let leaders: Vec<i32> = assigned.values().copied().collect();
        assert_eq!(leaders, vec![20, 30, 10, 20, 30]);
        assert_eq!(assigned.keys().last(), Some(&BucketId(4)));
    }

    #[test]
    fn election_prefers_the_least_loaded_then_lowest_id() {
        let load = BTreeMap::from([(3, 2), (1, 1), (2, 1)]);
        assert_eq!(elect(&load), Some(1));
        assert_eq!(elect(&BTreeMap::new()), None);
    }
}
