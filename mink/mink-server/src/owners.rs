use mink_core::Scope;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn owner(nodes: &[Uuid], key: &str) -> Option<Uuid> {
    nodes
        .iter()
        .max_by_key(|node| preference(**node, key))
        .copied()
}

pub(crate) fn preference(node: Uuid, key: &str) -> u64 {
    let digest = Sha256::digest(format!("{node}:{key}").as_bytes());

    u64::from_be_bytes(digest[..8].try_into().expect("a digest has 8 bytes"))
}

pub(crate) fn share(leases: usize, nodes: usize) -> usize {
    leases.div_ceil(nodes)
}

pub(crate) fn scope_key(scope: &Scope) -> String {
    match scope {
        Scope::Partition(partition) => format!("{}/{}", partition.topic(), partition.index()),
        Scope::Topic(topic) => topic.clone(),
    }
}

#[cfg(test)]
mod tests {
    use mink_core::Partition;

    use super::*;

    #[test]
    fn shares_round_up_over_the_nodes_indexing_a_topic() {
        assert_eq!(share(4, 2), 2);
        assert_eq!(share(5, 2), 3);
        assert_eq!(share(4, 3), 2);
        assert_eq!(share(0, 3), 0);
    }

    #[test]
    fn preferences_hash_the_node_and_scope_and_spread_the_leases() {
        let nodes: Vec<Uuid> = (1..=3).map(Uuid::from_u128).collect();
        let scopes: Vec<String> = (0..3000)
            .map(|index| scope_key(&Scope::Partition(Partition::new("orders", index).unwrap())))
            .collect();

        let most_preferred: Vec<usize> = nodes
            .iter()
            .map(|node| {
                scopes
                    .iter()
                    .filter(|scope| owner(&nodes, scope) == Some(*node))
                    .count()
            })
            .collect();

        assert_eq!(preference(nodes[0], "orders/0"), 18010754674424339750);
        assert_eq!(scope_key(&Scope::Topic("orders".to_owned())), "orders");
        assert_eq!(scopes[3], "orders/3");
        assert!(
            most_preferred
                .iter()
                .all(|count| (800..1200).contains(count)),
            "{most_preferred:?}"
        );
    }

    #[test]
    fn a_joining_node_takes_only_the_files_it_now_owns() {
        let three: Vec<Uuid> = (1..=3).map(Uuid::from_u128).collect();
        let four: Vec<Uuid> = (1..=4).map(Uuid::from_u128).collect();
        let paths: Vec<String> = (0..4000)
            .map(|file| format!("s3://mink/t/data/p=0/{file}"))
            .collect();

        let moved = paths
            .iter()
            .filter(|path| owner(&three, path) != owner(&four, path))
            .count();
        let to_new = paths
            .iter()
            .filter(|path| owner(&four, path) == Some(Uuid::from_u128(4)))
            .count();

        assert_eq!(moved, to_new);
        assert!((800..1200).contains(&moved), "{moved}");
    }
}
