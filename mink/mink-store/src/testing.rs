use std::time::Duration;

use mink_core::{Granularity, IndexedFile, Offsets, Partition, Scope, Snapshot, Topic};
use uuid::Uuid;

use crate::{Lease, Store};

pub(crate) const TTL: Duration = Duration::from_secs(60);
pub(crate) const INDEXER: Uuid = Uuid::from_u128(1);
pub(crate) const WRITER: Uuid = Uuid::from_u128(2);
pub(crate) const NEXT_WRITER: Uuid = Uuid::from_u128(3);

pub(crate) async fn store() -> Option<Store> {
    let url = mink_conformance::service("TEST_POSTGRES_URL")?;
    let url = mink_conformance::fresh_schema(&url).await;

    Some(Store::connect(&url).await.unwrap())
}

pub(crate) async fn topic(store: &Store, partition_count: i32) -> Topic {
    let topic = Topic::new(
        format!("t{}", Uuid::new_v4().simple()),
        Uuid::new_v4(),
        partition_count,
    )
    .unwrap();

    store.topics().add(&topic, Granularity::None).await.unwrap();

    topic
}

pub(crate) async fn partition_lease(
    store: &Store,
    topic: &Topic,
    index: i32,
    holder: Uuid,
) -> Lease {
    let scope = Scope::Partition(Partition::new(topic.name(), index).unwrap());

    store
        .leases()
        .acquire(&scope, holder, TTL)
        .await
        .unwrap()
        .unwrap()
}

pub(crate) async fn topic_lease(store: &Store, topic: &Topic, holder: Uuid) -> Lease {
    let scope = Scope::Topic(topic.name().to_owned());

    store
        .leases()
        .acquire(&scope, holder, TTL)
        .await
        .unwrap()
        .unwrap()
}

pub(crate) async fn started(store: &Store, topic: &Topic) -> Vec<Lease> {
    let mut leases = Vec::new();

    for partition in topic.partitions() {
        let lease = partition_lease(store, topic, partition.index(), INDEXER).await;
        store.indexed_ranges().start(&lease, 0).await.unwrap();
        leases.push(lease);
    }

    leases
}

pub(crate) fn snapshot(lease: &Lease, topic: &Topic, id: i64, ranges: &[(i64, i64)]) -> Snapshot {
    let offsets = ranges
        .iter()
        .enumerate()
        .map(|(index, (start, end))| {
            let partition = Partition::new(topic.name(), index as i32).unwrap();
            Offsets::new(partition, *start, *end).unwrap()
        })
        .collect();

    Snapshot::new(id, id * 1000, lease.epoch(), offsets).unwrap()
}

pub(crate) fn file(topic: &Topic, partition: i32, start: i64, end: i64) -> IndexedFile {
    mink_conformance::file(topic.name(), partition, start, end)
}
