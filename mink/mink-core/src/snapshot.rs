use std::collections::HashMap;

use crate::Error;
use crate::file::File;
use crate::offsets::Offsets;
use crate::topic::Partition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    id: i64,
    timestamp_ms: i64,
    epoch: i64,
    offsets: Vec<Offsets>,
}

impl Snapshot {
    pub fn new(
        id: i64,
        timestamp_ms: i64,
        epoch: i64,
        mut offsets: Vec<Offsets>,
    ) -> Result<Snapshot, Error> {
        if id < 1 {
            return Err(Error::SnapshotId(id));
        }

        if epoch < 1 {
            return Err(Error::SnapshotEpoch(epoch));
        }

        offsets.sort_by_key(|offsets| offsets.partition().index());

        let Some(first) = offsets.first() else {
            return Err(Error::EmptySnapshot);
        };
        let one_topic = offsets
            .iter()
            .all(|offsets| offsets.partition().topic() == first.partition().topic());
        let distinct = offsets
            .windows(2)
            .all(|pair| pair[0].partition().index() != pair[1].partition().index());
        if !one_topic || !distinct {
            return Err(Error::SnapshotPartitions);
        }

        Ok(Snapshot {
            id,
            timestamp_ms,
            epoch,
            offsets,
        })
    }

    pub fn topic(&self) -> &str {
        self.offsets[0].partition().topic()
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    pub fn key(&self) -> String {
        format!("{}-e{}", self.id, self.epoch)
    }

    pub fn offsets(&self) -> &[Offsets] {
        &self.offsets
    }

    pub fn offsets_of(&self, partition: &Partition) -> Option<&Offsets> {
        self.offsets
            .iter()
            .find(|offsets| offsets.partition() == partition)
    }

    pub fn covers(&self, file: &File) -> bool {
        self.offsets_of(file.partition())
            .is_some_and(|offsets| offsets.covers(file))
    }

    pub fn next_id(latest_id: Option<i64>) -> i64 {
        latest_id.map_or(1, |id| id + 1)
    }

    pub fn expired_through(
        snapshots: &[Snapshot],
        before_ms: i64,
        earliest: &HashMap<Partition, i64>,
    ) -> Option<i64> {
        let (_, older) = snapshots.split_last()?;
        let expired = |snapshot: &Snapshot| {
            snapshot.timestamp_ms < before_ms
                || snapshot.offsets.iter().any(|offsets| {
                    earliest
                        .get(offsets.partition())
                        .is_some_and(|earliest| offsets.start_offset() < *earliest)
                })
        };

        older
            .iter()
            .filter(|snapshot| expired(snapshot))
            .map(|snapshot| snapshot.id)
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TimePartition;

    fn offsets(topic: &str, index: i32) -> Offsets {
        Offsets::new(Partition::new(topic, index).unwrap(), 0, 10).unwrap()
    }

    fn snapshot(id: i64, timestamp_ms: i64, start_offset: i64) -> Snapshot {
        let partition = Partition::new("orders", 0).unwrap();
        let offsets = Offsets::new(partition, start_offset, 100).unwrap();

        Snapshot::new(id, timestamp_ms, 1, vec![offsets]).unwrap()
    }

    #[test]
    fn a_snapshot_takes_its_topic_from_its_offsets_in_partition_order() {
        let snapshot =
            Snapshot::new(1, 0, 1, vec![offsets("orders", 1), offsets("orders", 0)]).unwrap();

        let indexes: Vec<_> = snapshot
            .offsets()
            .iter()
            .map(|offsets| offsets.partition().index())
            .collect();

        assert_eq!(snapshot.topic(), "orders");
        assert_eq!(indexes, [0, 1]);
    }

    #[test]
    fn snapshots_are_rejected_when_empty_mixed_repeated_or_not_positive() {
        assert_eq!(Snapshot::new(1, 0, 1, vec![]), Err(Error::EmptySnapshot));
        assert_eq!(
            Snapshot::new(1, 0, 1, vec![offsets("orders", 0), offsets("users", 1)]),
            Err(Error::SnapshotPartitions)
        );
        assert_eq!(
            Snapshot::new(1, 0, 1, vec![offsets("orders", 0), offsets("orders", 0)]),
            Err(Error::SnapshotPartitions)
        );
        assert_eq!(
            Snapshot::new(0, 0, 1, vec![offsets("orders", 0)]),
            Err(Error::SnapshotId(0))
        );
        assert_eq!(
            Snapshot::new(1, 0, 0, vec![offsets("orders", 0)]),
            Err(Error::SnapshotEpoch(0))
        );
    }

    #[test]
    fn snapshots_expire_by_age_or_by_offsets_kafka_no_longer_has() {
        let snapshots = [
            snapshot(1, 10, 0),
            snapshot(2, 20, 0),
            snapshot(3, 30, 5),
            snapshot(4, 40, 5),
        ];
        let earliest = HashMap::from([(Partition::new("orders", 0).unwrap(), 5)]);

        let by_age = Snapshot::expired_through(&snapshots, 25, &HashMap::new());
        let by_offsets = Snapshot::expired_through(&snapshots, 0, &earliest);
        let neither = Snapshot::expired_through(&snapshots, 0, &HashMap::new());

        assert_eq!(by_age, Some(2));
        assert_eq!(by_offsets, Some(2));
        assert_eq!(neither, None);
    }

    #[test]
    fn the_latest_snapshot_never_expires() {
        let snapshots = [snapshot(1, 10, 0), snapshot(2, 20, 0)];

        let expired = Snapshot::expired_through(&snapshots, i64::MAX, &HashMap::new());
        let alone = Snapshot::expired_through(&snapshots[1..], i64::MAX, &HashMap::new());

        assert_eq!(expired, Some(1));
        assert_eq!(alone, None);
    }

    #[test]
    fn a_snapshot_covers_exactly_the_files_inside_its_offsets() {
        let partition = |index| Partition::new("orders", index).unwrap();
        let file = |index, start, end| {
            File::v1(partition(index), start, end, TimePartition::new(0, None), 0).unwrap()
        };
        let snapshot =
            Snapshot::new(1, 0, 1, vec![Offsets::new(partition(0), 10, 20).unwrap()]).unwrap();

        assert!(snapshot.covers(&file(0, 10, 20)));
        assert!(snapshot.covers(&file(0, 12, 15)));
        assert!(!snapshot.covers(&file(0, 5, 15)));
        assert!(!snapshot.covers(&file(0, 15, 25)));
        assert!(!snapshot.covers(&file(1, 10, 20)));
    }

    #[test]
    fn snapshot_ids_start_at_one_and_follow_the_latest() {
        assert_eq!(Snapshot::next_id(None), 1);
        assert_eq!(Snapshot::next_id(Some(7)), 8);
    }
}
