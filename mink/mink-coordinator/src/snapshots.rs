//! The snapshot cleaner interface and the selection of snapshots beyond the retention count.

use std::collections::BTreeMap;

use async_trait::async_trait;
use mink_metadata::{KvSnapshotRow, State};
use mink_table::Bucket;

use crate::Error;

#[async_trait]
pub trait SnapshotCleaner: Send + Sync {
    async fn discard(
        &self,
        bucket: Bucket,
        snapshot: &KvSnapshotRow,
        retained: &[KvSnapshotRow],
    ) -> Result<(), Error>;
}

pub struct NoopCleaner;

#[async_trait]
impl SnapshotCleaner for NoopCleaner {
    async fn discard(
        &self,
        _bucket: Bucket,
        _snapshot: &KvSnapshotRow,
        _retained: &[KvSnapshotRow],
    ) -> Result<(), Error> {
        Ok(())
    }
}

pub fn excess(
    state: &State,
    retained: usize,
) -> BTreeMap<Bucket, (Vec<KvSnapshotRow>, Vec<KvSnapshotRow>)> {
    let mut by_bucket: BTreeMap<Bucket, Vec<KvSnapshotRow>> = BTreeMap::new();
    for ((bucket, _), row) in state.catalog.kv_snapshots.iter() {
        by_bucket.entry(*bucket).or_default().push(row.clone());
    }

    by_bucket
        .into_iter()
        .filter(|(_, rows)| rows.len() > retained.max(1))
        .map(|(bucket, mut rows)| {
            rows.sort_by_key(|row| row.snapshot_id);
            let keep = rows.split_off(rows.len() - retained.max(1));
            (bucket, (rows, keep))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use mink_table::{BucketId, Id};

    use super::*;

    fn row(id: u64) -> KvSnapshotRow {
        KvSnapshotRow {
            snapshot_id: id,
            log_offset: id as i64 * 10,
            row_count: 1,
            path: format!("snap-{id}"),
        }
    }

    #[test]
    fn keeps_the_newest_and_lists_the_rest_oldest_first() {
        let bucket = Bucket::new(Id(1), BucketId(0));
        let other = Bucket::new(Id(1), BucketId(1));
        let mut state = State::default();
        for id in [3, 1, 2, 4] {
            state.catalog.kv_snapshots.insert((bucket, id), row(id));
        }
        state.catalog.kv_snapshots.insert((other, 7), row(7));
        let excess = excess(&state, 2);
        assert_eq!(excess.len(), 1);
        let (drop, keep) = &excess[&bucket];
        assert_eq!(drop, &[row(1), row(2)]);
        assert_eq!(keep, &[row(3), row(4)]);
        let (drop, keep) = &super::excess(&state, 0)[&bucket];
        assert_eq!(drop.len(), 3);
        assert_eq!(keep, &[row(4)]);
    }
}
