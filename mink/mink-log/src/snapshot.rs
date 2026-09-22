//! Serializable snapshot of idempotent writer state and the stores that persist it per bucket.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use mink_table::Bucket;
use s3stream::{KVClient, KeyValue};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::writer::{BatchMetadata, State, Writers};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub writer_id: i64,
    pub last_batch_sequence: i32,
    pub last_batch_base_offset: i64,
    pub last_batch_offset_delta: i32,
    pub last_batch_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub offset: i64,
    pub writers: Vec<Entry>,
}

impl Snapshot {
    pub fn capture(manager: &Writers) -> Self {
        let mut writers: Vec<Entry> = manager
            .active_writers()
            .filter_map(|state| {
                state.last_batch().map(|batch| Entry {
                    writer_id: state.writer_id(),
                    last_batch_sequence: batch.batch_sequence,
                    last_batch_base_offset: batch.first_offset(),
                    last_batch_offset_delta: batch.offset_delta,
                    last_batch_timestamp: state.last_batch_timestamp(),
                })
            })
            .collect();
        writers.sort_by_key(|w| w.writer_id);

        Snapshot {
            offset: manager.map_end_offset(),
            writers,
        }
    }

    pub fn restore(&self, manager: &mut Writers, now_ms: i64) {
        for entry in &self.writers {
            if now_ms - entry.last_batch_timestamp > manager.expiration_ms() {
                continue;
            }
            manager.load(State::with_batch(
                entry.writer_id,
                entry.last_batch_timestamp,
                BatchMetadata {
                    batch_sequence: entry.last_batch_sequence,
                    last_offset: entry.last_batch_base_offset
                        + i64::from(entry.last_batch_offset_delta),
                    offset_delta: entry.last_batch_offset_delta,
                    timestamp: entry.last_batch_timestamp,
                },
            ));
        }

        manager.set_map_end_offset(self.offset);
    }

    pub fn to_bytes(&self) -> Result<Bytes, Error> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| Error::Snapshot(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|e| Error::Snapshot(e.to_string()))
    }
}

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    async fn load(&self, bucket: &Bucket) -> Result<Option<Snapshot>, Error>;

    async fn store(&self, bucket: &Bucket, snapshot: &Snapshot) -> Result<(), Error>;

    async fn remove(&self, bucket: &Bucket) -> Result<(), Error>;
}

pub struct Kv {
    kv: Arc<dyn KVClient>,
}

impl Kv {
    pub fn new(kv: Arc<dyn KVClient>) -> Self {
        Kv { kv }
    }

    fn key(bucket: &Bucket) -> String {
        let partition = bucket
            .partition()
            .map_or_else(|| "-".to_owned(), |p| p.to_string());
        format!(
            "mink/writers/{}/{}/{}",
            bucket.table(),
            partition,
            bucket.bucket()
        )
    }
}

#[async_trait]
impl SnapshotStore for Kv {
    async fn load(&self, bucket: &Bucket) -> Result<Option<Snapshot>, Error> {
        match self.kv.get_kv(&Self::key(bucket)).await? {
            Some(bytes) => Ok(Some(Snapshot::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn store(&self, bucket: &Bucket, snapshot: &Snapshot) -> Result<(), Error> {
        self.kv
            .put_kv(KeyValue {
                key: Self::key(bucket),
                value: snapshot.to_bytes()?,
            })
            .await?;

        Ok(())
    }

    async fn remove(&self, bucket: &Bucket) -> Result<(), Error> {
        self.kv.del_kv(&Self::key(bucket)).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mink_table::{BucketId, Id, PartitionId};
    use s3stream::MemoryKvClient;

    use super::*;

    fn manager_with_two_writers() -> Writers {
        let mut manager = Writers::new(1_000);
        manager.load(State::with_batch(
            2,
            500,
            BatchMetadata {
                batch_sequence: 4,
                last_offset: 19,
                offset_delta: 9,
                timestamp: 500,
            },
        ));
        manager.load(State::with_batch(
            1,
            100,
            BatchMetadata {
                batch_sequence: 0,
                last_offset: 3,
                offset_delta: 3,
                timestamp: 100,
            },
        ));
        manager.set_map_end_offset(20);
        manager
    }

    #[test]
    fn capture_is_sorted_and_keeps_last_batch_only() {
        let snapshot = Snapshot::capture(&manager_with_two_writers());
        assert_eq!(snapshot.offset, 20);
        assert_eq!(
            snapshot.writers,
            vec![
                Entry {
                    writer_id: 1,
                    last_batch_sequence: 0,
                    last_batch_base_offset: 0,
                    last_batch_offset_delta: 3,
                    last_batch_timestamp: 100,
                },
                Entry {
                    writer_id: 2,
                    last_batch_sequence: 4,
                    last_batch_base_offset: 10,
                    last_batch_offset_delta: 9,
                    last_batch_timestamp: 500,
                },
            ]
        );
    }

    #[test]
    fn restore_round_trips_and_drops_expired() {
        let snapshot = Snapshot::capture(&manager_with_two_writers());
        let bytes = snapshot.to_bytes().unwrap();
        let parsed = Snapshot::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, snapshot);

        let mut restored = Writers::new(1_000);
        parsed.restore(&mut restored, 1_200);
        assert_eq!(restored.map_end_offset(), 20);
        assert!(restored.last_entry(1).is_none());
        let two = restored.last_entry(2).unwrap();
        assert_eq!(two.last_batch_sequence(), 4);
        assert_eq!(two.last_data_offset(), 19);
        assert_eq!(two.last_batch_timestamp(), 500);
        assert_eq!(Snapshot::capture(&restored).writers, snapshot.writers[1..]);
    }

    #[tokio::test]
    async fn kv_store_round_trips_per_bucket() {
        let store = Kv::new(MemoryKvClient::new());
        let plain = Bucket::new(Id(7), BucketId(0));
        let partitioned = Bucket::partitioned(Id(7), PartitionId(3), BucketId(0));
        let snapshot = Snapshot::capture(&manager_with_two_writers());

        assert!(store.load(&plain).await.unwrap().is_none());
        store.store(&plain, &snapshot).await.unwrap();
        assert_eq!(store.load(&plain).await.unwrap(), Some(snapshot.clone()));
        assert!(store.load(&partitioned).await.unwrap().is_none());

        store.remove(&plain).await.unwrap();
        assert!(store.load(&plain).await.unwrap().is_none());
        assert_eq!(Kv::key(&partitioned), "mink/writers/7/3/0");
        assert_eq!(Kv::key(&plain), "mink/writers/7/-/0");
    }
}
