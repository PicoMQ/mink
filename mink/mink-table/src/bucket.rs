//! Identity of a bucket within a table or partition, and the bounds on bucket counts.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{Error, Id, PartitionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BucketId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Bucket {
    table: Id,
    partition: Option<PartitionId>,
    bucket: BucketId,
}

impl BucketId {
    pub(crate) fn check_count(count: u32) -> Result<(), Error> {
        if count == 0 || count > i32::MAX as u32 {
            return Err(Error::BucketCount);
        }

        Ok(())
    }
}

impl Bucket {
    pub fn new(table: Id, bucket: BucketId) -> Self {
        Bucket {
            table,
            partition: None,
            bucket,
        }
    }

    pub fn partitioned(table: Id, partition: PartitionId, bucket: BucketId) -> Self {
        Bucket {
            table,
            partition: Some(partition),
            bucket,
        }
    }

    pub fn of(table: Id, partition: Option<PartitionId>, bucket: BucketId) -> Self {
        Bucket {
            table,
            partition,
            bucket,
        }
    }

    pub fn table(&self) -> Id {
        self.table
    }

    pub fn partition(&self) -> Option<PartitionId> {
        self.partition
    }

    pub fn bucket(&self) -> BucketId {
        self.bucket
    }
}

impl fmt::Display for BucketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.partition {
            Some(partition) => write!(f, "{}/{}/{}", self.table, partition, self.bucket),
            None => write!(f, "{}/{}", self.table, self.bucket),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_distinguishes_partitioned_buckets_by_segment_count() {
        let flat = Bucket::new(Id(7), BucketId(3));
        let partitioned = Bucket::partitioned(Id(7), PartitionId(11), BucketId(3));
        assert_eq!(flat.to_string(), "7/3");
        assert_eq!(partitioned.to_string(), "7/11/3");
        assert_ne!(flat, partitioned);
    }

    #[test]
    fn orders_by_table_then_partition_then_bucket() {
        let mut buckets = vec![
            Bucket::partitioned(Id(1), PartitionId(2), BucketId(0)),
            Bucket::new(Id(1), BucketId(1)),
            Bucket::new(Id(0), BucketId(9)),
            Bucket::new(Id(1), BucketId(0)),
        ];
        buckets.sort();
        assert_eq!(
            buckets,
            [
                Bucket::new(Id(0), BucketId(9)),
                Bucket::new(Id(1), BucketId(0)),
                Bucket::new(Id(1), BucketId(1)),
                Bucket::partitioned(Id(1), PartitionId(2), BucketId(0)),
            ]
        );
    }
}
