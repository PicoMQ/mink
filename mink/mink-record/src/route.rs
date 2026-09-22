//! Splits an Arrow batch into per-partition, per-bucket row groups and extracts the rows of a group.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_array::{RecordBatch, UInt32Array};
use mink_table::{BucketId, Bucketing, Descriptor, PartitionName};
use mink_types::Fields;

use crate::{Error, KeyEncoder, PartitionGetter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    pub partition: Option<PartitionName>,
    pub bucket: BucketId,
    pub rows: Vec<u32>,
}

pub struct Router {
    partitions: Option<PartitionGetter>,
    keys: Option<KeyEncoder>,
    bucketing: Bucketing,
    bucket_count: u32,
    sticky: AtomicU32,
}

impl Router {
    pub fn new(fields: &Fields, descriptor: &Descriptor) -> Result<Self, Error> {
        let partitions = descriptor
            .is_partitioned()
            .then(|| PartitionGetter::new(fields, descriptor.partition_keys()))
            .transpose()?;
        let bucketing = descriptor.bucketing();
        let keys = (!descriptor.bucket_keys().is_empty())
            .then(|| KeyEncoder::new(fields, descriptor.bucket_keys(), bucketing))
            .transpose()?;
        let bucket_count = descriptor.bucket_count().ok_or(Error::NoBucketCount)?;

        Ok(Router {
            partitions,
            keys,
            bucketing,
            bucket_count,
            sticky: AtomicU32::new(0),
        })
    }

    pub fn split(&self, batch: &RecordBatch) -> Result<Vec<Group>, Error> {
        let partitions = self
            .partitions
            .as_ref()
            .map(|getter| getter.bind(batch))
            .transpose()?;
        let keys = self
            .keys
            .as_ref()
            .map(|encoder| encoder.bind(batch))
            .transpose()?;

        let mut sticky = None;
        let mut groups: BTreeMap<(Option<PartitionName>, BucketId), Vec<u32>> = BTreeMap::new();
        let mut key = Vec::new();
        for row in 0..batch.num_rows() {
            let partition = partitions.as_ref().map(|p| p.name(row)).transpose()?;
            let bucket = match &keys {
                Some(keys) => {
                    keys.encode(row, &mut key)?;
                    self.bucketing
                        .bucket(&key, self.bucket_count)
                        .map_err(|e| Error::Bucket(e.to_string()))?
                }
                None => *sticky.get_or_insert_with(|| {
                    BucketId(self.sticky.fetch_add(1, Ordering::Relaxed) % self.bucket_count)
                }),
            };
            groups
                .entry((partition, bucket))
                .or_default()
                .push(row as u32);
        }

        Ok(groups
            .into_iter()
            .map(|((partition, bucket), rows)| Group {
                partition,
                bucket,
                rows,
            })
            .collect())
    }
}

pub fn take(
    batch: &RecordBatch,
    changes: Option<&[u8]>,
    group: &Group,
) -> Result<(RecordBatch, Option<Vec<u8>>), Error> {
    let indices = UInt32Array::from(group.rows.clone());
    let rows = arrow_select::take::take_record_batch(batch, &indices)
        .map_err(|e| Error::Ipc(e.to_string()))?;
    let changes = changes.map(|all| group.rows.iter().map(|&i| all[i as usize]).collect());

    Ok((rows, changes))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use mink_table::{Column, PrimaryKey, Schema};
    use mink_types::DataType;

    use super::*;

    fn schema(primary_key: bool) -> Schema {
        let mut builder = Schema::builder()
            .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
            .column(Column::new("region", DataType::string().with_nullable(!primary_key)).unwrap())
            .column(Column::new("v", DataType::string()).unwrap());
        if primary_key {
            builder =
                builder.primary_key(PrimaryKey::new(vec!["k".into(), "region".into()]).unwrap());
        }
        builder.build().unwrap()
    }

    fn batch(rows: &[(i64, &str, &str)]) -> RecordBatch {
        let s = schema(false);
        RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::from(s.fields())),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn descriptor(primary_key: bool, partitioned: bool, buckets: u32) -> Descriptor {
        let mut builder = Descriptor::builder(schema(primary_key)).bucket_count(buckets);
        if partitioned {
            builder = builder.partitioned_by(["region"]);
        }
        builder.build().unwrap()
    }

    #[test]
    fn keyed_rows_hash_to_buckets_and_split_by_partition() {
        let descriptor = descriptor(true, true, 4);
        let router = Router::new(descriptor.schema().fields(), &descriptor).unwrap();
        let rows = batch(&[
            (1, "us", "a"),
            (2, "eu", "b"),
            (1, "us", "c"),
            (3, "us", "d"),
            (2, "us", "e"),
        ]);
        let groups = router.split(&rows).unwrap();

        let of = |k: i64, region: &str| {
            groups
                .iter()
                .find(|g| {
                    g.partition.as_ref().unwrap().as_str() == region
                        && g.rows.iter().any(|&r| {
                            rows.column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(r as usize)
                                == k
                        })
                })
                .unwrap()
        };
        assert_eq!(
            of(1, "us")
                .rows
                .iter()
                .filter(|&&r| r == 0 || r == 2)
                .count(),
            2
        );
        assert!(groups.iter().all(|g| g.bucket.0 < 4));
        let all: Vec<u32> = groups.iter().flat_map(|g| g.rows.clone()).collect();
        let mut sorted = all.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2, 3, 4]);
        assert!(groups.iter().all(|g| {
            let same_region = |r: &u32| {
                rows.column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(*r as usize)
                    == g.partition.as_ref().unwrap().as_str()
            };
            g.rows.iter().all(same_region)
        }));
        let again = router.split(&rows).unwrap();
        assert_eq!(again, groups);

        let (sub, changes) = take(&rows, Some(&[0, 1, 2, 3, 4]), of(1, "us")).unwrap();
        assert_eq!(sub.num_rows(), 2);
        assert_eq!(changes.unwrap(), vec![0, 2]);
    }

    #[test]
    fn keyless_batches_stick_to_one_bucket_and_rotate() {
        let descriptor = descriptor(false, false, 3);
        let router = Router::new(descriptor.schema().fields(), &descriptor).unwrap();
        let rows = batch(&[(1, "us", "a"), (2, "eu", "b")]);
        let buckets: Vec<u32> = (0..4)
            .map(|_| {
                let groups = router.split(&rows).unwrap();
                assert_eq!(groups.len(), 1);
                assert_eq!(groups[0].partition, None);
                assert_eq!(groups[0].rows, vec![0, 1]);
                groups[0].bucket.0
            })
            .collect();
        assert_eq!(buckets, vec![0, 1, 2, 0]);
    }

    #[test]
    fn null_partition_values_are_rejected() {
        let descriptor = descriptor(false, true, 2);
        let router = Router::new(descriptor.schema().fields(), &descriptor).unwrap();
        let s = schema(false);
        let rows = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::from(s.fields())),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        assert!(matches!(
            router.split(&rows).unwrap_err(),
            Error::NullKey(c) if c == "region"
        ));
    }
}
