//! Resolves the --bucket and --partition flags of a table command to concrete buckets,
//! and renders a bucket as text.

use anyhow::{Context, bail};
use mink_client::Admin;
use mink_client::proto::TableInfo;
use mink_table::{Bucket, BucketId, PartitionName};

pub fn text(bucket: Bucket) -> String {
    match bucket.partition() {
        Some(p) => format!("p{}/b{}", p.0, bucket.bucket().0),
        None => format!("b{}", bucket.bucket().0),
    }
}

pub async fn resolve(
    admin: &Admin,
    info: &TableInfo,
    bucket: Option<u32>,
    partition: Option<&PartitionName>,
) -> anyhow::Result<Vec<Bucket>> {
    let partition_id = match partition {
        Some(name) => {
            if !info.descriptor.is_partitioned() {
                bail!("{} is not partitioned", info.path);
            }

            Some(
                admin
                    .list_partitions(&info.path)
                    .await?
                    .iter()
                    .find(|p| &p.name == name)
                    .map(|p| p.partition_id)
                    .with_context(|| format!("no partition {name}"))?,
            )
        }
        None => None,
    };

    let selected: Vec<Bucket> = info
        .buckets
        .iter()
        .map(|b| b.bucket)
        .filter(|b| partition_id.is_none_or(|p| b.partition() == Some(p)))
        .filter(|b| bucket.is_none_or(|id| b.bucket() == BucketId(id)))
        .collect();
    if selected.is_empty() {
        bail!("no such bucket");
    }

    Ok(selected)
}
