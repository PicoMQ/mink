//! A Mink table as a DataFusion table: its Arrow schema, which filters it takes, and the pinned splits
//! a scan runs over: key lookups when the primary key is pinned, otherwise the buckets the pinned
//! partition and bucket keys route to, planned against the lake once per query.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use mink_client::{Cluster, Lookup, Table, proto};
use mink_lake::{LakeSplit, Predicate, Reader};
use mink_read::{LakePosition, Options, Read};
use mink_record::{PartitionGetter, Router};
use mink_table::{Bucket, BucketId, Name, PartitionId, PartitionName, PartitionSpec};
use mink_types::Fields;

use crate::error::Error;
use crate::exec::{Scan, Split};
use crate::keys::Constraints;
use crate::log::Log;
use crate::predicate;

/// The most key combinations a query pins before it falls back to scanning.
const KEY_CAP: usize = 1024;

pub struct MinkTable {
    table: Table,
    schema: SchemaRef,
    read: Arc<Read>,
    lake: bool,
    partitions: BTreeMap<PartitionId, PartitionName>,
    lookup: Option<Arc<Lookup>>,
    keys: BTreeSet<String>,
}

impl MinkTable {
    pub async fn open(
        cluster: &Cluster,
        lake: Option<Arc<dyn Reader>>,
        path: &mink_table::Path,
    ) -> Result<Self, Error> {
        let admin = cluster.admin();
        admin.get_table(path).await?;
        let table = cluster.table(path).await?;
        let descriptor = table.descriptor();
        let partitions = if descriptor.partition_keys().is_empty() {
            BTreeMap::new()
        } else {
            admin
                .list_partitions(path)
                .await?
                .into_iter()
                .map(|p| (p.partition_id, p.name))
                .collect()
        };
        let read = Arc::new(Read::new(lake.clone(), Arc::new(Log::new(table.clone()))));
        let lookup = table.lookuper().ok().map(Arc::new);
        let keys = descriptor
            .partition_keys()
            .iter()
            .chain(descriptor.bucket_keys())
            .map(String::as_str)
            .chain(lookup.iter().flat_map(|l| l.key_columns()))
            .map(str::to_owned)
            .collect();

        Ok(MinkTable {
            schema: table.arrow_schema(),
            lake: lake.is_some() && descriptor.options().lake.is_some(),
            table,
            read,
            partitions,
            lookup,
            keys,
        })
    }

    fn fields(&self, columns: &[String]) -> Result<Fields, Error> {
        let fields = self.table.schema().fields();
        let indices: Vec<usize> = columns
            .iter()
            .map(|name| {
                fields
                    .index_of(name)
                    .ok_or_else(|| mink_record::Error::UnknownColumn(name.clone()))
            })
            .collect::<Result<_, _>>()?;

        Ok(fields.project(&indices)?)
    }

    fn partition(&self, bucket: Bucket) -> Option<PartitionName> {
        bucket
            .partition()
            .and_then(|id| self.partitions.get(&id).cloned())
    }

    fn buckets(
        &self,
        constraints: &Constraints,
    ) -> Result<Vec<(Bucket, Option<PartitionName>)>, Error> {
        let descriptor = self.table.descriptor();
        let partition_keys = descriptor.partition_keys();
        let all = self.table.buckets().map(|b| (b, self.partition(b)));
        let routed: Vec<String> = partition_keys
            .iter()
            .chain(descriptor.bucket_keys())
            .cloned()
            .collect();
        if !descriptor.bucket_keys().is_empty()
            && let Some(rows) = constraints.rows(&routed, &self.schema, KEY_CAP)?
        {
            let router = Router::new(&self.fields(&routed)?, descriptor)?;
            let kept: HashSet<(Option<PartitionName>, BucketId)> = router
                .split(&rows)?
                .into_iter()
                .map(|group| (group.partition, group.bucket))
                .collect();

            return Ok(all
                .filter(|(bucket, partition)| kept.contains(&(partition.clone(), bucket.bucket())))
                .collect());
        }
        if !constraints.pins_any(partition_keys) {
            return Ok(all.collect());
        }

        let mut pinned: Vec<(&String, HashSet<Name>)> = Vec::new();
        for key in partition_keys {
            let column = std::slice::from_ref(key);
            let Some(rows) = constraints.rows(column, &self.schema, usize::MAX)? else {
                continue;
            };
            let getter = PartitionGetter::new(&self.fields(column)?, column)?;
            let bound = getter.bind(&rows)?;
            let names = (0..rows.num_rows())
                .filter_map(|row| {
                    bound
                        .spec(row)
                        .map(|spec| spec.value(key).cloned())
                        .transpose()
                })
                .collect::<Result<HashSet<Name>, mink_record::Error>>()?;
            pinned.push((key, names));
        }
        let mut kept = Vec::new();
        for (bucket, partition) in all {
            let Some(name) = &partition else {
                continue;
            };
            let spec =
                PartitionSpec::from_name(partition_keys, name).map_err(mink_client::Error::from)?;
            if pinned
                .iter()
                .all(|(key, names)| spec.value(key).is_some_and(|value| names.contains(value)))
            {
                kept.push((bucket, partition));
            }
        }

        Ok(kept)
    }

    fn lake_filter(&self, filters: &[Expr]) -> Option<Predicate> {
        if !self.lake {
            return None;
        }

        Predicate::all(
            filters
                .iter()
                .filter_map(|filter| predicate::convert(filter, &self.schema)),
        )
    }

    async fn splits(
        &self,
        filters: &[Expr],
        filter: Option<&Predicate>,
    ) -> Result<Vec<Split>, Error> {
        let constraints = Constraints::new(filters, &self.schema);
        if let Some(lookup) = &self.lookup {
            let columns: Vec<String> = lookup.key_columns().map(str::to_owned).collect();
            if let Some(keys) = constraints.rows(&columns, &self.schema, KEY_CAP)? {
                return Ok(vec![Split::Lookup {
                    lookup: lookup.clone(),
                    keys,
                }]);
            }
        }
        let buckets = self.buckets(&constraints)?;
        let snapshot = if self.lake {
            self.table
                .cluster()
                .admin()
                .lake_snapshot(self.table.path())
                .await?
        } else {
            None
        };
        let descriptor = self.table.descriptor();
        match snapshot {
            Some(snapshot) => {
                let planned = self
                    .read
                    .plan_lake(self.table.path(), snapshot.snapshot_id, filter)
                    .await?;
                if descriptor.bucket_keys().is_empty() {
                    self.keyless(buckets, snapshot, planned).await
                } else {
                    self.bucketed(buckets, snapshot, planned).await
                }
            }
            None if descriptor.schema().primary_key().is_some() => Ok(buckets
                .into_iter()
                .map(|(bucket, _)| Split::Snapshot(bucket))
                .collect()),
            None => self.union(buckets, |_| None, |_| None).await,
        }
    }

    async fn union(
        &self,
        buckets: Vec<(Bucket, Option<PartitionName>)>,
        position: impl Fn(Bucket) -> Option<LakePosition>,
        mut split: impl FnMut(Bucket) -> Option<LakeSplit>,
    ) -> Result<Vec<Split>, Error> {
        let plans = buckets.into_iter().map(|(bucket, partition)| {
            self.read
                .plan(bucket, partition, position(bucket), split(bucket))
        });
        let plans = futures::future::try_join_all(plans).await?;

        Ok(plans.into_iter().map(Split::Union).collect())
    }

    async fn bucketed(
        &self,
        buckets: Vec<(Bucket, Option<PartitionName>)>,
        snapshot: proto::LakeSnapshot,
        planned: Vec<LakeSplit>,
    ) -> Result<Vec<Split>, Error> {
        let ends: HashMap<Bucket, i64> = snapshot.bucket_log_end_offset.into_iter().collect();
        let mut by_bucket: HashMap<(Option<PartitionName>, BucketId), LakeSplit> = planned
            .into_iter()
            .filter_map(|split| Some(((split.partition.clone(), split.bucket?), split)))
            .collect();
        let partitions: HashMap<Bucket, Option<PartitionName>> = buckets.iter().cloned().collect();

        self.union(
            buckets,
            |bucket| {
                Some(LakePosition {
                    snapshot_id: snapshot.snapshot_id,
                    log_end_offset: ends.get(&bucket).copied().unwrap_or(0),
                })
            },
            |bucket| by_bucket.remove(&(partitions[&bucket].clone(), bucket.bucket())),
        )
        .await
    }

    async fn keyless(
        &self,
        buckets: Vec<(Bucket, Option<PartitionName>)>,
        snapshot: proto::LakeSnapshot,
        planned: Vec<LakeSplit>,
    ) -> Result<Vec<Split>, Error> {
        let ends: HashMap<Bucket, i64> = snapshot.bucket_log_end_offset.into_iter().collect();
        let kept: BTreeSet<Option<PartitionName>> =
            buckets.iter().map(|(_, p)| p.clone()).collect();
        let mut splits: Vec<Split> = planned
            .into_iter()
            .filter(|split| split.bucket.is_none() && kept.contains(&split.partition))
            .map(Split::Lake)
            .collect();
        let tails = self
            .union(
                buckets,
                |bucket| {
                    Some(LakePosition {
                        snapshot_id: snapshot.snapshot_id,
                        log_end_offset: ends.get(&bucket).copied().unwrap_or(0),
                    })
                },
                |_| None,
            )
            .await?;
        splits.extend(tails);

        Ok(splits)
    }
}

impl fmt::Debug for MinkTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MinkTable")
            .field("path", self.table.path())
            .field("lake", &self.lake)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for MinkTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_keys = self.table.descriptor().partition_keys();
        Ok(filters
            .iter()
            .map(|filter| {
                let pinned = Constraints::pinned_by(filter, &self.schema);
                let partitions_only = pinned.as_ref().is_some_and(|pinned| {
                    !partition_keys.is_empty() && pinned.iter().all(|c| partition_keys.contains(c))
                });
                let routes = pinned
                    .as_ref()
                    .is_some_and(|pinned| pinned.iter().any(|c| self.keys.contains(c)));
                if partitions_only {
                    TableProviderFilterPushDown::Exact
                } else if routes
                    || (self.lake && predicate::convert(filter, &self.schema).is_some())
                {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(columns) => Arc::new(self.schema.project(columns)?),
            None => self.schema.clone(),
        };
        let filter = self.lake_filter(filters);
        let splits = self.splits(filters, filter.as_ref()).await?;
        let options = Options {
            projection: projection.cloned(),
            limit,
        };

        Ok(Arc::new(Scan::new(
            self.table.clone(),
            self.read.clone(),
            splits,
            options,
            filter,
            schema,
        )))
    }
}
