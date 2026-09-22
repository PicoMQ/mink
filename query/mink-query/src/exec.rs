//! The physical scan of one table: one DataFusion partition per split, each a pinned read of a bucket
//! or of a lake partition, or a lookup of pinned primary keys, with the pushed filter, projection
//! and limit.

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use mink_client::{Lookup, Table};
use mink_lake::{LakeSplit, Predicate, take_rows};
use mink_read::{Options, Plan, Read};
use mink_table::Bucket;

use crate::error::Error;

#[derive(Clone)]
pub enum Split {
    Union(Plan),
    Lake(LakeSplit),
    Snapshot(Bucket),
    Lookup {
        lookup: Arc<Lookup>,
        keys: RecordBatch,
    },
}

impl Split {
    fn lake(&self) -> Option<&LakeSplit> {
        match self {
            Split::Union(plan) => plan.split.as_ref(),
            Split::Lake(split) => Some(split),
            Split::Snapshot(_) | Split::Lookup { .. } => None,
        }
    }

    fn keys(&self) -> usize {
        match self {
            Split::Lookup { keys, .. } => keys.num_rows(),
            _ => 0,
        }
    }

    fn files(&self) -> usize {
        self.lake().map_or(0, |s| s.files)
    }

    fn rows(&self) -> Option<u64> {
        let lake = match self.lake() {
            Some(split) => split.rows?,
            None => 0,
        };
        let log = match self {
            Split::Union(plan) => plan.log_rows(),
            Split::Lake(_) => 0,
            Split::Lookup { keys, .. } => keys.num_rows() as u64,
            Split::Snapshot(_) => return None,
        };
        Some(lake + log)
    }

    fn bytes(&self) -> u64 {
        self.lake().map_or(0, |s| s.bytes)
    }
}

pub struct Scan {
    table: Table,
    read: Arc<Read>,
    splits: Vec<Split>,
    options: Options,
    filter: Option<Predicate>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl Scan {
    pub fn new(
        table: Table,
        read: Arc<Read>,
        splits: Vec<Split>,
        options: Options,
        filter: Option<Predicate>,
        schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(splits.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&metrics)
            .global_counter("files")
            .add(splits.iter().map(Split::files).sum());

        Scan {
            table,
            read,
            splits,
            options,
            filter,
            properties: Arc::new(properties),
            metrics,
        }
    }

    fn open(&self, split: Split) -> BoxStream<'static, Result<RecordBatch, Error>> {
        let table = self.table.clone();
        let read = self.read.clone();
        let options = self.options.clone();
        match split {
            Split::Union(plan) => Box::pin(
                futures::stream::once(async move {
                    read.read(table.schema(), plan, &options)
                        .await
                        .map(|rows| rows.map_err(Error::from))
                        .map_err(Error::from)
                })
                .try_flatten(),
            ),
            Split::Lake(split) => Box::pin(
                futures::stream::once(async move {
                    read.read_lake(table.schema(), split, &options)
                        .await
                        .map(|rows| rows.map_err(Error::from))
                        .map_err(Error::from)
                })
                .try_flatten(),
            ),
            Split::Snapshot(bucket) => Box::pin(
                futures::stream::once(async move {
                    let snapshot = table.snapshot(bucket, None).await?;
                    Ok::<_, Error>(finish(snapshot.batches.map_err(Error::from), options))
                })
                .try_flatten(),
            ),
            Split::Lookup { lookup, keys } => Box::pin(
                futures::stream::once(async move {
                    let found = lookup.lookup(&keys).await?.into_iter().flatten();
                    let rows = futures::stream::iter(found.map(Ok));
                    Ok::<_, Error>(finish(rows, options))
                })
                .try_flatten(),
            ),
        }
    }
}

fn finish(
    rows: impl futures::Stream<Item = Result<RecordBatch, Error>> + Send + 'static,
    options: Options,
) -> BoxStream<'static, Result<RecordBatch, Error>> {
    let rows = rows.map(move |rows| {
        let rows = rows?;
        match &options.projection {
            Some(columns) => Ok(mink_read::project(&rows, columns)?),
            None => Ok(rows),
        }
    });
    match options.limit {
        Some(limit) => Box::pin(take_rows(rows, limit)),
        None => Box::pin(rows),
    }
}

impl fmt::Debug for Split {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Split::Union(plan) => f.debug_tuple("Union").field(plan).finish(),
            Split::Lake(split) => f.debug_tuple("Lake").field(split).finish(),
            Split::Snapshot(bucket) => f.debug_tuple("Snapshot").field(bucket).finish(),
            Split::Lookup { keys, .. } => f.debug_tuple("Lookup").field(&keys.num_rows()).finish(),
        }
    }
}

impl fmt::Debug for Scan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scan")
            .field("table", self.table.path())
            .field("splits", &self.splits.len())
            .field("options", &self.options)
            .field("filter", &self.filter)
            .finish()
    }
}

impl DisplayAs for Scan {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MinkScan: table={}, splits={}, files={}",
            self.table.path(),
            self.splits.len(),
            self.splits.iter().map(Split::files).sum::<usize>()
        )?;
        let keys: usize = self.splits.iter().map(Split::keys).sum();
        if keys > 0 {
            write!(f, ", keys={keys}")?;
        }
        if let Some(filter) = &self.filter {
            write!(f, ", filter={filter}")?;
        }
        if let Some(projection) = &self.options.projection {
            write!(f, ", projection={projection:?}")?;
        }
        if let Some(limit) = self.options.limit {
            write!(f, ", limit={limit}")?;
        }

        Ok(())
    }
}

impl ExecutionPlan for Scan {
    fn name(&self) -> &str {
        "MinkScan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let Some(split) = self.splits.get(partition).cloned() else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            )));
        };
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let rows = self
            .open(split)
            .map_err(DataFusionError::from)
            .inspect_ok(move |batch| output_rows.add(batch.num_rows()));

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, rows)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let splits: &[Split] = match partition {
            Some(index) => self
                .splits
                .get(index)
                .map(std::slice::from_ref)
                .unwrap_or(&[]),
            None => &self.splits,
        };
        let rows = splits.iter().map(Split::rows).sum::<Option<u64>>();
        let bytes: u64 = splits.iter().map(Split::bytes).sum();

        Ok(Arc::new(Statistics {
            num_rows: match rows {
                Some(rows) => Precision::Inexact(rows as usize),
                None => Precision::Absent,
            },
            total_byte_size: Precision::Inexact(bytes as usize),
            column_statistics: Statistics::unknown_column(&self.schema()),
        }))
    }
}
