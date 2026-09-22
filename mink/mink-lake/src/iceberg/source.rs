//! Plans an Iceberg snapshot once into splits per partition and bucket, pruning files with the pushed
//! filter, and streams each split with projection, row-group pruning and an optional limit.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Literal, PrimitiveLiteral, Transform};
use iceberg::table::Table;
use iceberg::{ErrorKind, Runtime};
use mink_table::{BucketId, PartitionName, Path};

use crate::error::{Error, Result};
use crate::iceberg::catalog::Catalog;
use crate::iceberg::predicate::Binder;
use crate::predicate::Predicate;
use crate::source::{LakeSplit, Reader, ScanOptions, Split, Tasks, take_rows};

const BATCH_ROWS: usize = 8192;

pub struct Source {
    table: Table,
}

struct Planned {
    table: Table,
    tasks: Vec<FileScanTask>,
}

impl Source {
    pub async fn open(catalog: &Catalog, path: &Path) -> Result<Self> {
        let ident = Catalog::identifier(path);
        let table = catalog.target().load(&ident).await.map_err(|e| match e {
            Error::Iceberg(e) if e.kind() == ErrorKind::TableNotFound => {
                Error::TableNotFound(path.clone())
            }
            other => other,
        })?;

        Ok(Source { table })
    }

    pub fn new(table: Table) -> Self {
        Source { table }
    }

    pub fn user_schema(&self) -> Result<SchemaRef> {
        Ok(Arc::new(iceberg::arrow::schema_to_arrow_schema(
            self.table.metadata().current_schema(),
        )?))
    }

    fn locate(&self, task: &FileScanTask) -> Result<(Option<PartitionName>, Option<BucketId>)> {
        let partition = task
            .partition
            .as_ref()
            .ok_or_else(|| Error::Other(format!("{} has no partition", task.data_file_path)))?;
        let spec = task
            .partition_spec
            .as_deref()
            .unwrap_or_else(|| self.table.metadata().default_partition_spec());
        let mut bucket = None;
        let mut names = Vec::new();
        for (field, value) in spec.fields().iter().zip(partition.fields()) {
            match (&field.transform, value) {
                (Transform::Bucket(_), Some(Literal::Primitive(PrimitiveLiteral::Int(b)))) => {
                    bucket = Some(BucketId(*b as u32));
                }
                (Transform::Identity, Some(Literal::Primitive(PrimitiveLiteral::String(s)))) => {
                    names.push(s.as_str());
                }
                (transform, other) => {
                    return Err(Error::Other(format!(
                        "{}: partition value {other:?} for {transform} is not supported",
                        task.data_file_path
                    )));
                }
            }
        }
        let partition = if names.is_empty() {
            None
        } else {
            Some(
                names
                    .join("$")
                    .parse()
                    .map_err(|e| Error::Other(format!("partition name: {e}")))?,
            )
        };

        Ok((partition, bucket))
    }

    fn project_field_ids(&self, options: &ScanOptions) -> Vec<i32> {
        let fields = self.table.metadata().current_schema().as_struct().fields();
        match &options.projection {
            Some(columns) => columns
                .iter()
                .filter_map(|&i| fields.get(i))
                .map(|f| f.id)
                .collect(),
            None => fields.iter().map(|f| f.id).collect(),
        }
    }
}

#[async_trait]
impl crate::source::Source for Source {
    type Split = Vec<FileScanTask>;

    async fn plan(
        &self,
        snapshot_id: i64,
        filter: Option<&Predicate>,
    ) -> Result<Vec<Split<Vec<FileScanTask>>>> {
        let metadata = self.table.metadata();
        let mut scan = self
            .table
            .scan()
            .snapshot_id(snapshot_id)
            .select_all()
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true);
        if let Some(filter) = filter {
            let snapshot = metadata
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| Error::Other(format!("snapshot {snapshot_id} is not in the table")))?
                .schema(metadata)?;
            scan =
                scan.with_filter(Binder::new(metadata.current_schema(), &snapshot).bind(filter)?);
        }
        let tasks: Vec<FileScanTask> = scan.build()?.plan_files().await?.try_collect().await?;
        let mut groups: BTreeMap<(Option<PartitionName>, Option<BucketId>), Vec<FileScanTask>> =
            BTreeMap::new();
        for task in tasks {
            groups.entry(self.locate(&task)?).or_default().push(task);
        }

        Ok(groups
            .into_iter()
            .map(|((partition, bucket), tasks)| Split {
                bucket,
                partition,
                files: tasks.len(),
                rows: tasks.iter().map(|t| t.record_count).sum(),
                bytes: tasks.iter().map(|t| t.length).sum(),
                inner: tasks,
            })
            .collect())
    }

    async fn read(
        &self,
        split: Split<Vec<FileScanTask>>,
        options: ScanOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let ids = self.project_field_ids(&options);
        let current = self.table.metadata().current_schema().clone();
        let tasks: Vec<iceberg::Result<FileScanTask>> = split
            .inner
            .into_iter()
            .map(|mut task| {
                task.schema = current.clone();
                task.project_field_ids = ids.clone();
                Ok(task)
            })
            .collect();
        let rows = ArrowReaderBuilder::new(self.table.file_io().clone(), Runtime::try_current()?)
            .with_batch_size(BATCH_ROWS)
            .with_data_file_concurrency_limit(1)
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build()
            .read(Box::pin(futures::stream::iter(tasks)))?
            .stream()
            .map_err(Error::from);

        Ok(match options.limit {
            Some(limit) => Box::pin(take_rows(rows, limit)),
            None => Box::pin(rows),
        })
    }
}

#[async_trait]
impl Reader for Catalog {
    async fn user_schema(&self, path: &Path) -> Result<SchemaRef> {
        Source::open(self, path).await?.user_schema()
    }

    async fn plan(
        &self,
        path: &Path,
        snapshot_id: i64,
        filter: Option<&Predicate>,
    ) -> Result<Vec<LakeSplit>> {
        let source = Source::open(self, path).await?;
        let splits = crate::source::Source::plan(&source, snapshot_id, filter).await?;

        Ok(splits
            .into_iter()
            .map(|split| {
                split.map(|tasks| {
                    Tasks::new(Planned {
                        table: source.table.clone(),
                        tasks,
                    })
                })
            })
            .collect())
    }

    async fn read(
        &self,
        split: LakeSplit,
        options: ScanOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let planned: &Planned = split.inner.downcast()?;
        let source = Source::new(planned.table.clone());
        let tasks = planned.tasks.clone();

        crate::source::Source::read(&source, split.map(|_| tasks), options).await
    }
}
