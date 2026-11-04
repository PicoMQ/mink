use std::collections::{BTreeMap, BTreeSet, HashMap};

use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use mink_core::{Offsets, Partition, TieredRange};
use mink_iceberg::{ExternalSnapshot, end_offset_key, end_offsets, start_offset_key};
use mink_schema::binary_schema;
use tokio::time::Instant;

use super::Writer;
use crate::Error;
use crate::external;

impl Writer {
    pub(super) async fn commit_tiered(&mut self) -> Result<(), Error> {
        let store = &self.services.store;
        let topic = self.topic.name();

        let Some(external) = store.external_tables().get(topic).await? else {
            return Ok(());
        };
        let due = self
            .last_tiering
            .is_none_or(|last| last.elapsed() >= external.schedule().interval());
        if !due {
            return Ok(());
        }

        let catalog = self.services.external_catalog(external.catalog())?;
        let table = catalog.load(&external).await?;

        self.reconcile(&table).await?;
        let ranges = self.ranges_of_current_spec(&table).await?;
        let table = match spans(&ranges)? {
            spans if spans.is_empty() => table,
            spans => {
                let committed = commit(&table, &catalog, &ranges, &spans).await?;
                let ends = spans
                    .iter()
                    .map(|span| (span.partition().index(), span.end_offset()))
                    .collect();
                store.tiered_ranges().commit(&self.lease, &ends).await?;
                committed
            }
        };
        self.last_tiering = Some(Instant::now());

        self.read_external_snapshot(&table).await
    }

    async fn read_external_snapshot(&mut self, table: &Table) -> Result<(), Error> {
        let listed = self
            .unlisted_external_snapshot
            .as_ref()
            .or(self.table.external_snapshot())
            .map(ExternalSnapshot::snapshot_id);
        if table.metadata().current_snapshot_id() == listed {
            return Ok(());
        }

        let store = &self.services.store;
        let topic = self.topic.name();
        let specs = store.time_specs().list(topic).await?;
        let schemas = store.schemas().table_schemas(topic).await?;
        let schema = schemas.last().unwrap_or(binary_schema());

        let read = ExternalSnapshot::read(table, schema, &specs).await?;
        self.unlisted_external_snapshot =
            read.filter(|read| Some(read) != self.table.external_snapshot());
        Ok(())
    }

    async fn reconcile(&self, table: &Table) -> Result<(), Error> {
        let topic = self.topic.name();
        let ranges = self.services.store.tiered_ranges();

        let found = end_offsets(table.metadata().properties())?;
        let committed = ranges.committed(topic).await?;
        for (partition, stored) in &committed {
            if found.get(partition).is_none_or(|found| found < stored) {
                return Err(Error::ExternalProgressLost(topic.to_owned()));
            }
        }

        let landed: BTreeMap<i32, i64> = found
            .into_iter()
            .filter(|(partition, end)| committed.get(partition) != Some(end))
            .collect();
        if !landed.is_empty() {
            ranges.commit(&self.lease, &landed).await?;
        }

        Ok(())
    }

    async fn ranges_of_current_spec(&self, table: &Table) -> Result<Vec<TieredRange>, Error> {
        let store = &self.services.store;
        let spec_id = table.metadata().default_partition_spec().spec_id();

        let ranges = store.tiered_ranges().list(self.topic.name()).await?;
        let stale: BTreeSet<Partition> = ranges
            .iter()
            .filter(|range| range.spec_id() != spec_id)
            .map(|range| range.offsets().partition().clone())
            .collect();

        for partition in &stale {
            for range in store
                .tiered_ranges()
                .discard(&self.lease, partition)
                .await?
            {
                match mink_iceberg::data_files(table, range.spec_id(), range.encoded_data_files()) {
                    Ok(files) => external::delete_data_files(table, &files).await,
                    Err(error) => tracing::warn!(
                        ?partition,
                        %error,
                        "the files of a discarded tiered range were not deleted"
                    ),
                }
            }
        }

        Ok(ranges
            .into_iter()
            .filter(|range| !stale.contains(range.offsets().partition()))
            .collect())
    }
}

fn spans(ranges: &[TieredRange]) -> Result<Vec<Offsets>, Error> {
    let mut spans: Vec<Offsets> = Vec::new();

    for range in ranges {
        let offsets = range.offsets();
        match spans.last_mut() {
            Some(span) if span.partition() == offsets.partition() => {
                *span = Offsets::new(
                    span.partition().clone(),
                    span.start_offset(),
                    offsets.end_offset(),
                )?;
            }
            _ => spans.push(offsets.clone()),
        }
    }

    Ok(spans)
}

async fn commit(
    table: &Table,
    catalog: &external::Catalog,
    ranges: &[TieredRange],
    spans: &[Offsets],
) -> Result<Table, Error> {
    let mut files = Vec::new();
    for range in ranges {
        files.extend(mink_iceberg::data_files(
            table,
            range.spec_id(),
            range.encoded_data_files(),
        )?);
    }

    let mut summary = HashMap::new();
    let mut properties = HashMap::new();
    for span in spans {
        let partition = span.partition().index();
        let end = (end_offset_key(partition), span.end_offset().to_string());

        summary.insert(start_offset_key(partition), span.start_offset().to_string());
        summary.insert(end.0.clone(), end.1.clone());
        properties.insert(end.0, end.1);
    }

    let transaction = Transaction::new(table);
    let transaction = transaction
        .fast_append()
        .with_check_duplicate(false)
        .add_data_files(files)
        .set_snapshot_properties(summary)
        .apply(transaction)?;

    let mut updates = transaction.update_table_properties();
    for (key, value) in properties {
        updates = updates.set(key, value);
    }
    let transaction = updates.apply(transaction)?;

    Ok(transaction.commit(&catalog.pinned(table.clone())).await?)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::TryStreamExt;
    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, SortOrder, TableMetadata, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    };
    use iceberg::table::Table;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog as _, ErrorKind, Runtime, TableIdent, TableUpdate};
    use mink_core::{
        ExternalTable, Partition, Record, Scope, Snapshot, TableLocation, TierSchedule, TieredRange,
    };
    use mink_iceberg::{FileSchema, end_offset_key, last_time_field_id};
    use uuid::Uuid;

    use super::{commit, spans};
    use crate::Error;
    use crate::testing::{TTL, Tiering, registered, tiering};
    use crate::writer::Writer;

    async fn table(tiering: &Tiering) -> Table {
        tiering
            .catalog
            .load_table(&tiering.identifier)
            .await
            .unwrap()
    }

    async fn rows(table: &Table) -> usize {
        let batches: Vec<_> = table
            .scan()
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    async fn pending(tiering: &Tiering) -> Vec<TieredRange> {
        tiering
            .services
            .store
            .tiered_ranges()
            .list(tiering.topic.name())
            .await
            .unwrap()
    }

    async fn appended_by_a_user(tiering: &Tiering) {
        let external = table(tiering).await;
        let partition = Partition::new(tiering.topic.name(), 0).unwrap();
        let record = Record::new(100, 0, None, Some(Bytes::from_static(b"x")), Vec::new()).unwrap();
        let rows = mink_iceberg::record_batch(&partition, &[record], &FileSchema::Binary).unwrap();
        let files = mink_iceberg::write_data_files(&external, vec![rows], "user")
            .await
            .unwrap();
        let transaction = Transaction::new(&external);
        let transaction = transaction
            .fast_append()
            .add_data_files(files)
            .apply(transaction)
            .unwrap();
        transaction.commit(tiering.catalog.as_ref()).await.unwrap();
    }

    async fn tiered(tiering: &Tiering) {
        tiering.index_and_commit(&[3, 2]).await;
        tiering.tier().await.tick().await.unwrap();
    }

    async fn merged(tiering: &Tiering) -> Snapshot {
        tiered(tiering).await;
        let mut writer = tiering.writer().await;
        writer.commit_tiered().await.unwrap();

        writer.commit().await.unwrap().unwrap()
    }

    async fn latest(tiering: &Tiering) -> Snapshot {
        tiering
            .services
            .store
            .snapshots()
            .latest(tiering.topic.name())
            .await
            .unwrap()
            .unwrap()
    }

    async fn planned(tiering: &Tiering, snapshot: &Snapshot) -> (u64, Vec<String>) {
        let io = table(tiering).await.file_io().clone();
        let warehouse = &tiering.services.warehouse;
        let location = TableLocation::new(warehouse.root(), tiering.topic.name());
        for listed in warehouse
            .list(&location.metadata_directory())
            .await
            .unwrap()
        {
            let bytes = warehouse.read(listed.path()).await.unwrap();
            io.new_output(listed.path())
                .unwrap()
                .write(bytes)
                .await
                .unwrap();
        }
        let bytes = io
            .new_input(location.metadata_file(&snapshot.key()))
            .unwrap()
            .read()
            .await
            .unwrap();
        let metadata: TableMetadata = serde_json::from_slice(&bytes).unwrap();
        let table = Table::builder()
            .identifier(TableIdent::from_strs(["kafka", tiering.topic.name()]).unwrap())
            .metadata(metadata)
            .file_io(io)
            .runtime(Runtime::try_current().unwrap())
            .build()
            .unwrap();

        let tasks: Vec<_> = table
            .scan()
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let records = tasks.iter().filter_map(|task| task.record_count).sum();
        let paths = tasks.into_iter().map(|task| task.data_file_path).collect();
        (records, paths)
    }

    fn start(snapshot: &Snapshot) -> i64 {
        snapshot.offsets()[0].start_offset()
    }

    async fn set_property(tiering: &Tiering, key: String, value: &str) {
        let updates = vec![TableUpdate::SetProperties {
            updates: HashMap::from([(key, value.to_owned())]),
        }];

        tiering
            .catalog
            .commit(&tiering.identifier, Vec::new(), updates)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_tier_commit_sets_the_end_offsets_with_its_files() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiered(&tiering).await;

        tiering.writer().await.commit_tiered().await.unwrap();

        let table = table(&tiering).await;
        let ranges = tiering.services.store.tiered_ranges();
        assert_eq!(table.metadata().properties()[&end_offset_key(0)], "6");
        assert_eq!(rows(&table).await, 6);
        assert_eq!(
            ranges.committed(tiering.topic.name()).await.unwrap(),
            BTreeMap::from([(0, 6)])
        );
        assert!(ranges.list(tiering.topic.name()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_commit_that_landed_unseen_is_settled_without_a_second_append() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiered(&tiering).await;
        let loaded = table(&tiering).await;
        let ranges = pending(&tiering).await;
        commit(&loaded, &tiering.catalog, &ranges, &spans(&ranges).unwrap())
            .await
            .unwrap();

        tiering.writer().await.commit_tiered().await.unwrap();

        let table = table(&tiering).await;
        assert_eq!(table.metadata().snapshots().count(), 1);
        assert_eq!(rows(&table).await, 6);
        assert_eq!(
            tiering
                .services
                .store
                .tiered_ranges()
                .committed(tiering.topic.name())
                .await
                .unwrap(),
            BTreeMap::from([(0, 6)])
        );
    }

    #[tokio::test]
    async fn a_second_commit_from_the_same_table_conflicts_and_appends_nothing() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiered(&tiering).await;
        let stale = table(&tiering).await;
        let ranges = pending(&tiering).await;
        let spans = spans(&ranges).unwrap();

        commit(&stale, &tiering.catalog, &ranges, &spans)
            .await
            .unwrap();
        let again = commit(&stale, &tiering.catalog, &ranges, &spans).await;

        assert!(
            matches!(
                &again,
                Err(Error::Iceberg(mink_iceberg::Error::Iceberg(error)))
                    if error.kind() == ErrorKind::CatalogCommitConflicts
            ),
            "{again:?}"
        );
        assert_eq!(rows(&table(&tiering).await).await, 6);
    }

    #[tokio::test]
    async fn a_rolled_back_end_offset_stops_tiering() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiered(&tiering).await;
        tiering.writer().await.commit_tiered().await.unwrap();
        set_property(&tiering, end_offset_key(0), "3").await;

        let stopped = tiering.writer().await.commit_tiered().await;

        assert!(
            matches!(stopped, Err(Error::ExternalProgressLost(_))),
            "{stopped:?}"
        );
    }

    #[tokio::test]
    async fn an_end_offset_mink_did_not_write_stops_tiering() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiered(&tiering).await;
        set_property(&tiering, end_offset_key(0), "5").await;

        let stopped = tiering.writer().await.commit_tiered().await;

        assert!(
            matches!(
                stopped,
                Err(Error::Store(mink_store::Error::NotATieredEnd {
                    offset: 5,
                    ..
                }))
            ),
            "{stopped:?}"
        );
        assert_eq!(pending(&tiering).await.len(), 1);
    }

    #[tokio::test]
    async fn a_replaced_external_table_stops_tiering() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        let other = registered(&tiering.services, 1).await;
        let identifier = TableIdent::from_strs(["mink", other.name()]).unwrap();
        let created = tiering
            .catalog
            .create_table_with_ids(
                &identifier,
                mink_schema::binary_schema().clone(),
                UnboundPartitionSpec::builder().build(),
                SortOrder::unsorted_order(),
                HashMap::new(),
            )
            .await
            .unwrap();
        let external = ExternalTable::new(
            other.name(),
            "fixture",
            "mink",
            other.name(),
            Uuid::new_v4(),
            created.metadata().location(),
            TierSchedule::default(),
        )
        .unwrap();
        tiering
            .services
            .store
            .external_tables()
            .add(&external)
            .await
            .unwrap();
        let scope = Scope::Topic(other.name().to_owned());
        let lease = tiering
            .services
            .store
            .leases()
            .acquire(&scope, Uuid::from_u128(5), TTL)
            .await
            .unwrap()
            .unwrap();
        let mut writer = Writer::start(tiering.services.clone(), lease, other.name().to_owned())
            .await
            .unwrap();

        let stopped = writer.commit_tiered().await;

        assert!(
            matches!(stopped, Err(Error::ReplacedExternalTable(_))),
            "{stopped:?}"
        );
    }

    #[tokio::test]
    async fn ranges_of_an_older_partition_spec_are_written_again() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiered(&tiering).await;
        let spec = UnboundPartitionSpec::builder()
            .with_spec_id(9)
            .add_partition_fields([UnboundPartitionField {
                source_id: mink_schema::TIMESTAMP_ID,
                field_id: Some(last_time_field_id() + 2),
                name: "month".to_owned(),
                transform: Transform::Month,
            }])
            .unwrap()
            .build();
        tiering
            .catalog
            .commit(
                &tiering.identifier,
                Vec::new(),
                vec![
                    TableUpdate::AddSpec { spec },
                    TableUpdate::SetDefaultSpec { spec_id: -1 },
                ],
            )
            .await
            .unwrap();
        let before = table(&tiering).await;
        let stale = pending(&tiering).await;
        let stale_files =
            mink_iceberg::data_files(&before, stale[0].spec_id(), stale[0].encoded_data_files())
                .unwrap();

        tiering.writer().await.commit_tiered().await.unwrap();
        let discarded = tiering
            .services
            .store
            .tiered_ranges()
            .list(tiering.topic.name())
            .await
            .unwrap();
        tiering.tier().await.tick().await.unwrap();
        tiering.writer().await.commit_tiered().await.unwrap();

        let table = table(&tiering).await;
        assert!(discarded.is_empty());
        assert_eq!(rows(&table).await, 6);
        assert_eq!(table.metadata().properties()[&end_offset_key(0)], "6");
        for file in stale_files {
            assert!(!table.file_io().exists(file.file_path()).await.unwrap());
        }
    }

    #[tokio::test]
    async fn the_merged_table_lists_each_record_once_after_a_writer_restart() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        let merged = merged(&tiering).await;

        tiering.index_and_commit(&[0]).await;

        let restarted = latest(&tiering).await;
        let (records, paths) = planned(&tiering, &restarted).await;
        let spec = table(&tiering)
            .await
            .metadata()
            .default_partition_spec()
            .clone();
        let mink = tiering.services.warehouse.root().to_owned();
        assert_eq!(spec.fields()[0].field_id, last_time_field_id() + 1);
        assert_eq!((start(&merged), start(&restarted)), (6, 6));
        assert_eq!(restarted.offsets()[0].end_offset(), 9);
        assert_eq!(records, 9);
        assert_eq!(
            paths.iter().filter(|path| path.starts_with(&mink)).count(),
            1
        );
    }

    #[tokio::test]
    async fn a_change_to_the_external_table_is_listed_by_the_next_commit() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        let before = merged(&tiering).await;
        appended_by_a_user(&tiering).await;

        let mut writer = tiering.writer().await;
        writer.commit_tiered().await.unwrap();
        let after = writer.commit().await.unwrap();

        let after = after.expect("the external change is committed");
        assert_eq!(planned(&tiering, &before).await.0, 6);
        assert_eq!(planned(&tiering, &after).await.0, 7);
        assert_eq!(start(&after), 6);
    }

    #[tokio::test]
    async fn a_rejected_external_snapshot_keeps_the_listed_one() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        let before = merged(&tiering).await;
        appended_by_a_user(&tiering).await;
        let binary = mink_schema::binary_schema();
        let extra = NestedField::optional(100, "extra", Type::Primitive(PrimitiveType::String));
        let schema = Schema::builder()
            .with_schema_id(binary.schema_id() + 1)
            .with_fields(
                binary
                    .as_struct()
                    .fields()
                    .iter()
                    .cloned()
                    .chain([extra.into()]),
            )
            .build()
            .unwrap();
        tiering
            .catalog
            .commit(
                &tiering.identifier,
                Vec::new(),
                vec![
                    TableUpdate::AddSchema { schema },
                    TableUpdate::SetCurrentSchema { schema_id: -1 },
                ],
            )
            .await
            .unwrap();

        let mut writer = tiering.writer().await;
        let rejected = writer.commit_tiered().await;
        writer.commit().await.unwrap();

        let after = latest(&tiering).await;
        assert!(
            matches!(
                rejected,
                Err(Error::Iceberg(mink_iceberg::Error::ExternalSchema))
            ),
            "{rejected:?}"
        );
        assert!(writer.unlisted_external_snapshot.is_none());
        assert_eq!(start(&after), start(&before));
        assert_eq!(planned(&tiering, &after).await.0, 6);
    }
}
