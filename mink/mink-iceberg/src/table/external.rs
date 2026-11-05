use std::collections::BTreeMap;
use std::str::FromStr;

use iceberg::spec::{
    DataContentType, ManifestContentType, ManifestFile, PartitionField, PartitionSpec, Schema,
    SortOrder, Summary, TableMetadata,
};
use iceberg::table::Table as ExternalTable;
use mink_core::TimeSpecs;

use super::FormatVersion;
use super::keys::{
    EXTERNAL_GENERATION, EXTERNAL_SNAPSHOT_ID, end_offsets, external_end_offset_key,
    external_end_offsets,
};
use super::specs::{PARTITION_FIELD, last_time_field_id, partition_specs};
use crate::Error;

const EXTERNAL_SPEC_ID_BASE: i32 = 1 << 30;
const SPECS_PER_GENERATION: i32 = 1 << 16;
const MAX_GENERATION: i32 =
    (i32::MAX - EXTERNAL_SPEC_ID_BASE - (SPECS_PER_GENERATION - 1)) / SPECS_PER_GENERATION;

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalSnapshot {
    generation: i32,
    snapshot_id: i64,
    manifests: Vec<ManifestFile>,
    specs: ExternalSpecs,
    end_offsets: BTreeMap<i32, i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ExternalSpecs {
    partition_specs: Vec<PartitionSpec>,
    sort_orders: Vec<SortOrder>,
}

impl ExternalSnapshot {
    pub async fn check(
        table: &ExternalTable,
        schema: &Schema,
        specs: &TimeSpecs,
    ) -> Result<(), Error> {
        checked_manifests(table, schema, specs).await.map(|_| ())
    }

    pub async fn read(
        table: &ExternalTable,
        generation: i32,
        schema: &Schema,
        specs: &TimeSpecs,
    ) -> Result<Option<ExternalSnapshot>, Error> {
        if !(1..=MAX_GENERATION).contains(&generation) {
            return Err(Error::ExternalGeneration(generation));
        }
        let Some((snapshot_id, checked)) = checked_manifests(table, schema, specs).await? else {
            return Ok(None);
        };

        let mut manifests = Vec::with_capacity(checked.len());
        for mut manifest in checked {
            manifest.partition_spec_id = listed_spec_id(generation, manifest.partition_spec_id)?;
            manifests.push(manifest);
        }

        let metadata = table.metadata();
        let mut partition_specs = Vec::new();
        for spec in specs_by_id(metadata) {
            let id = listed_spec_id(generation, spec.spec_id())?;
            partition_specs.push(spec.with_spec_id(id));
        }

        Ok(Some(ExternalSnapshot {
            generation,
            snapshot_id,
            manifests,
            specs: ExternalSpecs {
                partition_specs,
                sort_orders: sort_orders_by_id(metadata),
            },
            end_offsets: end_offsets(metadata.properties())?,
        }))
    }

    pub(super) fn listed_in(
        declared: &BTreeMap<i32, ExternalSpecs>,
        manifests: Vec<ManifestFile>,
        summary: &Summary,
    ) -> Result<Option<ExternalSnapshot>, Error> {
        let properties = &summary.additional_properties;
        let Some(generation) = listed_generation(summary)? else {
            return Ok(None);
        };
        let snapshot_id = summary_value(properties, EXTERNAL_SNAPSHOT_ID)?;
        let specs = declared
            .get(&generation)
            .cloned()
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "a snapshot lists external generation {generation}, which the metadata declares no specs of"
                ))
            })?;

        Ok(Some(ExternalSnapshot {
            generation,
            snapshot_id,
            manifests,
            specs,
            end_offsets: external_end_offsets(properties)?,
        }))
    }

    pub fn generation(&self) -> i32 {
        self.generation
    }

    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub fn end_offset(&self, partition: i32) -> Option<i64> {
        self.end_offsets.get(&partition).copied()
    }

    pub fn sequence_number(&self) -> Option<i64> {
        self.manifests
            .iter()
            .map(|manifest| manifest.sequence_number)
            .max()
    }

    pub(super) fn manifests(&self) -> &[ManifestFile] {
        &self.manifests
    }

    pub(super) fn specs(&self) -> &ExternalSpecs {
        &self.specs
    }

    pub(super) fn summary_properties(&self) -> impl Iterator<Item = (String, String)> {
        let identity = [
            (EXTERNAL_GENERATION.to_owned(), self.generation.to_string()),
            (
                EXTERNAL_SNAPSHOT_ID.to_owned(),
                self.snapshot_id.to_string(),
            ),
        ];
        let end_offsets = self
            .end_offsets
            .iter()
            .map(|(partition, end)| (external_end_offset_key(*partition), end.to_string()));

        identity.into_iter().chain(end_offsets)
    }

    pub(super) fn data_files(&self) -> Option<u64> {
        self.sum(ManifestContentType::Data, files)
    }

    pub(super) fn records(&self) -> Option<u64> {
        self.sum(ManifestContentType::Data, rows)
    }

    pub(super) fn delete_files(&self) -> Option<u64> {
        self.sum(ManifestContentType::Deletes, files)
    }

    pub(super) fn position_deletes(&self) -> Option<u64> {
        self.sum(ManifestContentType::Deletes, rows)
    }

    fn sum(
        &self,
        content: ManifestContentType,
        count: fn(&ManifestFile) -> Option<u64>,
    ) -> Option<u64> {
        self.manifests
            .iter()
            .filter(|manifest| manifest.content == content)
            .map(count)
            .sum()
    }
}

impl ExternalSpecs {
    pub(super) fn declared_in(metadata: &TableMetadata) -> BTreeMap<i32, ExternalSpecs> {
        let mut declared: BTreeMap<i32, ExternalSpecs> = BTreeMap::new();
        for spec in specs_by_id(metadata) {
            if let Some(generation) = generation_of(spec.spec_id()) {
                declared
                    .entry(generation)
                    .or_insert_with(|| ExternalSpecs {
                        partition_specs: Vec::new(),
                        sort_orders: Vec::new(),
                    })
                    .partition_specs
                    .push(spec);
            }
        }

        if let Some(newest) = declared.values_mut().next_back() {
            newest.sort_orders = sort_orders_by_id(metadata);
        }
        declared
    }

    pub(super) fn merge(&mut self, newer: &ExternalSpecs) {
        for spec in &newer.partition_specs {
            match self
                .partition_specs
                .iter_mut()
                .find(|known| known.spec_id() == spec.spec_id())
            {
                Some(known) => *known = spec.clone(),
                None => self.partition_specs.push(spec.clone()),
            }
        }
        self.partition_specs.sort_by_key(PartitionSpec::spec_id);
        self.sort_orders = newer.sort_orders.clone();
    }

    pub(super) fn partition_specs(&self) -> &[PartitionSpec] {
        &self.partition_specs
    }

    pub(super) fn sort_orders(&self) -> &[SortOrder] {
        &self.sort_orders
    }
}

pub(super) fn listed_generation(summary: &Summary) -> Result<Option<i32>, Error> {
    let properties = &summary.additional_properties;

    match properties.contains_key(EXTERNAL_GENERATION) {
        true => Ok(Some(summary_value(properties, EXTERNAL_GENERATION)?)),
        false => Ok(None),
    }
}

fn summary_value<T: FromStr>(
    properties: &std::collections::HashMap<String, String>,
    key: &str,
) -> Result<T, Error> {
    let value = properties
        .get(key)
        .ok_or_else(|| Error::Corrupt(format!("summary {key} is missing")))?;

    value
        .parse()
        .map_err(|_| Error::Corrupt(format!("summary {key} is {value:?}")))
}

pub fn check_schema(schema: &Schema, external: &Schema) -> Result<(), Error> {
    if schema.as_struct() != external.as_struct() {
        return Err(Error::ExternalSchema);
    }

    Ok(())
}

pub(super) fn is_listed_spec_id(spec_id: i32) -> bool {
    spec_id >= EXTERNAL_SPEC_ID_BASE
}

fn listed_spec_id(generation: i32, external_spec_id: i32) -> Result<i32, Error> {
    if !(0..SPECS_PER_GENERATION).contains(&external_spec_id) {
        return Err(Error::ExternalSpecId(external_spec_id));
    }

    Ok(EXTERNAL_SPEC_ID_BASE + generation * SPECS_PER_GENERATION + external_spec_id)
}

fn generation_of(spec_id: i32) -> Option<i32> {
    is_listed_spec_id(spec_id).then(|| (spec_id - EXTERNAL_SPEC_ID_BASE) / SPECS_PER_GENERATION)
}

fn files(manifest: &ManifestFile) -> Option<u64> {
    Some(u64::from(manifest.added_files_count?) + u64::from(manifest.existing_files_count?))
}

fn rows(manifest: &ManifestFile) -> Option<u64> {
    Some(manifest.added_rows_count? + manifest.existing_rows_count?)
}

fn specs_by_id(metadata: &TableMetadata) -> Vec<PartitionSpec> {
    let mut specs: Vec<PartitionSpec> = metadata
        .partition_specs_iter()
        .map(|spec| (**spec).clone())
        .collect();
    specs.sort_by_key(PartitionSpec::spec_id);

    specs
}

fn sort_orders_by_id(metadata: &TableMetadata) -> Vec<SortOrder> {
    let mut orders: Vec<SortOrder> = metadata
        .sort_orders_iter()
        .filter(|order| !order.is_unsorted())
        .map(|order| (**order).clone())
        .collect();
    orders.sort_by_key(|order| order.order_id);

    orders
}

async fn checked_manifests(
    table: &ExternalTable,
    schema: &Schema,
    specs: &TimeSpecs,
) -> Result<Option<(i64, Vec<ManifestFile>)>, Error> {
    let metadata = table.metadata();
    FormatVersion::from_iceberg(metadata.format_version())?;
    check_schema(schema, metadata.current_schema())?;
    check_partition_fields(metadata, specs)?;

    let Some(current) = metadata.current_snapshot() else {
        return Ok(None);
    };
    let manifests: Vec<ManifestFile> = table
        .manifest_list_reader(current)
        .load()
        .await?
        .consume_entries()
        .into_iter()
        .collect();

    for manifest in &manifests {
        if manifest.content == ManifestContentType::Deletes {
            let deletes = manifest.load_manifest(table.file_io()).await?;
            let equality = deletes
                .entries()
                .iter()
                .any(|entry| entry.content_type() == DataContentType::EqualityDeletes);
            if equality {
                return Err(Error::EqualityDeletes);
            }
        }
    }

    Ok(Some((current.snapshot_id(), manifests)))
}

fn check_partition_fields(metadata: &TableMetadata, specs: &TimeSpecs) -> Result<(), Error> {
    let own = partition_specs(specs)?;
    let own: Vec<&PartitionField> = own.iter().flat_map(PartitionSpec::fields).collect();
    let time_field_ids = PARTITION_FIELD..=last_time_field_id();

    for field in metadata
        .partition_specs_iter()
        .flat_map(|spec| spec.fields())
    {
        if time_field_ids.contains(&field.field_id) && !own.contains(&field) {
            return Err(Error::ExternalPartitionField(field.field_id));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::FormatVersion as IcebergFormatVersion;
    use iceberg::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, NestedField,
        PrimitiveType, Struct, TableMetadataBuilder, Type,
    };
    use mink_core::Granularity;
    use mink_schema::binary_schema;

    use super::*;
    use crate::table::testing::{specs, tiered};
    use crate::testing::{appended, external_table};

    async fn read(
        table: &ExternalTable,
        specs: &TimeSpecs,
    ) -> Result<Option<ExternalSnapshot>, Error> {
        ExternalSnapshot::read(table, 1, binary_schema(), specs).await
    }

    fn deletes(content: DataContentType) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path("memory://tables/orders/data/deletes.parquet".to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(Literal::date(0))]))
            .partition_spec_id(1)
            .record_count(2)
            .file_size_in_bytes(100);
        if content == DataContentType::EqualityDeletes {
            builder.equality_ids(Some(vec![2]));
        }

        builder.build().unwrap()
    }

    #[tokio::test]
    async fn a_table_without_a_snapshot_has_nothing_to_list() {
        let table = external_table(last_time_field_id() + 1, HashMap::new());

        assert_eq!(read(&table, &specs()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_table_of_another_schema_is_rejected() {
        let table = tiered().await;
        let other = Schema::builder()
            .with_fields([
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let read = ExternalSnapshot::read(&table, 1, &other, &specs()).await;

        assert!(matches!(read, Err(Error::ExternalSchema)), "{read:?}");
    }

    #[tokio::test]
    async fn a_table_of_another_format_version_is_rejected() {
        let table = tiered().await;
        let metadata = TableMetadataBuilder::new_from_metadata(table.metadata().clone(), None)
            .upgrade_format_version(IcebergFormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let upgraded = ExternalTable::builder()
            .identifier(table.identifier().clone())
            .metadata(metadata)
            .file_io(table.file_io().clone())
            .runtime(iceberg::Runtime::try_current().unwrap())
            .build()
            .unwrap();

        let read = read(&upgraded, &specs()).await;

        assert!(matches!(read, Err(Error::FormatVersion(3))), "{read:?}");
    }

    #[tokio::test]
    async fn a_partition_field_with_an_id_a_time_field_has_or_may_take_is_rejected() {
        let hourly = TimeSpecs::new(vec![Granularity::Hour]).unwrap();
        let taken = external_table(1001, HashMap::new());
        let free = external_table(last_time_field_id(), HashMap::new());
        let apart = external_table(last_time_field_id() + 1, HashMap::new());

        let taken = read(&taken, &hourly).await;
        let free = read(&free, &hourly).await;

        assert!(
            matches!(taken, Err(Error::ExternalPartitionField(1001))),
            "{taken:?}"
        );
        assert!(
            matches!(free, Err(Error::ExternalPartitionField(id)) if id == last_time_field_id()),
            "{free:?}"
        );
        assert!(read(&apart, &hourly).await.is_ok());
    }

    #[tokio::test]
    async fn position_deletes_are_listed_and_equality_deletes_rejected() {
        let table = tiered().await;
        let positions = appended(
            &table,
            Vec::new(),
            vec![deletes(DataContentType::PositionDeletes)],
            &[(0, 6)],
        )
        .await;
        let equalities = appended(
            &table,
            Vec::new(),
            vec![deletes(DataContentType::EqualityDeletes)],
            &[(0, 6)],
        )
        .await;

        let listed = read(&positions, &specs()).await.unwrap().unwrap();
        let rejected = read(&equalities, &specs()).await;

        assert_eq!(listed.manifests().len(), 2);
        assert_eq!(listed.sequence_number(), Some(2));
        assert_eq!(
            (
                listed.data_files(),
                listed.records(),
                listed.delete_files(),
                listed.position_deletes()
            ),
            (Some(1), Some(6), Some(1), Some(2))
        );
        assert!(
            matches!(rejected, Err(Error::EqualityDeletes)),
            "{rejected:?}"
        );
    }

    #[tokio::test]
    async fn generations_run_from_one_to_the_last_that_fits_a_spec_id() {
        let table = tiered().await;
        let read = async |generation: i32| {
            ExternalSnapshot::read(&table, generation, binary_schema(), &specs()).await
        };

        let last = read(MAX_GENERATION).await.unwrap().unwrap();

        assert!(matches!(read(0).await, Err(Error::ExternalGeneration(0))));
        assert!(matches!(
            read(MAX_GENERATION + 1).await,
            Err(Error::ExternalGeneration(_))
        ));
        assert_eq!(last.generation(), 16383);
    }

    #[tokio::test]
    async fn a_relisted_generation_keeps_the_specs_it_listed_before() {
        let table = tiered().await;
        let read = ExternalSnapshot::read(&table, 1, binary_schema(), &specs())
            .await
            .unwrap()
            .unwrap();
        let mut declared = read.specs().clone();
        let fewer = ExternalSpecs {
            partition_specs: read.specs().partition_specs()[1..].to_vec(),
            sort_orders: Vec::new(),
        };

        declared.merge(&fewer);

        assert_eq!(declared.partition_specs(), read.specs().partition_specs());
        assert!(declared.sort_orders().is_empty());
    }
}
