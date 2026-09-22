//! Connection to an Iceberg catalog: creating and evolving tables from descriptors, and opening writers and committers.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use iceberg::spec::{FormatVersion, PartitionSpec, Transform, Type};
use iceberg::{
    CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use mink_common::sync::lock;
use mink_coordinator::{LakeCatalog, lake};
use mink_table::{Descriptor, LakeFormat, Path};

use crate::committer::CommitterContext;
use crate::config::{CatalogKind, Iceberg};
use crate::error::{Error, Result};
use crate::iceberg::commit::CommitTarget;
use crate::iceberg::memory::MemoryCommitTarget;
use crate::iceberg::rest::RestCommitTarget;
use crate::iceberg::schema::{
    Ids, Spec, evolve_schema, format_version, table_properties, table_schema, to_iceberg,
};
use crate::iceberg::write::{Committable, WriteResult};
use crate::writer::{Factory, WriterContext};

const CATALOG_NAME: &str = "mink-iceberg-catalog";

type Shared = (Arc<dyn iceberg::Catalog>, Arc<dyn CommitTarget>);

fn memory_catalogs() -> &'static Mutex<HashMap<String, Shared>> {
    static CATALOGS: OnceLock<Mutex<HashMap<String, Shared>>> = OnceLock::new();
    CATALOGS.get_or_init(Default::default)
}

#[derive(Clone)]
pub struct Catalog {
    catalog: Arc<dyn iceberg::Catalog>,
    target: Arc<dyn CommitTarget>,
}

impl fmt::Debug for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Catalog").finish_non_exhaustive()
    }
}

impl Catalog {
    pub async fn connect(config: &Iceberg) -> Result<Self> {
        let storage = Arc::new(OpenDalResolvingStorageFactory::new());
        let mut props: HashMap<String, String> = config
            .properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let (catalog, target): (Arc<dyn iceberg::Catalog>, Arc<dyn CommitTarget>) = match config
            .catalog
        {
            CatalogKind::Rest => {
                let uri = config.uri.clone().ok_or_else(|| {
                    Error::invalid("datalake.iceberg.uri is required for a REST catalog")
                })?;
                let rest_props = props.clone();
                props.insert(REST_CATALOG_PROP_URI.into(), uri.clone());
                if let Some(warehouse) = &config.warehouse {
                    props.insert(REST_CATALOG_PROP_WAREHOUSE.into(), warehouse.clone());
                }
                let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
                    RestCatalogBuilder::default()
                        .with_storage_factory(storage)
                        .load(CATALOG_NAME, props)
                        .await?,
                );
                let target = RestCommitTarget::connect(
                    catalog.clone(),
                    &uri,
                    config.warehouse.as_deref(),
                    &rest_props,
                )
                .await?;
                (catalog, Arc::new(target))
            }
            CatalogKind::Memory => {
                let warehouse = config.warehouse.clone().ok_or_else(|| {
                    Error::invalid("datalake.iceberg.warehouse is required for a memory catalog")
                })?;
                // One catalog per warehouse per process so in-process nodes share tables.
                let shared = lock(memory_catalogs()).get(&warehouse).cloned();
                match shared {
                    Some(shared) => shared,
                    None => {
                        props.insert(MEMORY_CATALOG_WAREHOUSE.into(), warehouse.clone());
                        let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
                            MemoryCatalogBuilder::default()
                                .with_storage_factory(storage)
                                .load(CATALOG_NAME, props)
                                .await?,
                        );
                        let target: Arc<dyn CommitTarget> =
                            Arc::new(MemoryCommitTarget::new(catalog.clone()));
                        lock(memory_catalogs())
                            .entry(warehouse)
                            .or_insert((catalog, target))
                            .clone()
                    }
                }
            }
        };

        Ok(Catalog { catalog, target })
    }

    pub fn target(&self) -> &Arc<dyn CommitTarget> {
        &self.target
    }

    pub fn catalog(&self) -> &Arc<dyn iceberg::Catalog> {
        &self.catalog
    }

    pub fn identifier(path: &Path) -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new(path.database().as_str().to_string()),
            path.table().as_str().to_string(),
        )
    }

    pub async fn attach(&self, path: &Path, descriptor: &Descriptor) -> Result<lake::Created> {
        let ident = Self::identifier(path);
        let table = self.target.load(&ident).await.map_err(|e| match e {
            Error::Iceberg(e) if e.kind() == ErrorKind::TableNotFound => {
                Error::TableNotFound(path.clone())
            }
            other => other,
        })?;
        let metadata = table.metadata();
        let version = format_version(descriptor)?;
        if version != metadata.format_version() {
            return Err(Error::invalid(format!(
                "attached table is format version {}, descriptor asks for {version}",
                metadata.format_version()
            )));
        }
        let descriptor = if descriptor.schema().columns().is_empty() {
            adopt(
                descriptor,
                metadata.current_schema(),
                metadata.default_partition_spec(),
            )?
        } else {
            check_attached_schema(descriptor.schema(), metadata.current_schema(), version)?;
            check_attached_partitioning(
                descriptor,
                metadata.current_schema(),
                metadata.default_partition_spec(),
            )?;
            descriptor.clone()
        };

        let properties = table_properties(&descriptor);
        let updates: HashMap<String, String> = properties
            .into_iter()
            .filter(|(key, value)| metadata.properties().get(key) != Some(value))
            .collect();
        if !updates.is_empty() {
            self.target
                .commit(
                    &ident,
                    vec![TableRequirement::UuidMatch {
                        uuid: metadata.uuid(),
                    }],
                    vec![TableUpdate::SetProperties { updates }],
                )
                .await?;
        }

        Ok(lake::Created {
            descriptor: Some(descriptor),
            baseline_snapshot_id: metadata.current_snapshot_id(),
        })
    }

    pub async fn create(&self, path: &Path, descriptor: &Descriptor) -> Result<()> {
        let spec = Spec::new(path, descriptor)?;
        let ident = Self::identifier(path);
        let creation = || {
            TableCreation::builder()
                .name(ident.name().to_string())
                .schema(spec.schema.clone())
                .partition_spec(spec.partition_spec.clone())
                .sort_order(spec.sort_order.clone())
                .properties(spec.properties.clone())
                .format_version(spec.format_version)
                .build()
        };

        let attempt = |creation| async {
            match self.catalog.create_table(ident.namespace(), creation).await {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == ErrorKind::NamespaceNotFound => Ok(false),
                Err(e) if e.kind() == ErrorKind::TableAlreadyExists => {
                    Err(Error::TableExists(path.clone()))
                }
                Err(e) => Err(Error::from(e)),
            }
        };
        if attempt(creation()).await? {
            return Ok(());
        }

        self.create_namespace(ident.namespace()).await?;
        if attempt(creation()).await? {
            return Ok(());
        }

        Err(Error::Other(format!(
            "namespace {} is still missing after it was created; table {path} not created",
            path.database()
        )))
    }

    pub async fn alter(&self, path: &Path, from: &Descriptor, to: &Descriptor) -> Result<()> {
        let ident = Self::identifier(path);
        let table = self.target.load(&ident).await?;
        let metadata = table.metadata();

        let mut updates = Vec::new();
        let mut requirements = vec![TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        }];
        let version = format_version(to)?;
        if version < metadata.format_version() {
            return Err(Error::invalid(format!(
                "Iceberg format version cannot go from {} to {version}",
                metadata.format_version()
            )));
        }
        if version > metadata.format_version() {
            updates.push(TableUpdate::UpgradeFormatVersion {
                format_version: version,
            });
        }
        if let Some((schema, last_column_id)) = evolve_schema(
            metadata.current_schema(),
            metadata.last_column_id(),
            from,
            to,
        )? {
            requirements.push(TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id(),
            });
            requirements.push(TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: metadata.last_column_id(),
            });
            updates.push(TableUpdate::AddSchema { schema });
            updates.push(TableUpdate::SetCurrentSchema { schema_id: -1 });
            tracing::info!(table = %path, last_column_id, "iceberg schema evolved");
        }

        let before = table_properties(from);
        let after = table_properties(to);
        let set: HashMap<String, String> = after
            .iter()
            .filter(|(key, value)| before.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let removals: Vec<String> = before
            .keys()
            .filter(|key| !after.contains_key(*key))
            .cloned()
            .collect();
        if !set.is_empty() {
            updates.push(TableUpdate::SetProperties { updates: set });
        }
        if !removals.is_empty() {
            updates.push(TableUpdate::RemoveProperties { removals });
        }
        if updates.is_empty() {
            return Ok(());
        }

        self.target.commit(&ident, requirements, updates).await?;

        Ok(())
    }

    async fn create_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        if self.catalog.namespace_exists(namespace).await? {
            return Ok(());
        }

        match self
            .catalog
            .create_namespace(namespace, HashMap::new())
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NamespaceAlreadyExists => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

fn adopt(
    descriptor: &Descriptor,
    current: &iceberg::spec::Schema,
    spec: &PartitionSpec,
) -> Result<Descriptor> {
    let Layout { identities, bucket } = partition_layout(current, spec)?;
    let mut builder = descriptor
        .to_builder()
        .schema(table_schema(current)?)
        .partitioned_by(identities.iter().map(|s| s.to_string()));
    if let Some((column, n)) = bucket {
        builder = builder.bucket_keys([column.to_string()]).bucket_count(n);
    }
    builder.build().map_err(Error::other)
}

struct Layout<'a> {
    identities: Vec<&'a str>,
    bucket: Option<(&'a str, u32)>,
}

fn partition_layout<'a>(
    current: &'a iceberg::spec::Schema,
    spec: &PartitionSpec,
) -> Result<Layout<'a>> {
    let mut identities = Vec::new();
    let mut bucket: Option<(&str, u32)> = None;
    for field in spec.fields() {
        let source = current
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.id == field.source_id)
            .map(|f| f.name.as_str())
            .ok_or_else(|| {
                Error::invalid(format!(
                    "attached table partition field `{}` has no top-level source column",
                    field.name
                ))
            })?;
        match field.transform {
            Transform::Identity => identities.push(source),
            Transform::Bucket(n) => {
                if bucket.is_some() {
                    return Err(Error::invalid(
                        "attached table has more than one bucket partition field",
                    ));
                }
                bucket = Some((source, n));
            }
            ref other => {
                return Err(Error::invalid(format!(
                    "attached table partition transform {other} is not supported"
                )));
            }
        }
    }

    Ok(Layout { identities, bucket })
}

fn check_attached_schema(
    given: &mink_table::Schema,
    current: &iceberg::spec::Schema,
    version: FormatVersion,
) -> Result<()> {
    let fields = current.as_struct().fields();
    if given.columns().len() != fields.len() {
        return Err(Error::invalid(format!(
            "attached table has {} columns, descriptor has {}",
            fields.len(),
            given.columns().len()
        )));
    }
    for (column, field) in given.columns().iter().zip(fields) {
        if column.name() != field.name {
            return Err(Error::invalid(format!(
                "attached table column `{}` does not match descriptor column `{}`",
                field.name,
                column.name()
            )));
        }
        let expected = to_iceberg(column.data_type(), &mut Ids::added(0), version);
        if !same_type(&expected, &field.field_type) {
            return Err(Error::invalid(format!(
                "attached table column `{}` is {}, descriptor says {}",
                field.name, field.field_type, expected
            )));
        }
        if field.required && column.data_type().is_nullable() {
            return Err(Error::invalid(format!(
                "attached table column `{}` is required, descriptor says nullable",
                field.name
            )));
        }
    }
    let key: Vec<&str> = current
        .identifier_field_ids()
        .filter_map(|id| current.field_by_id(id).map(|f| f.name.as_str()))
        .collect();
    let given_key: Vec<&str> = given
        .primary_key()
        .map(|k| k.columns().iter().map(String::as_str).collect())
        .unwrap_or_default();
    let mut key_sorted = key.clone();
    key_sorted.sort_unstable();
    let mut given_sorted = given_key.clone();
    given_sorted.sort_unstable();
    if key_sorted != given_sorted {
        return Err(Error::invalid(format!(
            "attached table identifier fields {key:?} do not match primary key {given_key:?}"
        )));
    }

    Ok(())
}

fn same_type(a: &Type, b: &Type) -> bool {
    match (a, b) {
        (Type::Primitive(x), Type::Primitive(y)) => x == y,
        (Type::List(x), Type::List(y)) => {
            x.element_field.required == y.element_field.required
                && same_type(&x.element_field.field_type, &y.element_field.field_type)
        }
        (Type::Map(x), Type::Map(y)) => {
            same_type(&x.key_field.field_type, &y.key_field.field_type)
                && x.value_field.required == y.value_field.required
                && same_type(&x.value_field.field_type, &y.value_field.field_type)
        }
        (Type::Struct(x), Type::Struct(y)) => {
            x.fields().len() == y.fields().len()
                && x.fields().iter().zip(y.fields()).all(|(f, g)| {
                    f.name == g.name
                        && f.required == g.required
                        && same_type(&f.field_type, &g.field_type)
                })
        }
        _ => false,
    }
}

fn check_attached_partitioning(
    descriptor: &Descriptor,
    current: &iceberg::spec::Schema,
    spec: &PartitionSpec,
) -> Result<()> {
    let bucket_count = descriptor
        .bucket_count()
        .ok_or_else(|| Error::invalid("Bucket count (bucket.num) must be set"))?;
    let Layout { identities, bucket } = partition_layout(current, spec)?;
    if identities != descriptor.partition_keys() {
        return Err(Error::invalid(format!(
            "attached table is partitioned by {identities:?}, descriptor by {:?}",
            descriptor.partition_keys()
        )));
    }
    let keys = descriptor.bucket_keys();
    match (bucket, keys) {
        (None, []) => {}
        (Some((column, n)), [key]) if column == key && n == bucket_count => {}
        (Some((column, n)), _) => {
            return Err(Error::invalid(format!(
                "attached table buckets by `{column}` into {n}; descriptor needs the same bucket \
                 key and bucket.num, got {keys:?} and {bucket_count}"
            )));
        }
        (None, _) => {
            return Err(Error::invalid(format!(
                "descriptor has bucket key {keys:?} but the attached table is not bucketed"
            )));
        }
    }
    if descriptor.has_primary_key() && bucket.is_none() {
        return Err(Error::invalid(
            "primary key tables need a bucket partition field in the attached table",
        ));
    }

    Ok(())
}

#[async_trait]
impl LakeCatalog for Catalog {
    fn format(&self) -> Option<LakeFormat> {
        Some(LakeFormat::Iceberg)
    }

    async fn create_table(
        &self,
        path: &Path,
        descriptor: &Descriptor,
    ) -> Result<lake::Created, lake::Error> {
        if descriptor.options().lake_attach {
            return self
                .attach(path, descriptor)
                .await
                .map_err(|e| e.into_catalog(path));
        }
        self.create(path, descriptor)
            .await
            .map(|()| lake::Created::FRESH)
            .map_err(|e| e.into_catalog(path))
    }

    async fn alter_table(
        &self,
        path: &Path,
        from: &Descriptor,
        to: &Descriptor,
    ) -> Result<(), lake::Error> {
        self.alter(path, from, to)
            .await
            .map_err(|e| e.into_catalog(path))
    }
}

#[async_trait]
impl Factory for Catalog {
    type WriteResult = WriteResult;
    type Committable = Committable;

    async fn create_writer(
        &self,
        context: WriterContext,
    ) -> Result<Box<dyn crate::writer::Writer<WriteResult>>> {
        let table = self
            .target
            .load(&Self::identifier(&context.path))
            .await
            .map_err(|e| {
                Error::Other(format!(
                    "Failed to get table {} in Iceberg: {e}",
                    context.path
                ))
            })?;

        Ok(Box::new(
            crate::iceberg::writer::Writer::open(&table, &context).await?,
        ))
    }

    async fn create_committer(
        &self,
        context: CommitterContext,
    ) -> Result<Box<dyn crate::committer::Committer<WriteResult, Committable>>> {
        Ok(Box::new(
            crate::iceberg::committer::Committer::open(self.target.clone(), &context.path).await?,
        ))
    }
}
