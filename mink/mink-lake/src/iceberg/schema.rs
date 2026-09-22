//! Derives the Iceberg schema, partition spec and properties from a table descriptor, evolves them,
//! and maps Iceberg schemas back to table schemas.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{
    FormatVersion, ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema,
    SortOrder, StructType, Transform, Type, UnboundPartitionSpec,
};
use mink_table::{Column, Descriptor, Path, PrimaryKey};
use mink_types::{DataType, Decimal, Field, FieldId, Fields, Kind, Length, Precision};

use crate::error::{Error, Result};

pub const FORMAT_VERSION_OPTION: &str = "iceberg.format-version";

const RESERVED_PROPERTIES: [&str; 3] =
    ["write.delete.mode", "write.update.mode", "write.merge.mode"];
const MERGE_ON_READ: &str = "merge-on-read";

const OPTION_PREFIX: &str = "mink.";
const ICEBERG_PREFIX: &str = "iceberg.";

#[derive(Debug, Clone)]
pub struct Spec {
    pub schema: Schema,
    pub partition_spec: UnboundPartitionSpec,
    pub sort_order: SortOrder,
    pub properties: HashMap<String, String>,
    pub format_version: FormatVersion,
}

impl Spec {
    pub fn new(path: &Path, descriptor: &Descriptor) -> Result<Self> {
        let format_version = format_version(descriptor)?;
        let schema = schema(descriptor, format_version)?;
        let partition_spec = partition_spec(descriptor, &schema)?;
        let properties = table_properties(descriptor);
        tracing::debug!(table = %path, pk = descriptor.has_primary_key(), version = %format_version, "derived iceberg table spec");

        Ok(Spec {
            schema,
            partition_spec,
            sort_order: SortOrder::unsorted_order(),
            properties,
            format_version,
        })
    }
}

pub(crate) fn format_version(descriptor: &Descriptor) -> Result<FormatVersion> {
    match descriptor
        .custom()
        .get(FORMAT_VERSION_OPTION)
        .map(String::as_str)
    {
        None | Some("2") => Ok(FormatVersion::V2),
        Some("3") => Ok(FormatVersion::V3),
        Some(other) => Err(Error::invalid(format!(
            "{FORMAT_VERSION_OPTION} must be 2 or 3, got `{other}`"
        ))),
    }
}

fn schema(descriptor: &Descriptor, version: FormatVersion) -> Result<Schema> {
    let columns = descriptor.schema().columns();
    let mut ids = Ids::new(columns.len());
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        fields.push(Arc::new(column_field(column, &mut ids, version)));
    }

    let mut builder = Schema::builder().with_fields(fields.iter().cloned());
    if descriptor.has_primary_key() {
        let identifiers = descriptor
            .schema()
            .primary_key_indexes()
            .into_iter()
            .map(|index| fields[index].id);
        builder = builder.with_identifier_field_ids(identifiers);
    }

    Ok(builder.build()?)
}

pub(crate) fn evolve_schema(
    current: &Schema,
    last_column_id: i32,
    from: &Descriptor,
    to: &Descriptor,
) -> Result<Option<(Schema, i32)>> {
    let version = format_version(to)?;
    let before: HashMap<FieldId, &Column> = from
        .schema()
        .columns()
        .iter()
        .filter_map(|c| c.id().map(|id| (id, c)))
        .collect();
    let after: Vec<FieldId> = to
        .schema()
        .columns()
        .iter()
        .filter_map(Column::id)
        .collect();

    let mut fields: Vec<NestedFieldRef> = Vec::with_capacity(after.len());
    let mut next = last_column_id + 1;
    let mut changed = before.keys().any(|id| !after.contains(id));
    for column in to.schema().columns() {
        let id = column
            .id()
            .ok_or_else(|| Error::invalid("column without a field id"))?;
        let Some(previous) = before.get(&id) else {
            let mut ids = Ids::added(next);
            fields.push(Arc::new(column_field(column, &mut ids, version)));
            next = ids.end();
            changed = true;
            continue;
        };
        let existing = current.field_by_name(previous.name()).ok_or_else(|| {
            Error::invalid(format!(
                "Iceberg table has no column `{}` to evolve",
                previous.name()
            ))
        })?;
        let mut evolved = existing.as_ref().clone();
        if previous.name() != column.name() {
            evolved.name = column.name().to_string();
            changed = true;
        }
        if previous.data_type() != column.data_type() {
            let target = to_iceberg(column.data_type(), &mut Ids::added(0), version);
            let retyped = retype(&existing.field_type, &target, column.name())?;
            if retyped != *existing.field_type {
                evolved.field_type = Box::new(retyped);
            }
            let required = !column.data_type().is_nullable();
            if required && !existing.required {
                return Err(Error::invalid(format!(
                    "column `{}` cannot become required",
                    column.name()
                )));
            }
            evolved.required = required;
            changed = true;
        }
        if previous.description() != column.description() {
            evolved.doc = column.description().map(str::to_string);
            changed = true;
        }
        fields.push(Arc::new(evolved));
    }
    if !changed {
        return Ok(None);
    }

    let schema = Schema::builder()
        .with_schema_id(current.schema_id() + 1)
        .with_fields(fields)
        .with_identifier_field_ids(current.identifier_field_ids())
        .build()?;

    Ok(Some((schema, next - 1)))
}

fn retype(existing: &Type, to: &Type, column: &str) -> Result<Type> {
    let cannot = || {
        Error::invalid(format!(
            "Iceberg cannot promote column `{column}` from {existing} to {to}"
        ))
    };
    Ok(match (existing, to) {
        (Type::Primitive(from), Type::Primitive(target)) => {
            if from == target || primitive_promotable(from, target) {
                to.clone()
            } else {
                return Err(cannot());
            }
        }
        (Type::List(from), Type::List(target)) => {
            let element = &from.element_field;
            let mut retyped = element.as_ref().clone();
            retyped.field_type = Box::new(retype(
                &element.field_type,
                &target.element_field.field_type,
                column,
            )?);
            retyped.required = target.element_field.required;
            Type::List(ListType::new(Arc::new(retyped)))
        }
        (Type::Map(from), Type::Map(target)) => {
            if from.key_field.field_type != target.key_field.field_type {
                return Err(cannot());
            }
            let mut value = from.value_field.as_ref().clone();
            value.field_type = Box::new(retype(
                &from.value_field.field_type,
                &target.value_field.field_type,
                column,
            )?);
            value.required = target.value_field.required;
            Type::Map(MapType::new(Arc::clone(&from.key_field), Arc::new(value)))
        }
        (Type::Struct(from), Type::Struct(target))
            if from.fields().len() == target.fields().len() =>
        {
            let mut fields = Vec::with_capacity(from.fields().len());
            for (existing, wanted) in from.fields().iter().zip(target.fields()) {
                let mut retyped = existing.as_ref().clone();
                retyped.name = wanted.name.clone();
                retyped.doc = wanted.doc.clone();
                retyped.required = wanted.required;
                retyped.field_type =
                    Box::new(retype(&existing.field_type, &wanted.field_type, column)?);
                fields.push(Arc::new(retyped));
            }
            Type::Struct(StructType::new(fields))
        }
        _ => return Err(cannot()),
    })
}

fn primitive_promotable(from: &PrimitiveType, to: &PrimitiveType) -> bool {
    matches!(
        (from, to),
        (PrimitiveType::Int, PrimitiveType::Long) | (PrimitiveType::Float, PrimitiveType::Double)
    ) || matches!(
        (from, to),
        (
            PrimitiveType::Decimal { precision: p, scale: s },
            PrimitiveType::Decimal { precision: q, scale: t }
        ) if s == t && q > p
    )
}

pub(crate) fn to_iceberg(data_type: &DataType, ids: &mut Ids, version: FormatVersion) -> Type {
    let nanos = |p: &Precision| version == FormatVersion::V3 && p.get() > Precision::MICROS.get();
    match data_type.kind() {
        Kind::Boolean => Type::Primitive(PrimitiveType::Boolean),
        Kind::TinyInt | Kind::SmallInt | Kind::Int => Type::Primitive(PrimitiveType::Int),
        Kind::BigInt => Type::Primitive(PrimitiveType::Long),
        Kind::Float => Type::Primitive(PrimitiveType::Float),
        Kind::Double => Type::Primitive(PrimitiveType::Double),
        Kind::Char(_) | Kind::String => Type::Primitive(PrimitiveType::String),
        Kind::Binary(_) | Kind::Bytes => Type::Primitive(PrimitiveType::Binary),
        Kind::Decimal(decimal) => Type::Primitive(PrimitiveType::Decimal {
            precision: u32::from(decimal.precision()),
            scale: u32::from(decimal.scale()),
        }),
        Kind::Date => Type::Primitive(PrimitiveType::Date),
        Kind::Time(_) => Type::Primitive(PrimitiveType::Time),
        Kind::Timestamp(p) if nanos(p) => Type::Primitive(PrimitiveType::TimestampNs),
        Kind::Timestamp(_) => Type::Primitive(PrimitiveType::Timestamp),
        Kind::TimestampLtz(p) if nanos(p) => Type::Primitive(PrimitiveType::TimestamptzNs),
        Kind::TimestampLtz(_) => Type::Primitive(PrimitiveType::Timestamptz),
        Kind::Array(element) => {
            let element_type = to_iceberg(element, ids, version);
            let id = ids.next();
            Type::List(ListType::new(Arc::new(NestedField::list_element(
                id,
                element_type,
                !element.is_nullable(),
            ))))
        }
        Kind::Map { key, value } => {
            let key_id = ids.next();
            let value_id = ids.next();
            let key_type = to_iceberg(key, ids, version);
            let value_type = to_iceberg(value, ids, version);
            Type::Map(MapType::new(
                Arc::new(NestedField::map_key_element(key_id, key_type)),
                Arc::new(NestedField::map_value_element(
                    value_id,
                    value_type,
                    !value.is_nullable(),
                )),
            ))
        }
        Kind::Row(row) => {
            let mut fields = Vec::with_capacity(row.len());
            for field in row.iter() {
                let field_type = to_iceberg(field.data_type(), ids, version);
                let mut nested = NestedField::new(
                    ids.next(),
                    field.name(),
                    field_type,
                    !field.data_type().is_nullable(),
                );
                if let Some(description) = field.description() {
                    nested = nested.with_doc(description);
                }
                fields.push(Arc::new(nested));
            }
            Type::Struct(StructType::new(fields))
        }
    }
}

pub fn from_iceberg(field_type: &Type, nullable: bool) -> Result<DataType> {
    let kind = match field_type {
        Type::Primitive(primitive) => match primitive {
            PrimitiveType::Boolean => Kind::Boolean,
            PrimitiveType::Int => Kind::Int,
            PrimitiveType::Long => Kind::BigInt,
            PrimitiveType::Float => Kind::Float,
            PrimitiveType::Double => Kind::Double,
            PrimitiveType::Decimal { precision, scale } => Kind::Decimal(
                Decimal::new(
                    u8::try_from(*precision).map_err(Error::other)?,
                    u8::try_from(*scale).map_err(Error::other)?,
                )
                .map_err(Error::other)?,
            ),
            PrimitiveType::Date => Kind::Date,
            PrimitiveType::Time => Kind::Time(Precision::MICROS),
            PrimitiveType::Timestamp => Kind::Timestamp(Precision::MICROS),
            PrimitiveType::Timestamptz => Kind::TimestampLtz(Precision::MICROS),
            PrimitiveType::TimestampNs => Kind::Timestamp(Precision::NANOS),
            PrimitiveType::TimestamptzNs => Kind::TimestampLtz(Precision::NANOS),
            PrimitiveType::String => Kind::String,
            PrimitiveType::Uuid => Kind::Binary(Length::new(16).map_err(Error::other)?),
            PrimitiveType::Fixed(n) => Kind::Binary(
                Length::new(u32::try_from(*n).map_err(Error::other)?).map_err(Error::other)?,
            ),
            PrimitiveType::Binary => Kind::Bytes,
        },
        Type::List(list) => Kind::Array(Box::new(from_iceberg(
            &list.element_field.field_type,
            !list.element_field.required,
        )?)),
        Type::Map(map) => Kind::Map {
            key: Box::new(from_iceberg(&map.key_field.field_type, false)?),
            value: Box::new(from_iceberg(
                &map.value_field.field_type,
                !map.value_field.required,
            )?),
        },
        Type::Struct(row) => {
            let fields = row
                .fields()
                .iter()
                .map(|f| {
                    let mut field = Field::new(&f.name, from_iceberg(&f.field_type, !f.required)?)
                        .map_err(Error::other)?;
                    if let Some(doc) = &f.doc {
                        field = field.with_description(doc);
                    }
                    Ok(field)
                })
                .collect::<Result<Vec<_>>>()?;
            Kind::Row(Fields::new(fields).map_err(Error::other)?)
        }
    };

    Ok(DataType::new(kind, nullable))
}

pub fn table_schema(schema: &Schema) -> Result<mink_table::Schema> {
    let mut builder = mink_table::Schema::builder();
    for field in schema.as_struct().fields() {
        let mut column = Column::new(
            &field.name,
            from_iceberg(&field.field_type, !field.required)?,
        )
        .map_err(Error::other)?;
        if let Some(doc) = &field.doc {
            column = column.with_description(doc);
        }
        builder = builder.column(column);
    }
    let identifiers: Vec<i32> = schema.identifier_field_ids().collect();
    for id in &identifiers {
        if !schema.as_struct().fields().iter().any(|f| f.id == *id) {
            return Err(Error::invalid(format!(
                "identifier field {id} is not a top-level column"
            )));
        }
    }
    let key: Vec<String> = schema
        .as_struct()
        .fields()
        .iter()
        .filter(|f| identifiers.contains(&f.id))
        .map(|f| f.name.clone())
        .collect();
    if !key.is_empty() {
        builder = builder.primary_key(PrimaryKey::new(key).map_err(Error::other)?);
    }

    builder.build().map_err(Error::other)
}

#[derive(Debug)]
pub(crate) struct Ids {
    top_level: i32,
    nested: i32,
}

impl Ids {
    fn new(top_level_fields: usize) -> Self {
        Ids {
            top_level: 0,
            nested: i32::try_from(top_level_fields).expect("field count fits i32"),
        }
    }

    pub(crate) fn added(first: i32) -> Self {
        Ids {
            top_level: first,
            nested: first + 1,
        }
    }

    fn end(&self) -> i32 {
        self.nested.max(self.top_level)
    }

    fn top_level(&mut self) -> i32 {
        let id = self.top_level;
        self.top_level += 1;
        id
    }

    fn next(&mut self) -> i32 {
        let id = self.nested;
        self.nested += 1;
        id
    }
}

fn partition_spec(descriptor: &Descriptor, schema: &Schema) -> Result<UnboundPartitionSpec> {
    let bucket_keys = descriptor.bucket_keys();
    let bucket_count = descriptor
        .bucket_count()
        .ok_or_else(|| Error::invalid("Bucket count (bucket.num) must be set"))?;
    if bucket_keys.len() > 1 {
        return Err(Error::invalid(
            "Only one bucket key is supported for Iceberg at the moment",
        ));
    }
    if bucket_keys.is_empty() && descriptor.has_primary_key() {
        return Err(Error::invalid(
            "Bucket key must be set for primary key Iceberg tables",
        ));
    }

    let mut builder = UnboundPartitionSpec::builder().with_spec_id(0);
    for key in descriptor.partition_keys() {
        let field = field(schema, key)?;
        if field.field_type.as_ref() != &Type::Primitive(PrimitiveType::String) {
            return Err(Error::invalid(format!(
                "Iceberg partition keys must be strings; column `{key}` is not"
            )));
        }
        builder = builder.add_partition_field(field.id, key, Transform::Identity)?;
    }
    if let Some(key) = bucket_keys.first() {
        builder = builder.add_partition_field(
            field(schema, key)?.id,
            format!("{key}_bucket"),
            Transform::Bucket(bucket_count),
        )?;
    }

    Ok(builder.build())
}

pub(crate) fn table_properties(descriptor: &Descriptor) -> HashMap<String, String> {
    let mut properties = HashMap::new();
    if descriptor.has_primary_key() {
        for key in RESERVED_PROPERTIES {
            properties.insert(key.to_string(), MERGE_ON_READ.to_string());
        }
    }
    for (key, value) in table_options(descriptor) {
        set_property(&mut properties, &key, value);
    }
    for (key, value) in descriptor.custom() {
        if key != FORMAT_VERSION_OPTION {
            set_property(&mut properties, key, value.clone());
        }
    }

    properties
}

fn property_key(key: &str) -> String {
    match key.strip_prefix(ICEBERG_PREFIX) {
        Some(rest) => rest.to_string(),
        None => format!("{OPTION_PREFIX}{key}"),
    }
}

fn column_field(column: &Column, ids: &mut Ids, version: FormatVersion) -> NestedField {
    let id = ids.top_level();
    let field_type = to_iceberg(column.data_type(), ids, version);
    let mut field = NestedField::new(
        id,
        column.name(),
        field_type,
        !column.data_type().is_nullable(),
    );
    if let Some(description) = column.description() {
        field = field.with_doc(description);
    }

    field
}

fn set_property(properties: &mut HashMap<String, String>, key: &str, value: String) {
    properties.insert(property_key(key), value);
}

fn table_options(descriptor: &Descriptor) -> Vec<(String, String)> {
    let options = descriptor.options();
    let mut out = vec![
        ("table.datalake.enabled".into(), "true".into()),
        ("table.log.format".into(), options.log_format.to_string()),
    ];
    if let Some(lake) = options.lake {
        out.push(("table.datalake.format".into(), lake.to_string()));
        out.push((
            "table.datalake.freshness".into(),
            format!("{}ms", options.lake_freshness.as_millis()),
        ));
    }
    if descriptor.has_primary_key() {
        out.push(("table.kv.format".into(), options.kv_format.to_string()));
    }
    if let Some(engine) = &options.merge_engine {
        out.push(("table.merge-engine".into(), engine.to_string()));
    }
    if let Some(auto) = &options.auto_partition {
        out.push(("table.auto-partition.enabled".into(), "true".into()));
        out.push((
            "table.auto-partition.time-unit".into(),
            auto.time_unit.to_string(),
        ));
        out.push((
            "table.auto-partition.time-zone".into(),
            auto.time_zone.clone(),
        ));
        if let Some(key) = &auto.key {
            out.push(("table.auto-partition.key".into(), key.clone()));
        }
    }

    out
}

fn field<'a>(schema: &'a Schema, name: &str) -> Result<&'a NestedField> {
    schema
        .field_by_name(name)
        .map(|field| field.as_ref())
        .ok_or_else(|| Error::invalid(format!("Cannot find field '{name}' in the schema")))
}
