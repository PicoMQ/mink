use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::Schema;
use mink_core::{Header, Part};

use crate::Error;
use crate::columns::{ColumnValue, to_columns};
use crate::merge::FieldIds;
use crate::payload::PayloadSchema;
use crate::wire::{PartSchemaRef, read_header};

#[derive(Default)]
pub struct Decoders {
    by_ref: HashMap<PartSchemaRef, PartDecoder>,
}

enum PartDecoder {
    Typed {
        schema: Arc<PayloadSchema>,
        field_ids: FieldIds,
    },
    Raw,
}

pub enum PartValues<'a> {
    Null,
    Raw(&'a [u8]),
    Columns(HashMap<i32, ColumnValue>),
}

impl Decoders {
    pub fn insert_typed(
        &mut self,
        schema_ref: PartSchemaRef,
        schema: Arc<PayloadSchema>,
        field_ids: FieldIds,
    ) {
        self.by_ref
            .insert(schema_ref, PartDecoder::Typed { schema, field_ids });
    }

    pub fn insert_raw(&mut self, schema_ref: PartSchemaRef) {
        self.by_ref.insert(schema_ref, PartDecoder::Raw);
    }

    pub fn values<'a>(
        &self,
        part: Part,
        bytes: Option<&'a [u8]>,
        headers: &[Header],
        table: &Schema,
    ) -> Result<PartValues<'a>, Error> {
        let Some(bytes) = bytes else {
            return Ok(PartValues::Null);
        };
        let Some(header) = read_header(part, bytes, headers) else {
            return Ok(PartValues::Raw(bytes));
        };

        let schema_ref = PartSchemaRef::new(part, header.schema_ref);
        let values = match self.by_ref.get(&schema_ref) {
            Some(PartDecoder::Typed { schema, field_ids }) => schema
                .decode(&header.payload)
                .and_then(|value| to_columns(value, field_ids, table))
                .map_or(PartValues::Raw(bytes), PartValues::Columns),
            Some(PartDecoder::Raw) => PartValues::Raw(bytes),
            None => {
                return Err(Error::UnmergedSchema {
                    part,
                    schema_ref: header.schema_ref,
                });
            }
        };

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    use super::*;
    use crate::binary::binary_schema;
    use crate::registry::{RegistrySchema, SchemaFormat, SchemaKey, SchemaRef, SchemaSet};
    use crate::value::PayloadValue;

    fn guid_header() -> Header {
        let value: Vec<u8> = [1].into_iter().chain([7; 16]).collect();

        Header::new("__value_schema_id", Some(Bytes::from(value)))
    }

    fn table() -> Schema {
        let int = NestedField::optional(10, "value", Type::Primitive(PrimitiveType::Int));
        let fields = binary_schema()
            .as_struct()
            .fields()
            .iter()
            .cloned()
            .chain([int.into()]);

        Schema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .unwrap()
    }

    #[test]
    fn undecodable_parts_keep_their_raw_bytes() {
        let int = RegistrySchema::new(
            SchemaKey::Confluent(1),
            SchemaFormat::Avro,
            "\"int\"",
            Vec::new(),
        );
        let schema = Arc::new(PayloadSchema::parse(&SchemaSet::new(int, Vec::new())).unwrap());
        let field_ids = FieldIds::from_iter([(String::new(), 10)]);
        let mut decoders = Decoders::default();
        decoders.insert_typed(
            PartSchemaRef::new(Part::Value, SchemaRef::ConfluentId(1)),
            schema,
            field_ids,
        );
        decoders.insert_raw(PartSchemaRef::new(Part::Value, SchemaRef::ConfluentId(2)));
        let table = table();

        let rejected: &'static [u8] = &[0, 0, 0, 0, 2, 2];
        let trailing: &'static [u8] = &[0, 0, 0, 0, 1, 2, 2];
        let decoded: &'static [u8] = &[0, 0, 0, 0, 1, 2];
        let values =
            |bytes: Option<&'static [u8]>| decoders.values(Part::Value, bytes, &[], &table);

        assert!(matches!(values(None), Ok(PartValues::Null)));
        assert!(matches!(values(Some(rejected)), Ok(PartValues::Raw(raw)) if raw == rejected));
        assert!(matches!(values(Some(trailing)), Ok(PartValues::Raw(raw)) if raw == trailing));
        assert!(matches!(
            values(Some(decoded)),
            Ok(PartValues::Columns(columns))
                if columns.get(&10) == Some(&ColumnValue::Scalar(PayloadValue::Int(1)))
        ));
    }

    #[test]
    fn an_unmerged_schema_fails_the_build() {
        let decoders = Decoders::default();
        let table = binary_schema();

        let by_id = decoders.values(Part::Key, Some(&[0, 0, 0, 0, 9, 2]), &[], table);
        let by_guid = decoders.values(Part::Value, Some(&[2]), &[guid_header()], table);

        assert!(matches!(
            by_id,
            Err(Error::UnmergedSchema {
                part: Part::Key,
                schema_ref: SchemaRef::ConfluentId(9)
            })
        ));
        assert!(matches!(
            by_guid,
            Err(Error::UnmergedSchema {
                part: Part::Value,
                ..
            })
        ));
    }
}
