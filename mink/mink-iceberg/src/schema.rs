use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, FieldRef, Fields, SchemaRef};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::Schema;
use mink_schema::{Decoders, HEADERS, TIMESTAMP, binary_schema};

use crate::Error;

pub enum FileSchema {
    Binary,
    Typed(Box<TypedSchema>),
}

pub struct TypedSchema {
    schema: Schema,
    arrow: SchemaRef,
    decoders: Decoders,
}

pub(crate) struct BinaryArrow {
    pub(crate) schema: SchemaRef,
    pub(crate) timezone: Option<Arc<str>>,
    pub(crate) header_element: FieldRef,
    pub(crate) header_fields: Fields,
}

pub(crate) static BINARY_ARROW: LazyLock<BinaryArrow> = LazyLock::new(|| {
    let schema =
        Arc::new(schema_to_arrow_schema(binary_schema()).expect("binary schema converts to arrow"));
    let field = |name: &str| {
        schema
            .field_with_name(name)
            .expect("binary schema field")
            .data_type()
            .clone()
    };

    let DataType::Timestamp(_, timezone) = field(TIMESTAMP) else {
        panic!("binary schema _kafka_timestamp is a timestamp")
    };
    let DataType::List(header_element) = field(HEADERS) else {
        panic!("binary schema _kafka_headers is a list")
    };
    let DataType::Struct(header_fields) = header_element.data_type().clone() else {
        panic!("binary schema header is a struct")
    };

    BinaryArrow {
        schema,
        timezone,
        header_element,
        header_fields,
    }
});

impl FileSchema {
    pub fn id(&self) -> i32 {
        self.schema().schema_id()
    }

    pub fn schema(&self) -> &Schema {
        match self {
            FileSchema::Binary => binary_schema(),
            FileSchema::Typed(typed) => &typed.schema,
        }
    }

    pub(crate) fn arrow(&self) -> &SchemaRef {
        match self {
            FileSchema::Binary => &BINARY_ARROW.schema,
            FileSchema::Typed(typed) => &typed.arrow,
        }
    }
}

impl TypedSchema {
    pub fn new(schema: Schema, decoders: Decoders) -> Result<TypedSchema, Error> {
        let arrow = Arc::new(schema_to_arrow_schema(&schema)?);

        Ok(TypedSchema {
            schema,
            arrow,
            decoders,
        })
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn decoders(&self) -> &Decoders {
        &self.decoders
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_binary_schema_has_one_arrow_column_per_field() {
        assert_eq!(BINARY_ARROW.schema.fields().len(), 6);
    }
}
