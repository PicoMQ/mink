use std::sync::LazyLock;

use iceberg::spec::{ListType, NestedField, PrimitiveType, Schema, StructType, Type};

pub const BINARY_SCHEMA_ID: i32 = 0;
pub const PARTITION_ID: i32 = 1;
pub const OFFSET_ID: i32 = 2;
pub const TIMESTAMP_ID: i32 = 3;
pub const KEY_ID: i32 = 4;
pub const HEADERS_ID: i32 = 5;
const HEADER_ID: i32 = 6;
const HEADER_KEY_ID: i32 = 7;
const HEADER_VALUE_ID: i32 = 8;
pub const VALUE_ID: i32 = 9;

pub const PARTITION: &str = "_kafka_partition";
pub(crate) const OFFSET: &str = "_kafka_offset";
pub const TIMESTAMP: &str = "_kafka_timestamp";
pub(crate) const KEY: &str = "_kafka_key";
pub const HEADERS: &str = "_kafka_headers";
pub(crate) const VALUE: &str = "_kafka_value";
pub(crate) const DECODED_KEY: &str = "_kafka_decoded_key";
pub(crate) const RESERVED_NAMES: [&str; 14] = [
    PARTITION,
    OFFSET,
    TIMESTAMP,
    KEY,
    HEADERS,
    VALUE,
    DECODED_KEY,
    "_file",
    "_pos",
    "_deleted",
    "_spec_id",
    "_partition",
    "_row_id",
    "_last_updated_sequence_number",
];

static BINARY_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    let header = StructType::new(vec![
        NestedField::required(HEADER_KEY_ID, "key", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(
            HEADER_VALUE_ID,
            "value",
            Type::Primitive(PrimitiveType::Binary),
        )
        .into(),
    ]);
    let headers =
        ListType::new(NestedField::list_element(HEADER_ID, Type::Struct(header), true).into());

    Schema::builder()
        .with_schema_id(BINARY_SCHEMA_ID)
        .with_fields([
            NestedField::required(PARTITION_ID, PARTITION, Type::Primitive(PrimitiveType::Int))
                .into(),
            NestedField::required(OFFSET_ID, OFFSET, Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(
                TIMESTAMP_ID,
                TIMESTAMP,
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .into(),
            NestedField::optional(KEY_ID, KEY, Type::Primitive(PrimitiveType::Binary)).into(),
            NestedField::required(HEADERS_ID, HEADERS, Type::List(headers)).into(),
            NestedField::optional(VALUE_ID, VALUE, Type::Primitive(PrimitiveType::Binary)).into(),
        ])
        .build()
        .expect("binary schema has unique field ids")
});

pub fn binary_schema() -> &'static Schema {
    &BINARY_SCHEMA
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;

    #[test]
    fn binary_schema_matches_the_fixture() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../conformance/fixtures/schema/binary.json");
        let fixture: Schema = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();

        let schema = binary_schema();

        assert_eq!(schema, &fixture);
        assert_eq!(schema.highest_field_id(), VALUE_ID);
    }
}
