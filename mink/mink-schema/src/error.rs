use mink_core::Part;

use crate::registry::{SchemaId, SchemaKey};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("registry schema {key} cannot be read: {message}")]
    PayloadSchema { key: SchemaKey, message: String },
    #[error("the {} of a record names {schema_id}, which its topic has not merged", part.name())]
    UnmergedSchema { part: Part, schema_id: SchemaId },
}
