use mink_core::Part;

use crate::registry::{SchemaKey, SchemaRef};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("registry schema {key} cannot be read: {message}")]
    PayloadSchema { key: SchemaKey, message: String },
    #[error("the {} of a record names {schema_ref}, which its topic has not merged", part.name())]
    UnmergedSchema { part: Part, schema_ref: SchemaRef },
}
