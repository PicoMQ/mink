use mink_core::Part;

use crate::registry::SchemaRef;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("registry schema {id} cannot be read: {message}")]
    PayloadSchema { id: i32, message: String },
    #[error("the {} of a record names {schema_ref}, which its topic has not merged", part.name())]
    UnmergedSchema { part: Part, schema_ref: SchemaRef },
}
