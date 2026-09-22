//! Error type for invalid type parameters, field definitions, parsing and Arrow conversion.

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Error {
    #[error("length must be between 1 and {max}", max = i32::MAX)]
    Length,
    #[error("precision {0} is out of range 0..=9")]
    Precision(u8),
    #[error("decimal precision {0} is out of range 1..=38")]
    DecimalPrecision(u8),
    #[error("decimal scale {scale} exceeds precision {precision}")]
    DecimalScale { precision: u8, scale: u8 },
    #[error("field name must contain a non-whitespace character")]
    BlankFieldName,
    #[error("duplicate field name `{0}`")]
    DuplicateFieldName(String),
    #[error("field index {index} is out of range for {count} fields")]
    FieldIndex { index: usize, count: usize },
    #[error("could not parse type at position {position}: {message}")]
    Parse { position: usize, message: String },
    #[error("unsupported arrow type {0}")]
    UnsupportedArrow(String),
    #[error("invalid field id metadata `{0}`")]
    FieldId(String),
}
