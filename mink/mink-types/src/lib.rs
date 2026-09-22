//! The logical type system: data types with nullability, fields, and the conversions to and from
//! Arrow schemas, JSON, and the SQL-style textual form.

mod arrow;
mod datatype;
mod decimal;
mod error;
mod family;
mod field;
mod fields;
mod format;
mod json;
mod length;
mod parse;
mod precision;
mod root;

pub use arrow::FIELD_ID_METADATA;
pub use datatype::{DataType, Kind};
pub use decimal::Decimal;
pub use error::Error;
pub use family::Family;
pub use field::{Field, FieldId};
pub use fields::Fields;
pub use length::Length;
pub use precision::Precision;
pub use root::Root;
