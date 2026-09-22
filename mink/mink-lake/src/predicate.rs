//! Filters a lake read can push down: a column compared with a literal, membership in a literal set,
//! null tests, and their boolean combinations. Literals are Arrow arrays so any engine can produce them
//! and any lake format can bind them.

use std::fmt;

use arrow_array::ArrayRef;
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, TimeUnit};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compare {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl Compare {
    pub fn flip(self) -> Self {
        match self {
            Compare::Eq => Compare::Eq,
            Compare::NotEq => Compare::NotEq,
            Compare::Lt => Compare::Gt,
            Compare::LtEq => Compare::GtEq,
            Compare::Gt => Compare::Lt,
            Compare::GtEq => Compare::LtEq,
        }
    }
}

impl fmt::Display for Compare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Compare::Eq => "=",
            Compare::NotEq => "<>",
            Compare::Lt => "<",
            Compare::LtEq => "<=",
            Compare::Gt => ">",
            Compare::GtEq => ">=",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Compare {
        column: String,
        op: Compare,
        value: ArrayRef,
    },
    In {
        column: String,
        values: ArrayRef,
        negated: bool,
    },
    Null {
        column: String,
        negated: bool,
    },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub fn all(predicates: impl IntoIterator<Item = Predicate>) -> Option<Predicate> {
        let mut predicates: Vec<Predicate> = predicates.into_iter().collect();
        match predicates.len() {
            0 => None,
            1 => predicates.pop(),
            _ => Some(Predicate::And(predicates)),
        }
    }

    /// Whether literals of this type can be bound by every lake format.
    pub fn supports(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Date32
                | DataType::Timestamp(
                    TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
                    _
                )
        )
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Predicate::Compare { column, op, value } => {
                write!(f, "{column} {op} {}", literal(value, 0))
            }
            Predicate::In {
                column,
                values,
                negated,
            } => {
                let keyword = if *negated { "NOT IN" } else { "IN" };
                write!(f, "{column} {keyword} (")?;
                for row in 0..values.len() {
                    if row > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str(&literal(values, row))?;
                }
                f.write_str(")")
            }
            Predicate::Null { column, negated } => {
                let test = if *negated { "IS NOT NULL" } else { "IS NULL" };
                write!(f, "{column} {test}")
            }
            Predicate::And(items) => join(f, items, " AND "),
            Predicate::Or(items) => join(f, items, " OR "),
            Predicate::Not(inner) => write!(f, "NOT ({inner})"),
        }
    }
}

fn join(f: &mut fmt::Formatter<'_>, items: &[Predicate], separator: &str) -> fmt::Result {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            f.write_str(separator)?;
        }
        match item {
            Predicate::And(_) | Predicate::Or(_) => write!(f, "({item})")?,
            _ => write!(f, "{item}")?,
        }
    }
    Ok(())
}

fn literal(values: &ArrayRef, row: usize) -> String {
    let text = array_value_to_string(values, row).unwrap_or_else(|e| e.to_string());
    match values.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => format!("'{text}'"),
        _ => text,
    }
}
