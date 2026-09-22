//! A logical data type: a kind with its parameters plus a nullability flag, with constructors and child traversal.

use crate::{Decimal, Family, Field, Fields, Length, Precision, Root};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataType {
    kind: Kind,
    nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Kind {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Char(Length),
    String,
    Binary(Length),
    Bytes,
    Decimal(Decimal),
    Date,
    Time(Precision),
    Timestamp(Precision),
    TimestampLtz(Precision),
    Array(Box<DataType>),
    Map {
        key: Box<DataType>,
        value: Box<DataType>,
    },
    Row(Fields),
}

impl Kind {
    pub fn root(&self) -> Root {
        match self {
            Kind::Boolean => Root::Boolean,
            Kind::TinyInt => Root::TinyInt,
            Kind::SmallInt => Root::SmallInt,
            Kind::Int => Root::Int,
            Kind::BigInt => Root::BigInt,
            Kind::Float => Root::Float,
            Kind::Double => Root::Double,
            Kind::Char(_) => Root::Char,
            Kind::String => Root::String,
            Kind::Binary(_) => Root::Binary,
            Kind::Bytes => Root::Bytes,
            Kind::Decimal(_) => Root::Decimal,
            Kind::Date => Root::Date,
            Kind::Time(_) => Root::Time,
            Kind::Timestamp(_) => Root::Timestamp,
            Kind::TimestampLtz(_) => Root::TimestampLtz,
            Kind::Array(_) => Root::Array,
            Kind::Map { .. } => Root::Map,
            Kind::Row(_) => Root::Row,
        }
    }
}

impl DataType {
    pub fn new(kind: Kind, nullable: bool) -> Self {
        let kind = match kind {
            Kind::Map { key, value } if key.nullable => Kind::Map {
                key: Box::new(key.with_nullable(false)),
                value,
            },
            other => other,
        };

        DataType { kind, nullable }
    }

    pub fn boolean() -> Self {
        Self::new(Kind::Boolean, true)
    }

    pub fn tiny_int() -> Self {
        Self::new(Kind::TinyInt, true)
    }

    pub fn small_int() -> Self {
        Self::new(Kind::SmallInt, true)
    }

    pub fn int() -> Self {
        Self::new(Kind::Int, true)
    }

    pub fn big_int() -> Self {
        Self::new(Kind::BigInt, true)
    }

    pub fn float() -> Self {
        Self::new(Kind::Float, true)
    }

    pub fn double() -> Self {
        Self::new(Kind::Double, true)
    }

    pub fn char(length: Length) -> Self {
        Self::new(Kind::Char(length), true)
    }

    pub fn string() -> Self {
        Self::new(Kind::String, true)
    }

    pub fn binary(length: Length) -> Self {
        Self::new(Kind::Binary(length), true)
    }

    pub fn bytes() -> Self {
        Self::new(Kind::Bytes, true)
    }

    pub fn decimal(decimal: Decimal) -> Self {
        Self::new(Kind::Decimal(decimal), true)
    }

    pub fn date() -> Self {
        Self::new(Kind::Date, true)
    }

    pub fn time(precision: Precision) -> Self {
        Self::new(Kind::Time(precision), true)
    }

    pub fn timestamp(precision: Precision) -> Self {
        Self::new(Kind::Timestamp(precision), true)
    }

    pub fn timestamp_ltz(precision: Precision) -> Self {
        Self::new(Kind::TimestampLtz(precision), true)
    }

    pub fn array(element: DataType) -> Self {
        Self::new(Kind::Array(Box::new(element)), true)
    }

    pub fn map(key: DataType, value: DataType) -> Self {
        Self::new(
            Kind::Map {
                key: Box::new(key),
                value: Box::new(value),
            },
            true,
        )
    }

    pub fn row(fields: Fields) -> Self {
        Self::new(Kind::Row(fields), true)
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn kind(&self) -> &Kind {
        &self.kind
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn root(&self) -> Root {
        self.kind.root()
    }

    pub fn is(&self, family: Family) -> bool {
        self.root().is(family)
    }

    pub fn children(&self) -> Vec<&DataType> {
        match &self.kind {
            Kind::Array(element) => vec![element],
            Kind::Map { key, value } => vec![key, value],
            Kind::Row(fields) => fields.iter().map(Field::data_type).collect(),
            _ => Vec::new(),
        }
    }

    pub fn assign_field_ids(&self, next: &mut u32) -> DataType {
        let kind = match &self.kind {
            Kind::Array(element) => Kind::Array(Box::new(element.assign_field_ids(next))),
            Kind::Map { key, value } => Kind::Map {
                key: Box::new(key.assign_field_ids(next)),
                value: Box::new(value.assign_field_ids(next)),
            },
            Kind::Row(fields) => Kind::Row(fields.assign_ids(next)),
            leaf => leaf.clone(),
        };
        DataType::new(kind, self.nullable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_are_nullable_by_default() {
        assert!(DataType::int().is_nullable());
        assert!(!DataType::int().with_nullable(false).is_nullable());
    }

    #[test]
    fn map_key_is_forced_non_nullable() {
        let map = DataType::map(DataType::string(), DataType::int());
        let Kind::Map { key, value } = map.kind() else {
            panic!("expected map");
        };
        assert!(!key.is_nullable());
        assert!(value.is_nullable());

        let rebuilt = DataType::new(map.kind().clone(), false);
        let Kind::Map { key, .. } = rebuilt.kind() else {
            panic!("expected map");
        };
        assert!(!key.is_nullable());
    }

    #[test]
    fn with_nullable_does_not_touch_children() {
        let array = DataType::array(DataType::int()).with_nullable(false);
        assert!(!array.is_nullable());
        assert!(array.children()[0].is_nullable());
    }

    #[test]
    fn children_of_each_shape() {
        assert!(DataType::int().children().is_empty());
        assert_eq!(DataType::array(DataType::int()).children().len(), 1);
        assert_eq!(
            DataType::map(DataType::int(), DataType::int())
                .children()
                .len(),
            2
        );
        let fields = Fields::new(vec![
            Field::new("a", DataType::int()).unwrap(),
            Field::new("b", DataType::int()).unwrap(),
            Field::new("c", DataType::int()).unwrap(),
        ])
        .unwrap();
        assert_eq!(DataType::row(fields).children().len(), 3);
    }

    #[test]
    fn root_and_family_follow_kind() {
        let decimal = DataType::decimal(Decimal::new(12, 3).unwrap());
        assert_eq!(decimal.root(), Root::Decimal);
        assert!(decimal.is(Family::ExactNumeric));
        assert!(!decimal.is(Family::IntegerNumeric));
    }
}
