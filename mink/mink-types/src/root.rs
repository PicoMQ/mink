//! The root of each data type, its keyword, and the type families it belongs to.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, Family};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Root {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Char,
    String,
    Binary,
    Bytes,
    Decimal,
    Date,
    Time,
    Timestamp,
    TimestampLtz,
    Array,
    Map,
    Row,
}

impl Root {
    pub const ALL: [Root; 19] = [
        Root::Boolean,
        Root::TinyInt,
        Root::SmallInt,
        Root::Int,
        Root::BigInt,
        Root::Float,
        Root::Double,
        Root::Char,
        Root::String,
        Root::Binary,
        Root::Bytes,
        Root::Decimal,
        Root::Date,
        Root::Time,
        Root::Timestamp,
        Root::TimestampLtz,
        Root::Array,
        Root::Map,
        Root::Row,
    ];

    pub const fn keyword(self) -> &'static str {
        match self {
            Root::Boolean => "BOOLEAN",
            Root::TinyInt => "TINYINT",
            Root::SmallInt => "SMALLINT",
            Root::Int => "INT",
            Root::BigInt => "BIGINT",
            Root::Float => "FLOAT",
            Root::Double => "DOUBLE",
            Root::Char => "CHAR",
            Root::String => "STRING",
            Root::Binary => "BINARY",
            Root::Bytes => "BYTES",
            Root::Decimal => "DECIMAL",
            Root::Date => "DATE",
            Root::Time => "TIME",
            Root::Timestamp => "TIMESTAMP",
            Root::TimestampLtz => "TIMESTAMP_LTZ",
            Root::Array => "ARRAY",
            Root::Map => "MAP",
            Root::Row => "ROW",
        }
    }

    pub const fn families(self) -> &'static [Family] {
        use Family::*;

        match self {
            Root::Boolean => &[Predefined],
            Root::TinyInt | Root::SmallInt | Root::Int | Root::BigInt => {
                &[Predefined, Numeric, IntegerNumeric, ExactNumeric]
            }
            Root::Float | Root::Double => &[Predefined, Numeric, ApproximateNumeric],
            Root::Char | Root::String => &[Predefined, CharacterString],
            Root::Binary | Root::Bytes => &[Predefined, BinaryString],
            Root::Decimal => &[Predefined, Numeric, ExactNumeric],
            Root::Date => &[Predefined, Datetime],
            Root::Time => &[Predefined, Datetime, Time],
            Root::Timestamp => &[Predefined, Datetime, Timestamp],
            Root::TimestampLtz => &[Predefined, Datetime, Timestamp, Extension],
            Root::Array => &[Constructed, Collection],
            Root::Map => &[Constructed, Extension],
            Root::Row => &[Constructed],
        }
    }

    pub fn is(self, family: Family) -> bool {
        self.families().contains(&family)
    }
}

impl fmt::Display for Root {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.keyword())
    }
}

impl FromStr for Root {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Root::ALL
            .into_iter()
            .find(|root| root.keyword() == s)
            .ok_or_else(|| Error::Parse {
                position: 0,
                message: format!("unknown type root `{s}`"),
            })
    }
}

impl Serialize for Root {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.keyword())
    }
}

impl<'de> Deserialize<'de> for Root {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let keyword: Cow<'de, str> = Deserialize::deserialize(deserializer)?;
        keyword.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_are_unique_and_round_trip() {
        for root in Root::ALL {
            assert_eq!(root.keyword().parse::<Root>().unwrap(), root);
            assert_eq!(
                Root::ALL
                    .iter()
                    .filter(|r| r.keyword() == root.keyword())
                    .count(),
                1
            );
        }
    }

    #[test]
    fn every_root_is_predefined_or_constructed() {
        for root in Root::ALL {
            assert!(
                root.is(Family::Predefined) ^ root.is(Family::Constructed),
                "{root}"
            );
        }
    }

    #[test]
    fn integer_types_are_numeric_and_exact() {
        for root in [Root::TinyInt, Root::SmallInt, Root::Int, Root::BigInt] {
            assert!(root.is(Family::IntegerNumeric));
            assert!(root.is(Family::ExactNumeric));
            assert!(!root.is(Family::ApproximateNumeric));
        }
        assert!(Root::Decimal.is(Family::ExactNumeric));
        assert!(!Root::Decimal.is(Family::IntegerNumeric));
        assert!(Root::Double.is(Family::ApproximateNumeric));
    }

    #[test]
    fn datetime_family_covers_date_time_and_timestamps() {
        let datetime: Vec<Root> = Root::ALL
            .into_iter()
            .filter(|root| root.is(Family::Datetime))
            .collect();
        assert_eq!(
            datetime,
            [Root::Date, Root::Time, Root::Timestamp, Root::TimestampLtz]
        );
        assert!(!Root::Date.is(Family::Time));
        assert!(!Root::Date.is(Family::Timestamp));
    }

    #[test]
    fn unknown_keyword_is_an_error() {
        assert!(matches!(
            "VARCHAR".parse::<Root>(),
            Err(Error::Parse { .. })
        ));
        assert!(matches!("int".parse::<Root>(), Err(Error::Parse { .. })));
    }
}
