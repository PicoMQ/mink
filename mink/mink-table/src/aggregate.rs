//! Per-column aggregate functions for the aggregation merge engine and the types each one accepts.

use std::fmt;

use mink_types::{DataType, Family, Root};
use serde::{Deserialize, Serialize};

pub const DEFAULT_LIST_DELIMITER: &str = ",";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Aggregate {
    Sum,
    Product,
    Max,
    Min,
    LastValue,
    LastValueIgnoreNulls,
    FirstValue,
    FirstValueIgnoreNulls,
    ListAgg { delimiter: String },
    BoolAnd,
    BoolOr,
    Rbm32,
    Rbm64,
}

impl Aggregate {
    pub fn list_agg() -> Self {
        Aggregate::ListAgg {
            delimiter: DEFAULT_LIST_DELIMITER.to_owned(),
        }
    }

    pub fn supports(&self, data_type: &DataType) -> bool {
        match self {
            Aggregate::Sum | Aggregate::Product => data_type.is(Family::Numeric),
            Aggregate::Max | Aggregate::Min => {
                data_type.is(Family::Numeric)
                    || data_type.is(Family::CharacterString)
                    || data_type.is(Family::Datetime)
            }
            Aggregate::LastValue
            | Aggregate::LastValueIgnoreNulls
            | Aggregate::FirstValue
            | Aggregate::FirstValueIgnoreNulls => true,
            Aggregate::ListAgg { .. } => data_type.is(Family::CharacterString),
            Aggregate::BoolAnd | Aggregate::BoolOr => data_type.root() == Root::Boolean,
            Aggregate::Rbm32 | Aggregate::Rbm64 => data_type.root() == Root::Bytes,
        }
    }
}

impl fmt::Display for Aggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Aggregate::Sum => f.write_str("sum"),
            Aggregate::Product => f.write_str("product"),
            Aggregate::Max => f.write_str("max"),
            Aggregate::Min => f.write_str("min"),
            Aggregate::LastValue => f.write_str("last_value"),
            Aggregate::LastValueIgnoreNulls => f.write_str("last_value_ignore_nulls"),
            Aggregate::FirstValue => f.write_str("first_value"),
            Aggregate::FirstValueIgnoreNulls => f.write_str("first_value_ignore_nulls"),
            Aggregate::ListAgg { delimiter } => write!(f, "listagg({delimiter:?})"),
            Aggregate::BoolAnd => f.write_str("bool_and"),
            Aggregate::BoolOr => f.write_str("bool_or"),
            Aggregate::Rbm32 => f.write_str("rbm32"),
            Aggregate::Rbm64 => f.write_str("rbm64"),
        }
    }
}

#[cfg(test)]
mod tests {
    use mink_types::{Decimal, Length, Precision};

    use super::*;

    fn all_types() -> Vec<DataType> {
        vec![
            DataType::boolean(),
            DataType::tiny_int(),
            DataType::small_int(),
            DataType::int(),
            DataType::big_int(),
            DataType::float(),
            DataType::double(),
            DataType::char(Length::new(3).unwrap()),
            DataType::string(),
            DataType::binary(Length::new(3).unwrap()),
            DataType::bytes(),
            DataType::decimal(Decimal::new(10, 2).unwrap()),
            DataType::date(),
            DataType::time(Precision::new(3).unwrap()),
            DataType::timestamp(Precision::new(6).unwrap()),
            DataType::timestamp_ltz(Precision::new(6).unwrap()),
            DataType::array(DataType::int()),
            DataType::map(DataType::string(), DataType::int()),
            DataType::row(mink_types::Fields::new(vec![]).unwrap()),
        ]
    }

    fn supported(aggregate: &Aggregate) -> Vec<Root> {
        all_types()
            .iter()
            .filter(|data_type| aggregate.supports(data_type))
            .map(DataType::root)
            .collect()
    }

    #[test]
    fn type_support_matches_the_reference_tables() {
        use Root::*;

        let numeric = [TinyInt, SmallInt, Int, BigInt, Float, Double, Decimal];
        assert_eq!(supported(&Aggregate::Sum), numeric);
        assert_eq!(supported(&Aggregate::Product), numeric);

        let ordered = [
            TinyInt,
            SmallInt,
            Int,
            BigInt,
            Float,
            Double,
            Char,
            String,
            Decimal,
            Date,
            Time,
            Timestamp,
            TimestampLtz,
        ];
        assert_eq!(supported(&Aggregate::Max), ordered);
        assert_eq!(supported(&Aggregate::Min), ordered);

        assert_eq!(supported(&Aggregate::list_agg()), [Char, String]);
        assert_eq!(supported(&Aggregate::BoolAnd), [Boolean]);
        assert_eq!(supported(&Aggregate::BoolOr), [Boolean]);
        assert_eq!(supported(&Aggregate::Rbm32), [Bytes]);
        assert_eq!(supported(&Aggregate::Rbm64), [Bytes]);

        for any in [
            Aggregate::LastValue,
            Aggregate::LastValueIgnoreNulls,
            Aggregate::FirstValue,
            Aggregate::FirstValueIgnoreNulls,
        ] {
            assert_eq!(supported(&any).len(), all_types().len());
        }
    }

    #[test]
    fn json_shape() {
        assert_eq!(serde_json::to_string(&Aggregate::Sum).unwrap(), "\"sum\"");
        let json = serde_json::to_string(&Aggregate::list_agg()).unwrap();
        assert_eq!(json, r#"{"list_agg":{"delimiter":","}}"#);
        assert_eq!(
            serde_json::from_str::<Aggregate>(&json).unwrap(),
            Aggregate::list_agg()
        );
    }
}
