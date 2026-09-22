//! Merge engine that folds each column of a new row into the stored row with its configured aggregate.

use std::cmp::Ordering;
use std::sync::Arc;

use mink_record::{Row, Scalar};
use mink_table::{Aggregate, DeleteBehavior};
use mink_types::Kind;
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::Error;
use crate::merger::{Decoded, Merged, RowMerger, field};
use crate::schema::Version;
use crate::targets::{Targets, key_flags};
use crate::value::Value;

pub(crate) struct Aggregator {
    target: Arc<Version>,
    functions: Vec<Function>,
    targets: Option<Targets>,
    delete_behavior: DeleteBehavior,
}

impl Aggregator {
    pub(crate) fn new(target: Arc<Version>, delete_behavior: DeleteBehavior) -> Self {
        let functions = functions(&target);

        Aggregator {
            target,
            functions,
            targets: None,
            delete_behavior,
        }
    }

    pub(crate) fn partial(
        target: Arc<Version>,
        columns: &[usize],
        delete_behavior: DeleteBehavior,
    ) -> Result<Self, Error> {
        let functions = functions(&target);
        let targets = Targets::new(&target, columns, None, Error::AggregateNotNullable)?;

        Ok(Aggregator {
            target,
            functions,
            targets: Some(targets),
            delete_behavior,
        })
    }

    fn folds(&self, i: usize) -> bool {
        self.targets
            .as_ref()
            .is_none_or(|targets| targets.written[i])
    }
}

impl RowMerger for Aggregator {
    fn merge(&self, old: &Decoded<'_>, new: &Decoded<'_>) -> Result<Merged, Error> {
        let mut folded = Vec::with_capacity(self.functions.len());
        for (i, function) in self.functions.iter().enumerate() {
            let stored = field(&old.row, i);
            folded.push(if self.folds(i) {
                function
                    .fold(stored, field(&new.row, i))
                    .map_err(|found| self.operand_error(i, found))?
            } else {
                stored.map(Folded::Scalar)
            });
        }

        let row: Row<'_> = folded
            .iter()
            .map(|f| f.as_ref().map(Folded::scalar))
            .collect();

        Ok(Merged::Row(self.target.encode(&row)?))
    }

    fn delete(&self, old: &Decoded<'_>) -> Result<Option<Value>, Error> {
        match self.targets.as_ref().and_then(|t| t.after_delete(&old.row)) {
            Some(row) => Ok(Some(self.target.encode(&row)?)),
            None => Ok(None),
        }
    }

    fn delete_behavior(&self) -> DeleteBehavior {
        self.delete_behavior
    }
}

impl Aggregator {
    fn operand_error(&self, column: usize, found: Operand) -> Error {
        let name = self.target.schema.fields()[column].name().to_owned();
        match found {
            Operand::Bitmap => Error::AggregateBitmap(name),
            Operand::Type(found) => Error::AggregateOperand {
                aggregate: self.functions[column].name().to_owned(),
                column: name,
                found,
            },
        }
    }
}

fn functions(target: &Version) -> Vec<Function> {
    let keys = key_flags(target);

    target
        .schema
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| {
            if keys[i] {
                return Function::LastValue;
            }
            match column.aggregate() {
                None => Function::LastValueIgnoreNulls,
                Some(Aggregate::Sum) => Function::Sum,
                Some(Aggregate::Product) => Function::Product {
                    scale: match column.data_type().kind() {
                        Kind::Decimal(decimal) => decimal.scale(),
                        _ => 0,
                    },
                },
                Some(Aggregate::Max) => Function::Max,
                Some(Aggregate::Min) => Function::Min,
                Some(Aggregate::LastValue) => Function::LastValue,
                Some(Aggregate::LastValueIgnoreNulls) => Function::LastValueIgnoreNulls,
                Some(Aggregate::FirstValue) => Function::FirstValue,
                Some(Aggregate::FirstValueIgnoreNulls) => Function::FirstValueIgnoreNulls,
                Some(Aggregate::ListAgg { delimiter }) => Function::ListAgg {
                    delimiter: delimiter.clone(),
                },
                Some(Aggregate::BoolAnd) => Function::BoolAnd,
                Some(Aggregate::BoolOr) => Function::BoolOr,
                Some(Aggregate::Rbm32) => Function::Rbm32,
                Some(Aggregate::Rbm64) => Function::Rbm64,
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Function {
    Sum,
    Product { scale: u8 },
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

#[derive(Debug, Clone, PartialEq)]
enum Folded<'a> {
    Scalar(Scalar<'a>),
    Text(String),
    Bytes(Vec<u8>),
}

impl Folded<'_> {
    fn scalar(&self) -> Scalar<'_> {
        match self {
            Folded::Scalar(scalar) => *scalar,
            Folded::Text(text) => Scalar::String(text),
            Folded::Bytes(bytes) => Scalar::Bytes(bytes),
        }
    }
}

enum Operand {
    Type(String),
    Bitmap,
}

impl Function {
    fn name(&self) -> &'static str {
        match self {
            Function::Sum => "sum",
            Function::Product { .. } => "product",
            Function::Max => "max",
            Function::Min => "min",
            Function::LastValue => "last_value",
            Function::LastValueIgnoreNulls => "last_value_ignore_nulls",
            Function::FirstValue => "first_value",
            Function::FirstValueIgnoreNulls => "first_value_ignore_nulls",
            Function::ListAgg { .. } => "listagg",
            Function::BoolAnd => "bool_and",
            Function::BoolOr => "bool_or",
            Function::Rbm32 => "rbm32",
            Function::Rbm64 => "rbm64",
        }
    }

    fn fold<'a>(
        &self,
        accumulator: Option<Scalar<'a>>,
        input: Option<Scalar<'a>>,
    ) -> Result<Option<Folded<'a>>, Operand> {
        // FirstValue and FirstValueIgnoreNulls are the same: keep a non-null accumulator.
        let first = || accumulator.or(input).map(Folded::Scalar);
        match self {
            Function::LastValue => return Ok(input.map(Folded::Scalar)),
            Function::LastValueIgnoreNulls => return Ok(input.or(accumulator).map(Folded::Scalar)),
            Function::FirstValue | Function::FirstValueIgnoreNulls => return Ok(first()),
            _ => {}
        }

        let (Some(a), Some(b)) = (accumulator, input) else {
            return Ok(first());
        };

        let mismatch = || Operand::Type(format!("{a:?} and {b:?}"));
        let folded = match self {
            Function::Sum => return Ok(sum(a, b).ok_or_else(mismatch)?.map(Folded::Scalar)),
            Function::Product { scale } => {
                return Ok(product(a, b, *scale)
                    .ok_or_else(mismatch)?
                    .map(Folded::Scalar));
            }
            Function::Max => {
                Folded::Scalar(if compare(a, b).ok_or_else(mismatch)? == Ordering::Less {
                    b
                } else {
                    a
                })
            }
            Function::Min => Folded::Scalar(
                if compare(a, b).ok_or_else(mismatch)? == Ordering::Greater {
                    b
                } else {
                    a
                },
            ),
            Function::ListAgg { delimiter } => match (a, b) {
                (Scalar::String(a), Scalar::String(b)) => {
                    let mut text = String::with_capacity(a.len() + delimiter.len() + b.len());
                    text.push_str(a);
                    text.push_str(delimiter);
                    text.push_str(b);
                    Folded::Text(text)
                }
                _ => return Err(mismatch()),
            },
            Function::BoolAnd => match (a, b) {
                (Scalar::Boolean(a), Scalar::Boolean(b)) => Folded::Scalar(Scalar::Boolean(a && b)),
                _ => return Err(mismatch()),
            },
            Function::BoolOr => match (a, b) {
                (Scalar::Boolean(a), Scalar::Boolean(b)) => Folded::Scalar(Scalar::Boolean(a || b)),
                _ => return Err(mismatch()),
            },
            Function::Rbm32 => match (a, b) {
                (Scalar::Bytes(a), Scalar::Bytes(b)) => {
                    let mut union =
                        RoaringBitmap::deserialize_from(a).map_err(|_| Operand::Bitmap)?;
                    union |= RoaringBitmap::deserialize_from(b).map_err(|_| Operand::Bitmap)?;
                    let mut bytes = Vec::with_capacity(union.serialized_size());
                    union
                        .serialize_into(&mut bytes)
                        .map_err(|_| Operand::Bitmap)?;
                    Folded::Bytes(bytes)
                }
                _ => return Err(mismatch()),
            },
            Function::Rbm64 => match (a, b) {
                (Scalar::Bytes(a), Scalar::Bytes(b)) => {
                    let mut union =
                        RoaringTreemap::deserialize_from(a).map_err(|_| Operand::Bitmap)?;
                    union |= RoaringTreemap::deserialize_from(b).map_err(|_| Operand::Bitmap)?;
                    let mut bytes = Vec::with_capacity(union.serialized_size());
                    union
                        .serialize_into(&mut bytes)
                        .map_err(|_| Operand::Bitmap)?;
                    Folded::Bytes(bytes)
                }
                _ => return Err(mismatch()),
            },
            Function::LastValue
            | Function::LastValueIgnoreNulls
            | Function::FirstValue
            | Function::FirstValueIgnoreNulls => unreachable!("handled above"),
        };

        Ok(Some(folded))
    }
}

fn sum<'a>(a: Scalar<'a>, b: Scalar<'a>) -> Option<Option<Scalar<'a>>> {
    Some(Some(match (a, b) {
        (Scalar::TinyInt(a), Scalar::TinyInt(b)) => Scalar::TinyInt(a.wrapping_add(b)),
        (Scalar::SmallInt(a), Scalar::SmallInt(b)) => Scalar::SmallInt(a.wrapping_add(b)),
        (Scalar::Int(a), Scalar::Int(b)) => Scalar::Int(a.wrapping_add(b)),
        (Scalar::BigInt(a), Scalar::BigInt(b)) => Scalar::BigInt(a.wrapping_add(b)),
        (Scalar::Float(a), Scalar::Float(b)) => Scalar::Float(a + b),
        (Scalar::Double(a), Scalar::Double(b)) => Scalar::Double(a + b),
        (
            Scalar::Decimal {
                unscaled: a,
                precision,
            },
            Scalar::Decimal { unscaled: b, .. },
        ) => return Some(decimal(a.checked_add(b), precision)),
        _ => return None,
    }))
}

fn product<'a>(a: Scalar<'a>, b: Scalar<'a>, scale: u8) -> Option<Option<Scalar<'a>>> {
    Some(Some(match (a, b) {
        (Scalar::TinyInt(a), Scalar::TinyInt(b)) => Scalar::TinyInt(a.wrapping_mul(b)),
        (Scalar::SmallInt(a), Scalar::SmallInt(b)) => Scalar::SmallInt(a.wrapping_mul(b)),
        (Scalar::Int(a), Scalar::Int(b)) => Scalar::Int(a.wrapping_mul(b)),
        (Scalar::BigInt(a), Scalar::BigInt(b)) => Scalar::BigInt(a.wrapping_mul(b)),
        (Scalar::Float(a), Scalar::Float(b)) => Scalar::Float(a * b),
        (Scalar::Double(a), Scalar::Double(b)) => Scalar::Double(a * b),
        (
            Scalar::Decimal {
                unscaled: a,
                precision,
            },
            Scalar::Decimal { unscaled: b, .. },
        ) => {
            let exact = a.checked_mul(b);
            let rescaled = exact.and_then(|exact| {
                let divisor = 10i128.checked_pow(u32::from(scale))?;
                let quotient = exact / divisor;
                let remainder = (exact % divisor).abs();
                let round_up = remainder * 2 >= divisor;
                Some(match (round_up, exact.is_negative()) {
                    (false, _) => quotient,
                    (true, false) => quotient + 1,
                    (true, true) => quotient - 1,
                })
            });
            return Some(decimal(rescaled, precision));
        }
        _ => return None,
    }))
}

fn decimal(unscaled: Option<i128>, precision: u8) -> Option<Scalar<'static>> {
    let unscaled = unscaled?;
    let fits = match 10i128.checked_pow(u32::from(precision)) {
        Some(limit) => unscaled.abs() < limit,
        None => true,
    };
    fits.then_some(Scalar::Decimal {
        unscaled,
        precision,
    })
}

fn compare(a: Scalar<'_>, b: Scalar<'_>) -> Option<Ordering> {
    Some(match (a, b) {
        (Scalar::Boolean(a), Scalar::Boolean(b)) => a.cmp(&b),
        (Scalar::TinyInt(a), Scalar::TinyInt(b)) => a.cmp(&b),
        (Scalar::SmallInt(a), Scalar::SmallInt(b)) => a.cmp(&b),
        (Scalar::Int(a), Scalar::Int(b)) => a.cmp(&b),
        (Scalar::BigInt(a), Scalar::BigInt(b)) => a.cmp(&b),
        (Scalar::Float(a), Scalar::Float(b)) => a.total_cmp(&b),
        (Scalar::Double(a), Scalar::Double(b)) => a.total_cmp(&b),
        (Scalar::String(a), Scalar::String(b)) => a.cmp(b),
        (Scalar::Bytes(a), Scalar::Bytes(b)) => a.cmp(b),
        (Scalar::Decimal { unscaled: a, .. }, Scalar::Decimal { unscaled: b, .. }) => a.cmp(&b),
        (Scalar::Date(a), Scalar::Date(b)) => a.cmp(&b),
        (Scalar::Time(a), Scalar::Time(b)) => a.cmp(&b),
        (Scalar::Timestamp { at: a, .. }, Scalar::Timestamp { at: b, .. }) => {
            (a.millis, a.nanos).cmp(&(b.millis, b.nanos))
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fold<'a>(f: Function, a: Option<Scalar<'a>>, b: Option<Scalar<'a>>) -> Option<Folded<'a>> {
        f.fold(a, b).ok().unwrap()
    }

    fn scalar(s: Scalar<'_>) -> Option<Folded<'_>> {
        Some(Folded::Scalar(s))
    }

    #[test]
    fn nulls_take_the_other_side_except_for_last_value() {
        let one = Some(Scalar::Int(1));
        for f in [
            Function::Sum,
            Function::Product { scale: 0 },
            Function::Max,
            Function::Min,
            Function::LastValueIgnoreNulls,
            Function::FirstValue,
            Function::FirstValueIgnoreNulls,
        ] {
            assert_eq!(fold(f.clone(), None, one), scalar(Scalar::Int(1)), "{f:?}");
            assert_eq!(fold(f.clone(), one, None), scalar(Scalar::Int(1)), "{f:?}");
            assert_eq!(fold(f, None, None), None);
        }
        assert_eq!(fold(Function::LastValue, one, None), None);
        assert_eq!(fold(Function::LastValue, None, one), scalar(Scalar::Int(1)));
    }

    #[test]
    fn first_value_keeps_the_accumulator() {
        let (a, b) = (Some(Scalar::Int(1)), Some(Scalar::Int(2)));
        assert_eq!(fold(Function::FirstValue, a, b), scalar(Scalar::Int(1)));
        assert_eq!(
            fold(Function::FirstValueIgnoreNulls, a, b),
            scalar(Scalar::Int(1))
        );
        assert_eq!(fold(Function::LastValue, a, b), scalar(Scalar::Int(2)));
    }

    #[test]
    fn sums_wrap_like_java() {
        assert_eq!(
            fold(
                Function::Sum,
                Some(Scalar::TinyInt(127)),
                Some(Scalar::TinyInt(1))
            ),
            scalar(Scalar::TinyInt(-128))
        );
        assert_eq!(
            fold(
                Function::Sum,
                Some(Scalar::BigInt(i64::MAX)),
                Some(Scalar::BigInt(1))
            ),
            scalar(Scalar::BigInt(i64::MIN))
        );
        assert_eq!(
            fold(
                Function::Sum,
                Some(Scalar::Double(1.5)),
                Some(Scalar::Double(2.0))
            ),
            scalar(Scalar::Double(3.5))
        );
    }

    #[test]
    fn decimal_sum_and_product_stay_at_the_column_scale() {
        let d = |unscaled| Scalar::Decimal {
            unscaled,
            precision: 5,
        };
        assert_eq!(
            fold(Function::Sum, Some(d(1234)), Some(d(1))),
            scalar(d(1235))
        );
        assert_eq!(
            fold(Function::Product { scale: 2 }, Some(d(150)), Some(d(225))),
            scalar(d(338))
        );
        assert_eq!(
            fold(Function::Product { scale: 2 }, Some(d(-150)), Some(d(225))),
            scalar(d(-338))
        );
        assert_eq!(fold(Function::Sum, Some(d(99999)), Some(d(1))), None);
    }

    #[test]
    fn max_and_min_compare_within_a_type() {
        assert_eq!(
            fold(
                Function::Max,
                Some(Scalar::String("a")),
                Some(Scalar::String("b"))
            ),
            scalar(Scalar::String("b"))
        );
        assert_eq!(
            fold(
                Function::Min,
                Some(Scalar::String("a")),
                Some(Scalar::String("b"))
            ),
            scalar(Scalar::String("a"))
        );
        assert!(matches!(
            fold(Function::Max, Some(Scalar::Double(1.0)), Some(Scalar::Double(f64::NAN))),
            Some(Folded::Scalar(Scalar::Double(v))) if v.is_nan()
        ));
        assert!(
            Function::Max
                .fold(Some(Scalar::Int(1)), Some(Scalar::BigInt(2)))
                .is_err()
        );
    }

    #[test]
    fn listagg_joins_with_the_delimiter() {
        let f = Function::ListAgg {
            delimiter: ", ".to_owned(),
        };
        assert_eq!(
            fold(f, Some(Scalar::String("a")), Some(Scalar::String("b"))),
            Some(Folded::Text("a, b".to_owned()))
        );
    }

    #[test]
    fn booleans_fold() {
        let (t, f) = (Some(Scalar::Boolean(true)), Some(Scalar::Boolean(false)));
        assert_eq!(
            fold(Function::BoolAnd, t, f),
            scalar(Scalar::Boolean(false))
        );
        assert_eq!(fold(Function::BoolOr, t, f), scalar(Scalar::Boolean(true)));
    }

    #[test]
    fn bitmaps_union() {
        let bytes = |values: &[u32]| {
            let bitmap: RoaringBitmap = values.iter().copied().collect();
            let mut bytes = Vec::new();
            bitmap.serialize_into(&mut bytes).unwrap();
            bytes
        };
        let (a, b) = (bytes(&[1, 2]), bytes(&[2, 3]));
        let folded = fold(
            Function::Rbm32,
            Some(Scalar::Bytes(&a)),
            Some(Scalar::Bytes(&b)),
        );
        assert_eq!(folded, Some(Folded::Bytes(bytes(&[1, 2, 3]))));

        let bytes64 = |values: &[u64]| {
            let bitmap: RoaringTreemap = values.iter().copied().collect();
            let mut bytes = Vec::new();
            bitmap.serialize_into(&mut bytes).unwrap();
            bytes
        };
        let (a, b) = (bytes64(&[1, u64::MAX]), bytes64(&[2]));
        let folded = fold(
            Function::Rbm64,
            Some(Scalar::Bytes(&a)),
            Some(Scalar::Bytes(&b)),
        );
        assert_eq!(folded, Some(Folded::Bytes(bytes64(&[1, 2, u64::MAX]))));

        assert!(
            Function::Rbm32
                .fold(Some(Scalar::Bytes(b"junk")), Some(Scalar::Bytes(&a)))
                .is_err()
        );
    }
}
