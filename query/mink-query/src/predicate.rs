//! Translates the filters DataFusion pushes into a scan into the lake's predicate: comparisons of a
//! column with a literal, `IN` lists, null tests and their boolean combinations over supported types.

use arrow_schema::{DataType, Schema, TimeUnit};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use mink_lake::{Compare, Predicate};

pub fn convert(expr: &Expr, schema: &Schema) -> Option<Predicate> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => Some(Predicate::And(vec![
                convert(left, schema)?,
                convert(right, schema)?,
            ])),
            Operator::Or => Some(Predicate::Or(vec![
                convert(left, schema)?,
                convert(right, schema)?,
            ])),
            _ => {
                let op = compare(*op)?;
                match (left.as_ref(), right.as_ref()) {
                    (column, Expr::Literal(value, _)) => {
                        let column = Column::resolve(column, schema)?;
                        column.compare(op, value)
                    }
                    (Expr::Literal(value, _), column) => {
                        let column = Column::resolve(column, schema)?;
                        column.compare(op.flip(), value)
                    }
                    _ => None,
                }
            }
        },
        Expr::InList(list) => {
            let column = Column::resolve(&list.expr, schema)?;
            let values = list
                .list
                .iter()
                .map(|item| match item {
                    Expr::Literal(value, _) => column.literal(value),
                    _ => None,
                })
                .collect::<Option<Vec<ScalarValue>>>()?;
            if values.is_empty() {
                return None;
            }
            Some(Predicate::In {
                column: column.name.to_owned(),
                values: ScalarValue::iter_to_array(values).ok()?,
                negated: list.negated,
            })
        }
        Expr::IsNull(inner) => null_test(inner, false, schema),
        Expr::IsNotNull(inner) => null_test(inner, true, schema),
        Expr::Not(inner) => Some(Predicate::Not(Box::new(convert(inner, schema)?))),
        _ => None,
    }
}

/// A column reference as it appears in a filter: bare, or widened by a cast DataFusion inserted to
/// coerce it to the literal's type. Literals are brought back to the column's own type.
pub(crate) struct Column<'e> {
    pub(crate) name: &'e str,
    data_type: &'e DataType,
    widened: Option<&'e DataType>,
}

impl<'e> Column<'e> {
    pub(crate) fn resolve(expr: &'e Expr, schema: &'e Schema) -> Option<Self> {
        let (column, widened) = match expr {
            Expr::Column(column) => (column, None),
            Expr::Cast(cast) => match cast.expr.as_ref() {
                Expr::Column(column) => (column, Some(cast.field.data_type())),
                _ => return None,
            },
            _ => return None,
        };
        let data_type = schema.field_with_name(&column.name).ok()?.data_type();
        if !Predicate::supports(data_type) {
            return None;
        }
        if let Some(widened) = widened
            && !widens(data_type, widened)
        {
            return None;
        }

        Some(Column {
            name: &column.name,
            data_type,
            widened,
        })
    }

    pub(crate) fn literal(&self, value: &ScalarValue) -> Option<ScalarValue> {
        if value.is_null() {
            return None;
        }
        let narrowed = value.cast_to(self.data_type).ok()?;
        if let Some(widened) = self.widened
            && narrowed.cast_to(widened).ok()? != value.cast_to(widened).ok()?
        {
            return None;
        }

        Some(narrowed)
    }

    fn compare(&self, op: Compare, value: &ScalarValue) -> Option<Predicate> {
        Some(Predicate::Compare {
            column: self.name.to_owned(),
            op,
            value: self.literal(value)?.to_array().ok()?,
        })
    }
}

/// Casts that keep every value distinct and in the same order, so a comparison against the widened
/// column is the same comparison against the column itself.
fn widens(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    match (from, to) {
        (Int8, Int16 | Int32 | Int64 | Float32 | Float64)
        | (Int16, Int32 | Int64 | Float32 | Float64)
        | (Int32, Int64 | Float64)
        | (Float32, Float64)
        | (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View)
        | (Binary | LargeBinary | BinaryView, Binary | LargeBinary | BinaryView)
        | (Date32, Timestamp(_, None)) => true,
        (Timestamp(from_unit, from_zone), Timestamp(to_unit, to_zone)) => {
            from_zone == to_zone && unit_rank(from_unit) <= unit_rank(to_unit)
        }
        _ => false,
    }
}

fn unit_rank(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

fn compare(op: Operator) -> Option<Compare> {
    Some(match op {
        Operator::Eq => Compare::Eq,
        Operator::NotEq => Compare::NotEq,
        Operator::Lt => Compare::Lt,
        Operator::LtEq => Compare::LtEq,
        Operator::Gt => Compare::Gt,
        Operator::GtEq => Compare::GtEq,
        _ => return None,
    })
}

fn null_test(expr: &Expr, negated: bool, schema: &Schema) -> Option<Predicate> {
    let column = Column::resolve(expr, schema)?;

    Some(Predicate::Null {
        column: column.name.to_owned(),
        negated,
    })
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;
    use datafusion::logical_expr::{Cast, col, lit};
    use datafusion::prelude::Expr;

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
            Field::new("small", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ])
    }

    fn shown(expr: Expr) -> Option<String> {
        convert(&expr, &schema()).map(|p| p.to_string())
    }

    #[test]
    fn comparisons_cast_literals_to_the_column_and_flip_around_them() {
        assert_eq!(shown(col("k").gt(lit(5i32))).as_deref(), Some("k > 5"));
        assert_eq!(shown(lit(5i64).gt(col("k"))).as_deref(), Some("k < 5"));
        assert_eq!(shown(col("v").eq(lit("x"))).as_deref(), Some("v = 'x'"));
    }

    #[test]
    fn boolean_shapes_nest() {
        let expr = col("k")
            .gt_eq(lit(1i64))
            .and(col("v").is_not_null().or(col("k").eq(lit(0i64))));
        assert_eq!(
            shown(expr).as_deref(),
            Some("k >= 1 AND (v IS NOT NULL OR k = 0)")
        );
        assert_eq!(
            shown(Expr::Not(Box::new(
                col("k").in_list(vec![lit(1i64), lit(2i64)], false)
            )))
            .as_deref(),
            Some("NOT (k IN (1, 2))")
        );
    }

    #[test]
    fn widening_casts_on_the_column_fold_into_the_literal() {
        let widened = Expr::Cast(Cast::new(Box::new(col("small")), DataType::Int64));
        assert_eq!(
            shown(widened.clone().gt(lit(5i64))).as_deref(),
            Some("small > 5")
        );
        assert_eq!(
            shown(widened.clone().in_list(vec![lit(1i64), lit(2i64)], false)).as_deref(),
            Some("small IN (1, 2)")
        );
        assert_eq!(
            shown(widened.clone().is_null()).as_deref(),
            Some("small IS NULL")
        );
        assert_eq!(
            shown(widened.gt(lit(i64::from(i32::MAX) + 1))),
            None,
            "a literal outside the column's range does not fold"
        );
        let text = Expr::Cast(Cast::new(Box::new(col("v")), DataType::Utf8View));
        assert_eq!(
            shown(text.eq(lit(ScalarValue::Utf8View(Some("x".into()))))).as_deref(),
            Some("v = 'x'")
        );
        let truncated = Expr::Cast(Cast::new(Box::new(col("ts")), DataType::Date32));
        assert_eq!(shown(truncated.eq(lit(ScalarValue::Date32(Some(1))))), None);
        let narrowed = Expr::Cast(Cast::new(Box::new(col("k")), DataType::Int32));
        assert_eq!(shown(narrowed.eq(lit(1i32))), None);
    }

    #[test]
    fn what_the_lake_cannot_bind_stays_with_the_engine() {
        assert_eq!(shown(col("k").eq(lit(ScalarValue::Int64(None)))), None);
        assert_eq!(shown(col("k").gt(col("price"))), None);
        assert_eq!(shown(col("price").gt(lit(1i64))), None);
        assert_eq!(shown(col("missing").gt(lit(1i64))), None);
        assert_eq!(
            shown(col("k").gt(lit(1i64)).and(col("price").gt(lit(1i64)))),
            None
        );
        assert_eq!(shown(col("k").like(lit("%a"))), None);
    }
}
