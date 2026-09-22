//! Filters that pin columns to literal values (`= literal`, `IN (literals)`, disjunctions of those on
//! one column and conjunctions of any of them), and the typed key rows they enumerate for routing to
//! partitions and buckets or looking up by key.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

use crate::error::Result;
use crate::predicate::Column;

#[derive(Debug, Default)]
pub struct Constraints {
    pinned: BTreeMap<String, Vec<ScalarValue>>,
}

impl Constraints {
    pub fn new(filters: &[Expr], schema: &Schema) -> Self {
        let mut constraints = Constraints::default();
        for filter in filters {
            if let Some(pins) = pins(filter, schema) {
                for (column, values) in pins {
                    constraints.intersect(column, values);
                }
            }
        }

        constraints
    }

    /// The columns a filter pins, if the filter is made only of pins.
    pub fn pinned_by(filter: &Expr, schema: &Schema) -> Option<BTreeSet<String>> {
        Some(pins(filter, schema)?.into_iter().map(|(c, _)| c).collect())
    }

    pub fn pins_any(&self, columns: &[String]) -> bool {
        columns.iter().any(|c| self.pinned.contains_key(c))
    }

    /// Every combination of the pinned values of `columns`, as rows typed by `schema`, or `None`
    /// when a column is unpinned or there are more than `cap` combinations.
    pub fn rows(
        &self,
        columns: &[String],
        schema: &Schema,
        cap: usize,
    ) -> Result<Option<RecordBatch>> {
        let mut total: usize = 1;
        let mut pinned = Vec::with_capacity(columns.len());
        for column in columns {
            let Some(values) = self.pinned.get(column) else {
                return Ok(None);
            };
            total = total.saturating_mul(values.len());
            pinned.push(values);
        }
        if total > cap {
            return Ok(None);
        }

        let fields: Vec<_> = columns
            .iter()
            .map(|c| schema.field_with_name(c).cloned())
            .collect::<std::result::Result<_, _>>()?;
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        let mut repeat = total;
        for (values, field) in pinned.into_iter().zip(schema.fields()) {
            if total == 0 {
                arrays.push(arrow_array::new_empty_array(field.data_type()));
                continue;
            }
            repeat /= values.len();
            let cycle = values
                .iter()
                .flat_map(|v| std::iter::repeat_n(v.clone(), repeat));
            let column: Vec<ScalarValue> = cycle.cycle().take(total).collect();
            arrays.push(ScalarValue::iter_to_array(column)?);
        }

        Ok(Some(RecordBatch::try_new_with_options(
            schema,
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(total)),
        )?))
    }

    fn intersect(&mut self, column: String, values: Vec<ScalarValue>) {
        match self.pinned.get_mut(&column) {
            Some(existing) => existing.retain(|v| values.contains(v)),
            None => {
                self.pinned.insert(column, values);
            }
        }
    }
}

fn pins(filter: &Expr, schema: &Schema) -> Option<Vec<(String, Vec<ScalarValue>)>> {
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                let mut both = pins(left, schema)?;
                both.extend(pins(right, schema)?);
                Some(both)
            }
            Operator::Or => {
                let (mut left, right) = (pins(left, schema)?, pins(right, schema)?);
                match (left.as_mut_slice(), right.as_slice()) {
                    ([(column, values)], [(other, more)]) if column == other => {
                        for value in more {
                            if !values.contains(value) {
                                values.push(value.clone());
                            }
                        }
                        Some(left)
                    }
                    _ => None,
                }
            }
            Operator::Eq => {
                let (column, value) = match (left.as_ref(), right.as_ref()) {
                    (column, Expr::Literal(value, _)) | (Expr::Literal(value, _), column) => {
                        (Column::resolve(column, schema)?, value)
                    }
                    _ => return None,
                };
                Some(vec![(column.name.to_owned(), vec![column.literal(value)?])])
            }
            _ => None,
        },
        Expr::InList(list) if !list.negated => {
            let column = Column::resolve(&list.expr, schema)?;
            let mut values: Vec<ScalarValue> = Vec::with_capacity(list.list.len());
            for item in &list.list {
                let Expr::Literal(value, _) = item else {
                    return None;
                };
                let value = column.literal(value)?;
                if !values.contains(&value) {
                    values.push(value);
                }
            }
            Some(vec![(column.name.to_owned(), values)])
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_schema::{DataType, Field};
    use datafusion::logical_expr::{Cast, col, lit};

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ])
    }

    fn keys() -> Vec<String> {
        vec!["region".into(), "k".into()]
    }

    fn column(batch: &RecordBatch, name: &str) -> Vec<String> {
        let index = batch.schema().index_of(name).unwrap();
        let array = batch.column(index);
        (0..batch.num_rows())
            .map(|row| match array.data_type() {
                DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
                _ => array.as_string::<i32>().value(row).to_owned(),
            })
            .collect()
    }

    #[test]
    fn equality_and_in_lists_pin_columns_in_every_combination() {
        let filters = [
            col("region").in_list(vec![lit("eu"), lit("us")], false),
            col("k").eq(lit(5i32)),
        ];
        let constraints = Constraints::new(&filters, &schema());
        let rows = constraints.rows(&keys(), &schema(), 16).unwrap().unwrap();
        assert_eq!(column(&rows, "region"), ["eu", "us"]);
        assert_eq!(column(&rows, "k"), ["5", "5"]);
        assert_eq!(rows.schema().field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn disjunctions_on_one_column_union_its_values() {
        let filters = [col("k")
            .eq(lit(1i64))
            .or(col("k").eq(lit(2i64)))
            .or(col("k").in_list(vec![lit(2i64), lit(3i64)], false))];
        let rows = Constraints::new(&filters, &schema())
            .rows(&["k".to_owned()], &schema(), 16)
            .unwrap()
            .unwrap();
        assert_eq!(column(&rows, "k"), ["1", "2", "3"]);
        let mixed = col("k")
            .eq(lit(1i64))
            .or(col("k").eq(lit(2i64)).and(col("region").eq(lit("eu"))));
        assert!(Constraints::pinned_by(&mixed, &schema()).is_none());
    }

    #[test]
    fn conjunctions_and_repeated_columns_intersect() {
        let filters = [
            lit("eu")
                .eq(col("region"))
                .and(col("k").in_list(vec![lit(1i64), lit(2i64), lit(3i64)], false)),
            col("k").in_list(vec![lit(2i64), lit(3i64), lit(4i64)], false),
        ];
        let constraints = Constraints::new(&filters, &schema());
        let rows = constraints.rows(&keys(), &schema(), 16).unwrap().unwrap();
        assert_eq!(column(&rows, "k"), ["2", "3"]);

        let disjoint = [col("k").eq(lit(1i64)), col("k").eq(lit(2i64))];
        let rows = Constraints::new(&disjoint, &schema())
            .rows(&["k".to_owned()], &schema(), 16)
            .unwrap()
            .unwrap();
        assert_eq!(rows.num_rows(), 0, "nothing satisfies both");
    }

    #[test]
    fn widening_casts_on_the_key_still_pin_it() {
        let narrowed = Expr::Cast(Cast::new(Box::new(col("k")), DataType::Int32));
        assert!(Constraints::pinned_by(&narrowed.eq(lit(7i32)), &schema()).is_none());
        let widened = Expr::Cast(Cast::new(Box::new(col("region")), DataType::LargeUtf8));
        let filters = [widened.eq(lit(ScalarValue::LargeUtf8(Some("eu".into()))))];
        let rows = Constraints::new(&filters, &schema())
            .rows(&["region".to_owned()], &schema(), 16)
            .unwrap()
            .unwrap();
        assert_eq!(rows.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(column(&rows, "region"), ["eu"]);
    }

    #[test]
    fn unpinned_columns_the_cap_and_other_shapes_give_nothing() {
        let constraints = Constraints::new(&[col("region").eq(lit("eu"))], &schema());
        assert!(constraints.pins_any(&keys()));
        assert!(constraints.rows(&keys(), &schema(), 16).unwrap().is_none());

        let many: Vec<Expr> = (0..20).map(|i| lit(i as i64)).collect();
        let constraints = Constraints::new(&[col("k").in_list(many, false)], &schema());
        assert!(
            constraints
                .rows(&["k".to_owned()], &schema(), 16)
                .unwrap()
                .is_none()
        );

        for filter in [
            col("k").gt(lit(1i64)),
            col("k").eq(lit(1i64)).or(col("region").eq(lit("eu"))),
            col("k").in_list(vec![lit(1i64)], true),
            col("k").eq(col("region")),
            col("k").eq(lit(ScalarValue::Int64(None))),
        ] {
            assert!(
                Constraints::pinned_by(&filter, &schema()).is_none(),
                "{filter}"
            );
        }
        assert_eq!(
            Constraints::pinned_by(
                &col("k").eq(lit(1i64)).and(col("region").eq(lit("eu"))),
                &schema()
            ),
            Some(BTreeSet::from(["k".to_owned(), "region".to_owned()]))
        );
    }
}
