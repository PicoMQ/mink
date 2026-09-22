//! An ordered list of fields with unique names, plus lookup, projection and id assignment.

use std::collections::HashSet;
use std::ops::Deref;
use std::slice;

use crate::{Error, Field, FieldId};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fields(Vec<Field>);

impl Fields {
    pub fn new(fields: Vec<Field>) -> Result<Self, Error> {
        let mut seen = HashSet::with_capacity(fields.len());
        for field in &fields {
            if !seen.insert(field.name()) {
                return Err(Error::DuplicateFieldName(field.name().to_owned()));
            }
        }

        Ok(Fields(fields))
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.0.iter().position(|field| field.name() == name)
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.0.iter().find(|field| field.name() == name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(Field::name)
    }

    pub fn project(&self, indices: &[usize]) -> Result<Fields, Error> {
        let count = self.0.len();
        let projected = indices
            .iter()
            .map(|&index| {
                self.0
                    .get(index)
                    .cloned()
                    .ok_or(Error::FieldIndex { index, count })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Fields::new(projected)
    }

    pub fn assign_ids(&self, next: &mut u32) -> Fields {
        let assigned = self
            .0
            .iter()
            .map(|field| {
                let id = FieldId(*next);
                *next += 1;
                let data_type = field.data_type().assign_field_ids(next);
                field.clone().with_id(id).with_data_type(data_type)
            })
            .collect();
        Fields(assigned)
    }

    pub fn into_inner(self) -> Vec<Field> {
        self.0
    }
}

impl Deref for Fields {
    type Target = [Field];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for &'a Fields {
    type Item = &'a Field;
    type IntoIter = slice::Iter<'a, Field>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataType;

    fn field(name: &str) -> Field {
        Field::new(name, DataType::int()).unwrap()
    }

    #[test]
    fn names_must_be_unique() {
        let err = Fields::new(vec![field("a"), field("b"), field("a")]).unwrap_err();
        assert_eq!(err, Error::DuplicateFieldName("a".into()));
    }

    #[test]
    fn lookup_by_name() {
        let fields = Fields::new(vec![field("a"), field("b")]).unwrap();
        assert_eq!(fields.index_of("b"), Some(1));
        assert_eq!(fields.index_of("c"), None);
        assert_eq!(fields.field("a").map(Field::name), Some("a"));
        assert_eq!(fields.names().collect::<Vec<_>>(), ["a", "b"]);
    }

    #[test]
    fn project_reorders_and_validates() {
        let fields = Fields::new(vec![field("a"), field("b"), field("c")]).unwrap();
        let projected = fields.project(&[2, 0]).unwrap();
        assert_eq!(projected.names().collect::<Vec<_>>(), ["c", "a"]);
        assert_eq!(
            fields.project(&[3]).unwrap_err(),
            Error::FieldIndex { index: 3, count: 3 }
        );
        assert_eq!(
            fields.project(&[1, 1]).unwrap_err(),
            Error::DuplicateFieldName("b".into())
        );
    }

    #[test]
    fn assign_ids_is_depth_first_in_declaration_order() {
        let inner = Fields::new(vec![field("x"), field("y")]).unwrap();
        let outer = Fields::new(vec![
            field("a"),
            Field::new("nested", DataType::row(inner)).unwrap(),
            field("b"),
        ])
        .unwrap();

        let mut next = 0;
        let assigned = outer.assign_ids(&mut next);
        assert_eq!(next, 5);
        assert_eq!(assigned[0].id(), Some(FieldId(0)));
        assert_eq!(assigned[1].id(), Some(FieldId(1)));
        let crate::Kind::Row(nested) = assigned[1].data_type().kind() else {
            panic!("expected row");
        };
        assert_eq!(nested[0].id(), Some(FieldId(2)));
        assert_eq!(nested[1].id(), Some(FieldId(3)));
        assert_eq!(assigned[2].id(), Some(FieldId(4)));
    }
}
