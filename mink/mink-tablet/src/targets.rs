//! The target column set of a partial write: which columns are written, which are keys, and the
//! rules a target set must satisfy, plus the row a partial delete leaves behind.

use mink_record::Row;

use crate::Error;
use crate::merger::field;
use crate::schema::Version;

pub(crate) struct Targets {
    pub(crate) written: Vec<bool>,
    pub(crate) keys: Vec<bool>,
}

impl Targets {
    pub(crate) fn new(
        version: &Version,
        columns: &[usize],
        exempt: Option<usize>,
        not_nullable: fn(String) -> Error,
    ) -> Result<Self, Error> {
        let width = version.field_count();
        let mut written = vec![false; width];
        for &column in columns {
            *written.get_mut(column).ok_or(Error::TargetIndex(column))? = true;
        }

        let keys = key_flags(version);
        let fields = version.schema.fields();
        if version.key_indexes.iter().any(|&index| !written[index]) {
            let names = |indexes: &[usize]| -> Vec<String> {
                indexes
                    .iter()
                    .map(|&i| fields[i].name().to_owned())
                    .collect()
            };
            return Err(Error::TargetsMissKey {
                targets: names(columns),
                keys: names(&version.key_indexes),
            });
        }

        let required = fields
            .iter()
            .enumerate()
            .find(|(i, f)| !keys[*i] && exempt != Some(*i) && !f.data_type().is_nullable());
        if let Some((_, field)) = required {
            return Err(not_nullable(field.name().to_owned()));
        }

        Ok(Targets { written, keys })
    }

    pub(crate) fn keys_only(&self) -> bool {
        self.written == self.keys
    }

    pub(crate) fn after_delete<'a>(&self, old: &Row<'a>) -> Option<Row<'a>> {
        let others_null = (0..self.written.len())
            .filter(|&i| !self.written[i])
            .all(|i| field(old, i).is_none());
        if others_null {
            return None;
        }

        let row = (0..self.written.len())
            .map(|i| {
                if !self.keys[i] && self.written[i] {
                    None
                } else {
                    field(old, i)
                }
            })
            .collect();

        Some(row)
    }
}

pub(crate) fn key_flags(version: &Version) -> Vec<bool> {
    let mut keys = vec![false; version.field_count()];
    for &index in &version.key_indexes {
        keys[index] = true;
    }

    keys
}
