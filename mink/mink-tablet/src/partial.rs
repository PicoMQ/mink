//! Partial update merger: overwrites only the target columns and clears them on partial delete.

use std::sync::Arc;

use mink_record::Row;

use crate::Error;
use crate::merger::{Decoded, Merged, field};
use crate::schema::Version;
use crate::targets::Targets;
use crate::value::Value;

pub struct Updater {
    target: Arc<Version>,
    targets: Targets,
}

impl Updater {
    pub fn new(target: Arc<Version>, columns: &[usize]) -> Result<Self, Error> {
        let targets = Targets::new(
            &target,
            columns,
            target.auto_increment,
            Error::TargetNotNullable,
        )?;

        Ok(Updater { target, targets })
    }

    pub fn schema(&self) -> &Arc<Version> {
        &self.target
    }

    pub fn update(&self, old: &Decoded<'_>, partial: &Decoded<'_>) -> Result<Merged, Error> {
        if self.targets.keys_only() {
            return Ok(Merged::Old);
        }

        let written = &self.targets.written;
        let row: Row<'_> = (0..written.len())
            .map(|i| {
                if written[i] {
                    field(&partial.row, i)
                } else {
                    field(&old.row, i)
                }
            })
            .collect();

        Ok(Merged::Row(self.target.encode(&row)?))
    }

    pub fn delete(&self, old: &Decoded<'_>) -> Result<Option<Value>, Error> {
        match self.targets.after_delete(&old.row) {
            Some(row) => Ok(Some(self.target.encode(&row)?)),
            None => Ok(None),
        }
    }
}
