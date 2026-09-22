//! Collects change rows during a put and builds the log batch that records them.

use std::sync::Arc;

use mink_record::{ChangeType, Codec, Rows, Scalar, Spec};

use crate::Error;
use crate::schema::Version;

pub(crate) struct Changelog {
    schema: Arc<Version>,
    rows: Rows,
    changes: Vec<ChangeType>,
}

impl Changelog {
    pub(crate) fn new(schema: Arc<Version>, capacity: usize) -> Result<Self, Error> {
        let rows = Rows::new(schema.schema.fields(), capacity)?;

        Ok(Changelog {
            schema,
            rows,
            changes: Vec::with_capacity(capacity),
        })
    }

    pub(crate) fn append(
        &mut self,
        change: ChangeType,
        row: &[Option<Scalar<'_>>],
    ) -> Result<(), Error> {
        self.rows.push(row)?;
        self.changes.push(change);

        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub(crate) fn build(
        &mut self,
        writer_id: i64,
        batch_sequence: i32,
        codec: &dyn Codec,
    ) -> Result<Vec<u8>, Error> {
        let batch = self.rows.finish()?;
        let spec = Spec::new(self.schema.id, false).with_writer(writer_id, batch_sequence);
        let bytes = mink_record::build(spec, &self.changes, &batch, codec)?;
        self.changes.clear();

        Ok(bytes)
    }
}
