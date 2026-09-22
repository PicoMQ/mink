//! Schema versions of one table read from the metadata views, cached as they appear.

use std::sync::{Arc, Mutex};

use mink_common::sync::lock;
use mink_metadata::ViewPublisher;
use mink_table::{Id, Schema, SchemaId};

pub struct Schemas {
    views: Arc<ViewPublisher>,
    id: Id,
    seen: Mutex<Vec<Arc<Schema>>>,
}

impl Schemas {
    pub fn new(views: Arc<ViewPublisher>, table_id: Id) -> Option<Self> {
        let seen: Vec<Arc<Schema>> = views
            .load()
            .state
            .catalog
            .table_by_id(table_id)?
            .schemas
            .iter()
            .map(|s| Arc::new(s.clone()))
            .collect();

        Some(Schemas {
            views,
            id: table_id,
            seen: Mutex::new(seen),
        })
    }

    fn sync(&self) -> Vec<Arc<Schema>> {
        let mut seen = lock(&self.seen);
        let view = self.views.load();
        if let Some(table) = view.state.catalog.table_by_id(self.id) {
            for schema in &table.schemas[seen.len()..] {
                seen.push(Arc::new(schema.clone()));
            }
        }

        seen.clone()
    }
}

impl mink_tablet::Schemas for Schemas {
    fn latest(&self) -> (SchemaId, Arc<Schema>) {
        let seen = self.sync();
        let latest = seen.last().expect("populated on construction");

        (SchemaId((seen.len() - 1) as u32), latest.clone())
    }

    fn get(&self, id: SchemaId) -> Option<Arc<Schema>> {
        let index = id.0 as usize;
        if let Some(schema) = lock(&self.seen).get(index) {
            return Some(schema.clone());
        }

        self.sync().get(index).cloned()
    }
}
