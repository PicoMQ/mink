//! Commit target over the in-memory catalog, applying updates and requirements locally.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, Runtime, TableIdent, TableRequirement, TableUpdate};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::iceberg::commit::{CommitTarget, METADATA_DIR};

pub(crate) struct MemoryCommitTarget {
    catalog: Arc<dyn Catalog>,
    tables: Mutex<HashMap<TableIdent, Table>>,
}

impl MemoryCommitTarget {
    pub(crate) fn new(catalog: Arc<dyn Catalog>) -> Self {
        MemoryCommitTarget {
            catalog,
            tables: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl CommitTarget for MemoryCommitTarget {
    async fn load(&self, ident: &TableIdent) -> Result<Table> {
        if let Some(table) = self.tables.lock().await.get(ident) {
            return Ok(table.clone());
        }
        Ok(self.catalog.load_table(ident).await?)
    }

    async fn commit(
        &self,
        ident: &TableIdent,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<Table> {
        let mut tables = self.tables.lock().await;
        let current = match tables.get(ident) {
            Some(table) => table.clone(),
            None => self.catalog.load_table(ident).await?,
        };
        for requirement in &requirements {
            requirement
                .check(Some(current.metadata()))
                .map_err(|e| Error::CommitConflict(e.to_string()))?;
        }
        let mut builder = current
            .metadata()
            .clone()
            .into_builder(current.metadata_location().map(str::to_string));
        for update in updates {
            builder = update.apply(builder)?;
        }

        let metadata = builder.build()?.metadata;
        let location = format!(
            "{}/{METADATA_DIR}/{:05}-{}.metadata.json",
            metadata.location(),
            metadata.snapshots().len(),
            Uuid::now_v7()
        );
        let table = Table::builder()
            .identifier(ident.clone())
            .file_io(current.file_io().clone())
            .metadata(metadata)
            .metadata_location(location)
            .runtime(Runtime::try_current()?)
            .build()?;
        tables.insert(ident.clone(), table.clone());

        Ok(table)
    }
}
