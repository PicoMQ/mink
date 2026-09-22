//! The lake catalog interface the coordinator creates and alters lake tables through, with none and in-memory implementations.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;
use mink_common::sync::lock;
use mink_table::{Descriptor, LakeFormat, Path, Schema};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("lake table {0} already exists")]
    TableExists(Path),
    #[error("lake table {0} does not exist to attach to")]
    TableNotFound(Path),
    #[error("table {0} enables lake tiering but no lake catalog is configured")]
    NotConfigured(Path),
    #[error("table {path} cannot be created in the lake: {reason}")]
    Invalid { path: Path, reason: String },
    #[error("lake catalog: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Created {
    pub descriptor: Option<Descriptor>,
    pub baseline_snapshot_id: Option<i64>,
}

impl Created {
    pub const FRESH: Created = Created {
        descriptor: None,
        baseline_snapshot_id: None,
    };
}

#[async_trait]
pub trait LakeCatalog: Send + Sync {
    fn format(&self) -> Option<LakeFormat>;

    async fn create_table(&self, path: &Path, descriptor: &Descriptor) -> Result<Created, Error>;

    async fn alter_table(
        &self,
        path: &Path,
        from: &Descriptor,
        to: &Descriptor,
    ) -> Result<(), Error>;
}

#[derive(Debug, Default)]
pub struct NoLakeCatalog;

#[async_trait]
impl LakeCatalog for NoLakeCatalog {
    fn format(&self) -> Option<LakeFormat> {
        None
    }

    async fn create_table(&self, path: &Path, _descriptor: &Descriptor) -> Result<Created, Error> {
        Err(Error::NotConfigured(path.clone()))
    }

    async fn alter_table(
        &self,
        path: &Path,
        _from: &Descriptor,
        _to: &Descriptor,
    ) -> Result<(), Error> {
        Err(Error::NotConfigured(path.clone()))
    }
}

#[derive(Debug, Default)]
pub struct MemoryLakeCatalog {
    created: Mutex<BTreeSet<Path>>,
    altered: Mutex<BTreeMap<Path, Descriptor>>,
    attachable: Mutex<BTreeMap<Path, (Schema, i64)>>,
}

impl MemoryLakeCatalog {
    pub fn created(&self) -> Vec<Path> {
        lock(&self.created).iter().cloned().collect()
    }

    pub fn altered(&self, path: &Path) -> Option<Descriptor> {
        lock(&self.altered).get(path).cloned()
    }

    pub fn add_existing(&self, path: &Path, schema: Schema, snapshot_id: i64) {
        lock(&self.attachable).insert(path.clone(), (schema, snapshot_id));
    }
}

#[async_trait]
impl LakeCatalog for MemoryLakeCatalog {
    fn format(&self) -> Option<LakeFormat> {
        Some(LakeFormat::Iceberg)
    }

    async fn create_table(&self, path: &Path, descriptor: &Descriptor) -> Result<Created, Error> {
        if descriptor.options().lake_attach {
            let (schema, snapshot_id) = lock(&self.attachable)
                .get(path)
                .cloned()
                .ok_or_else(|| Error::TableNotFound(path.clone()))?;
            lock(&self.created).insert(path.clone());
            let descriptor = descriptor
                .to_builder()
                .schema(schema)
                .build()
                .map_err(|e| Error::Backend(e.to_string()))?;
            return Ok(Created {
                descriptor: Some(descriptor),
                baseline_snapshot_id: Some(snapshot_id),
            });
        }
        let mut created = lock(&self.created);
        if created.insert(path.clone()) && !lock(&self.attachable).contains_key(path) {
            Ok(Created::FRESH)
        } else {
            Err(Error::TableExists(path.clone()))
        }
    }

    async fn alter_table(
        &self,
        path: &Path,
        _from: &Descriptor,
        to: &Descriptor,
    ) -> Result<(), Error> {
        if !lock(&self.created).contains(path) {
            return Err(Error::Backend(format!("lake table {path} does not exist")));
        }
        lock(&self.altered).insert(path.clone(), to.clone());

        Ok(())
    }
}
