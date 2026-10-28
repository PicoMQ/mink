use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use mink_core::{Offsets, Snapshot, Topic};
use mink_iceberg::Table;

use super::tail;
use crate::Error;
use crate::runs::SharedRuns;
use crate::services::Services;

pub(crate) struct Tables {
    services: Arc<Services>,
    cached: Mutex<HashMap<String, Cached>>,
    tails: SharedRuns<Vec<Offsets>, MetadataFile>,
}

#[derive(Clone)]
pub(crate) struct MetadataFile {
    location: String,
    bytes: Bytes,
}

#[derive(Clone)]
struct Cached {
    snapshot: Snapshot,
    table: Table,
    tail: Option<Tail>,
}

#[derive(Clone)]
struct Tail {
    offsets: Vec<Offsets>,
    metadata: MetadataFile,
}

impl MetadataFile {
    pub(super) fn new(location: String, bytes: Bytes) -> MetadataFile {
        MetadataFile { location, bytes }
    }

    pub(crate) fn location(&self) -> &str {
        &self.location
    }

    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

impl Tables {
    pub(crate) fn new(services: Arc<Services>) -> Tables {
        Tables {
            services,
            cached: Mutex::new(HashMap::new()),
            tails: SharedRuns::new(),
        }
    }

    pub(crate) async fn names(&self) -> Result<Vec<String>, Error> {
        Ok(self.services.store.snapshots().topics().await?)
    }

    pub(crate) async fn exists(&self, name: &str) -> Result<bool, Error> {
        let store = &self.services.store;

        if store.topics().get(name).await?.is_none() {
            return Ok(false);
        }
        Ok(store.snapshots().latest(name).await?.is_some())
    }

    pub(crate) async fn load(&self, name: &str) -> Result<MetadataFile, Error> {
        let store = &self.services.store;
        let not_found = || Error::NoSuchTable(name.to_owned());
        let topic = store.topics().get(name).await?.ok_or_else(not_found)?;
        let snapshot = store
            .snapshots()
            .latest(name)
            .await?
            .ok_or_else(not_found)?;
        let cached = self.cached(&topic, &snapshot).await?;

        let offsets = tail::offsets(&self.services, &snapshot).await?;
        if offsets == snapshot.offsets() {
            let location = cached.table.metadata_location(&snapshot);
            let bytes = self.services.warehouse.read(&location).await?;
            return Ok(MetadataFile { location, bytes });
        }
        if let Some(tail) = cached.tail.filter(|tail| tail.offsets == offsets) {
            return Ok(tail.metadata);
        }

        let built = tail::build(
            &self.services,
            name,
            &cached.table,
            &snapshot,
            offsets.clone(),
        );
        let metadata = self.tails.run(offsets.clone(), built).await?;
        self.remember(
            name,
            &snapshot,
            Tail {
                offsets,
                metadata: metadata.clone(),
            },
        );
        Ok(metadata)
    }

    async fn cached(&self, topic: &Topic, snapshot: &Snapshot) -> Result<Cached, Error> {
        let cached = self
            .cached
            .lock()
            .expect("the table cache is never poisoned")
            .get(topic.name())
            .filter(|cached| cached.snapshot == *snapshot)
            .cloned();
        if let Some(cached) = cached {
            return Ok(cached);
        }

        let table = self.services.warehouse.load_table(topic, snapshot).await?;
        let cached = Cached {
            snapshot: snapshot.clone(),
            table,
            tail: None,
        };

        self.cached
            .lock()
            .expect("the table cache is never poisoned")
            .insert(topic.name().to_owned(), cached.clone());
        Ok(cached)
    }

    fn remember(&self, name: &str, snapshot: &Snapshot, tail: Tail) {
        let mut cached = self
            .cached
            .lock()
            .expect("the table cache is never poisoned");

        if let Some(cached) = cached
            .get_mut(name)
            .filter(|cached| cached.snapshot == *snapshot)
        {
            cached.tail = Some(tail);
        }
    }
}
