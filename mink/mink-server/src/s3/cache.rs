use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mink_core::File;
use nestor::{
    CacheConfig, Consistency, DiskConfig, FetchPolicy, Namespace, NamespaceId, Nestor, NestorError,
    ReadRange, ReadStream,
};
use nestor_store::ObjectStoreOrigin;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::key::{Key, Object};
use super::origin::FileOrigin;
use crate::Error;
use crate::services::Services;

const BUILD_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const METADATA_TTL: Duration = Duration::from_secs(60);
const UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) struct Cache {
    nestor: Nestor,
    files: NamespaceId,
    metadata: NamespaceId,
    origin: Arc<FileOrigin>,
    services: Arc<Services>,
    upkeep: JoinHandle<()>,
}

pub(crate) struct Usage {
    pub(crate) memory_bytes: u64,
    pub(crate) memory_max_bytes: u64,
    pub(crate) disk_max_bytes: Option<u64>,
    pub(crate) building: usize,
}

impl Cache {
    pub(crate) async fn open(services: Arc<Services>) -> Result<Cache, Error> {
        let config = &services.config;
        let origin = Arc::new(FileOrigin::new(services.clone()));

        let mut cache = CacheConfig::memory(config.cache_bytes as usize);
        if let Some(dir) = &config.cache_dir {
            let mut disk = DiskConfig::new(dir, config.cache_disk_bytes as usize);
            disk.admission = config.cache_admission;
            cache = cache.disk(disk);
        }

        let builds = FetchPolicy::default()
            .attempts(1)
            .hedge(None)
            .first_byte(BUILD_TIMEOUT)
            .attempt(BUILD_TIMEOUT)
            .deadline(BUILD_TIMEOUT);
        let files = Namespace::new("files", origin.clone())
            .consistency(Consistency::Immutable)
            .readahead(0)
            .fetch(builds);
        let metadata = Namespace::new(
            "metadata",
            Arc::new(ObjectStoreOrigin::new(services.warehouse.store())),
        )
        .consistency(Consistency::Etag { ttl: METADATA_TTL });

        let nestor = Nestor::builder(cache).build().await.map_err(Error::Cache)?;
        let (files, metadata) = services
            .metrics
            .with_recorder(|| (nestor.register(files), nestor.register(metadata)));

        let metrics = services.metrics.clone();
        let upkeep = tokio::spawn(async move {
            loop {
                tokio::time::sleep(UPKEEP_INTERVAL).await;
                metrics.upkeep();
            }
        });

        Ok(Cache {
            nestor,
            files,
            metadata,
            origin,
            services,
            upkeep,
        })
    }

    pub(crate) async fn read(
        &self,
        key: &Key,
        table: Uuid,
        range: ReadRange,
    ) -> Result<ReadStream, Error> {
        let (namespace, object) = self.object(key, table)?;

        let mut stream = self
            .nestor
            .get(namespace, &object, range)
            .await
            .map_err(|error| cache_error(key, error))?;

        stream
            .ready()
            .await
            .map_err(|error| cache_error(key, error))?;
        Ok(stream)
    }

    pub(crate) async fn length(&self, key: &Key, table: Uuid) -> Result<u64, Error> {
        if let Object::Data(_) = key.object() {
            let stream = self.read(key, table, ReadRange::Full).await?;

            return Ok(stream.range().end);
        }

        let (namespace, object) = self.object(key, table)?;

        let meta = self
            .nestor
            .head(namespace, &object)
            .await
            .map_err(|error| cache_error(key, error))?;
        Ok(meta.size)
    }

    pub(crate) async fn build(&self, file: &File) -> Result<Bytes, Error> {
        self.origin.build(file).await
    }

    pub(crate) fn usage(&self) -> Usage {
        let snapshot = self.nestor.snapshot();

        Usage {
            memory_bytes: snapshot.cache.memory_used as u64,
            memory_max_bytes: snapshot.cache.memory_cap as u64,
            disk_max_bytes: snapshot.cache.disk_cap.map(|bytes| bytes as u64),
            building: self.origin.building(),
        }
    }

    pub(crate) async fn close(&self) -> Result<(), Error> {
        self.upkeep.abort();

        self.nestor.close().await.map_err(Error::Cache)
    }

    fn object(&self, key: &Key, table: Uuid) -> Result<(NamespaceId, String), Error> {
        match key.object() {
            Object::Data(_) => Ok((self.files, format!("{table}/{}", key.path()))),
            Object::Metadata { .. } => {
                let object = self.services.warehouse.key(key.path())?;

                Ok((self.metadata, object.to_string()))
            }
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        self.upkeep.abort();
    }
}

fn cache_error(key: &Key, error: NestorError) -> Error {
    match error {
        NestorError::Range(_, size) => Error::Unsatisfiable(size),
        error if error.is_not_found() => Error::NoSuchKey(key.path().to_owned()),
        error => Error::Cache(error),
    }
}
