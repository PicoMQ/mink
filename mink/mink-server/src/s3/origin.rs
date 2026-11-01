use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mink_core::{BuiltFile, File, TableLocation};
use nestor::{GetOptions, GetResponse, ObjectMeta, Origin, OriginError};
use tokio::sync::Semaphore;

use super::build::build;
use crate::Error;
use crate::runs::SharedRuns;
use crate::services::Services;

pub(super) struct FileOrigin {
    services: Arc<Services>,
    runs: SharedRuns<File, Bytes>,
    permits: Semaphore,
}

pub(super) struct BuiltOrigin {
    services: Arc<Services>,
}

impl FileOrigin {
    pub(super) fn new(services: Arc<Services>) -> FileOrigin {
        let permits = Semaphore::new(services.config.max_builds);

        FileOrigin {
            services,
            runs: SharedRuns::new(),
            permits,
        }
    }

    pub(super) async fn build(&self, file: &File) -> Result<Bytes, Error> {
        self.runs
            .run(file.clone(), async {
                let _permit = self
                    .permits
                    .acquire()
                    .await
                    .expect("the build permits are never closed");

                build(&self.services, file).await
            })
            .await
    }

    pub(super) fn building(&self) -> usize {
        self.services.config.max_builds - self.permits.available_permits()
    }
}

impl BuiltOrigin {
    pub(super) fn new(services: Arc<Services>) -> BuiltOrigin {
        BuiltOrigin { services }
    }

    async fn encoded(&self, object: &str) -> Result<Bytes, OriginError> {
        let file = file(&self.services, object)?;
        let built = stored(&self.services, &file).await?;

        Ok(mink_store::built_bytes(&built))
    }
}

#[async_trait]
impl Origin for FileOrigin {
    async fn get(&self, object: &str, _: GetOptions) -> Result<GetResponse, OriginError> {
        let file = file(&self.services, object)?;

        Ok(response(self.build(&file).await.map_err(origin_error)?))
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        let file = file(&self.services, object)?;
        let built = stored(&self.services, &file).await?;

        Ok(ObjectMeta::new(built.indexed().stats().bytes(), None))
    }
}

#[async_trait]
impl Origin for BuiltOrigin {
    async fn get(&self, object: &str, _: GetOptions) -> Result<GetResponse, OriginError> {
        Ok(response(self.encoded(object).await?))
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        Ok(ObjectMeta::new(
            self.encoded(object).await?.len() as u64,
            None,
        ))
    }
}

fn file(services: &Services, object: &str) -> Result<File, OriginError> {
    let (_, path) = object.split_once('/').ok_or(OriginError::NotFound)?;

    TableLocation::parse_data_file(services.warehouse.root(), path).ok_or(OriginError::NotFound)
}

async fn stored(services: &Services, file: &File) -> Result<BuiltFile, OriginError> {
    let built = services
        .store
        .files()
        .built(file)
        .await
        .map_err(|error| origin_error(error.into()))?;

    built.ok_or(OriginError::NotFound)
}

fn response(bytes: Bytes) -> GetResponse {
    let size = bytes.len() as u64;

    GetResponse {
        meta: ObjectMeta::new(size, None),
        range: 0..size,
        body: futures::stream::once(std::future::ready(Ok(bytes))).boxed(),
    }
}

fn origin_error(error: Error) -> OriginError {
    match error {
        Error::NoSuchKey(_) => OriginError::NotFound,
        error => OriginError::io(error),
    }
}
