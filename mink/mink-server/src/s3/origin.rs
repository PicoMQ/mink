use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mink_core::{File, TableLocation};
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

    fn file(&self, object: &str) -> Result<File, OriginError> {
        let (_, path) = object.split_once('/').ok_or(OriginError::NotFound)?;

        TableLocation::parse_data_file(self.services.warehouse.root(), path)
            .ok_or(OriginError::NotFound)
    }
}

#[async_trait]
impl Origin for FileOrigin {
    async fn get(&self, object: &str, _: GetOptions) -> Result<GetResponse, OriginError> {
        let file = self.file(object)?;

        let bytes = self.build(&file).await.map_err(origin_error)?;

        let size = bytes.len() as u64;
        Ok(GetResponse {
            meta: ObjectMeta::new(size, None),
            range: 0..size,
            body: futures::stream::once(std::future::ready(Ok(bytes))).boxed(),
        })
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        let file = self.file(object)?;

        let stored = self
            .services
            .store
            .files()
            .stats(&file)
            .await
            .map_err(|error| origin_error(error.into()))?;

        let stats = stored.ok_or(OriginError::NotFound)?;
        Ok(ObjectMeta::new(stats.bytes(), None))
    }
}

fn origin_error(error: Error) -> OriginError {
    match error {
        Error::NoSuchKey(_) => OriginError::NotFound,
        error => OriginError::io(error),
    }
}
