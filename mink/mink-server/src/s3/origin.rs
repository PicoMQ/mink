use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mink_core::{BuiltFile, File, TableLocation};
use nestor::{GetOptions, GetResponse, ObjectMeta, Origin, OriginError};
use tokio::sync::Semaphore;

use super::build::{build, extent, path};
use crate::Error;
use crate::runs::SharedRuns;
use crate::services::Services;

pub(super) struct FileOrigin {
    services: Arc<Services>,
    runs: SharedRuns<String, Bytes>,
    permits: Arc<Semaphore>,
}

pub(super) struct BuiltOrigin {
    services: Arc<Services>,
}

impl FileOrigin {
    pub(super) fn new(services: Arc<Services>) -> FileOrigin {
        let permits = Arc::new(Semaphore::new(services.config.max_builds));

        FileOrigin {
            services,
            runs: SharedRuns::new(),
            permits,
        }
    }

    pub(super) async fn build(&self, file: &File) -> Result<Bytes, Error> {
        let name = path(&self.services, file);
        let (services, file) = (self.services.clone(), file.clone());

        self.spawned(name, async move { build(&services, &file).await })
            .await
    }

    pub(super) async fn extent(
        &self,
        built: &BuiltFile,
        range: Range<u64>,
    ) -> Result<Bytes, Error> {
        let name = extent_name(&path(&self.services, built.indexed().file()), &range);
        let (services, built) = (self.services.clone(), built.clone());

        self.spawned(name, async move { extent(&services, &built, range).await })
            .await
    }

    async fn spawned(
        &self,
        name: String,
        work: impl Future<Output = Result<Bytes, Error>> + Send + 'static,
    ) -> Result<Bytes, Error> {
        let permits = self.permits.clone();
        let task = async move {
            let _permit = permits
                .acquire_owned()
                .await
                .expect("the build permits are never closed");

            work.await
        };

        self.runs
            .run(name, async move {
                tokio::spawn(task)
                    .await
                    .map_err(|error| Error::Io(error.into()))?
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
        let (file, _) = located(&self.services, object)?;
        let built = stored(&self.services, &file).await?;

        Ok(mink_store::built_bytes(&built))
    }
}

#[async_trait]
impl Origin for FileOrigin {
    async fn get(&self, object: &str, _: GetOptions) -> Result<GetResponse, OriginError> {
        let bytes = match located(&self.services, object)? {
            (file, None) => self.build(&file).await,
            (file, Some(range)) => {
                let built = stored(&self.services, &file).await?;
                self.extent(&built, range).await
            }
        };

        Ok(response(bytes.map_err(origin_error)?))
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        let length = match located(&self.services, object)? {
            (_, Some(range)) => range.end - range.start,
            (file, None) => stored(&self.services, &file)
                .await?
                .indexed()
                .stats()
                .bytes(),
        };

        Ok(ObjectMeta::new(length, None))
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

pub(super) fn extent_name(path: &str, range: &Range<u64>) -> String {
    format!("{path}#{}-{}", range.start, range.end)
}

fn located(services: &Services, object: &str) -> Result<(File, Option<Range<u64>>), OriginError> {
    let (_, name) = object.split_once('/').ok_or(OriginError::NotFound)?;
    let (path, extent) = split(name).ok_or(OriginError::NotFound)?;
    let file = TableLocation::parse_data_file(services.warehouse.root(), path)
        .ok_or(OriginError::NotFound)?;

    Ok((file, extent))
}

fn split(name: &str) -> Option<(&str, Option<Range<u64>>)> {
    let Some((path, extent)) = name.split_once('#') else {
        return Some((name, None));
    };
    let (start, end) = extent.split_once('-')?;
    let range = start.parse().ok()?..end.parse().ok()?;

    (range.start < range.end).then_some((path, Some(range)))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extent_names_split_back_into_their_path_and_range() {
        let path = "s3://mink/warehouse/orders/data/p=0/0-10.t0.s0.v1.parquet";

        assert_eq!(
            split(&extent_name(path, &(4096..8192))),
            Some((path, Some(4096..8192)))
        );
        assert_eq!(split(path), Some((path, None)));
        assert_eq!(split(&format!("{path}#8-4")), None);
        assert_eq!(split(&format!("{path}#8")), None);
        assert_eq!(split(&format!("{path}#a-b")), None);
    }
}
