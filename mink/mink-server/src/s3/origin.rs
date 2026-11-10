use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mink_core::{BuiltFile, File, TableLocation};
use nestor::{GetOptions, GetResponse, ObjectMeta, Origin, OriginError};
use nestor_store::ObjectStoreOrigin;
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::build::{build, extent, path};
use crate::Error;
use crate::buckets::Buckets;
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

pub(super) struct BucketOrigin {
    buckets: Buckets,
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

    pub(super) async fn build(&self, table: Uuid, file: &File) -> Result<Bytes, Error> {
        let object = object_name(table, &path(&self.services, file), None);
        let (services, file) = (self.services.clone(), file.clone());

        self.run(object, async move { build(&services, &file).await })
            .await
    }

    pub(super) async fn extent(
        &self,
        table: Uuid,
        built: &BuiltFile,
        range: Range<u64>,
    ) -> Result<Bytes, Error> {
        let file = built.indexed().file();
        let object = object_name(table, &path(&self.services, file), Some(&range));
        let (services, built) = (self.services.clone(), built.clone());

        self.run(
            object,
            async move { extent(&services, &built, range).await },
        )
        .await
    }

    async fn run(
        &self,
        object: String,
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
            .run(object, async move {
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
        let (_, file, _) =
            parse_object(self.services.warehouse.root(), object).ok_or(OriginError::NotFound)?;
        let built = built(&self.services, &file).await?;

        Ok(mink_store::built_bytes(&built))
    }
}

impl BucketOrigin {
    pub(super) fn new(buckets: Buckets) -> BucketOrigin {
        BucketOrigin { buckets }
    }

    fn locate(&self, path: &str) -> Result<(ObjectStoreOrigin, String), OriginError> {
        let (store, key) = self
            .buckets
            .locate(path)
            .ok_or(OriginError::NotFound)?
            .map_err(origin_error)?;

        Ok((ObjectStoreOrigin::new(store), key.to_string()))
    }
}

#[async_trait]
impl Origin for FileOrigin {
    async fn get(&self, object: &str, _: GetOptions) -> Result<GetResponse, OriginError> {
        let bytes = match parse_object(self.services.warehouse.root(), object)
            .ok_or(OriginError::NotFound)?
        {
            (table, file, None) => self.build(table, &file).await,
            (table, file, Some(range)) => {
                let built = built(&self.services, &file).await?;
                self.extent(table, &built, range).await
            }
        };

        Ok(response(bytes.map_err(origin_error)?))
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        let length = match parse_object(self.services.warehouse.root(), object)
            .ok_or(OriginError::NotFound)?
        {
            (_, _, Some(range)) => range.end - range.start,
            (_, file, None) => built(&self.services, &file)
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

#[async_trait]
impl Origin for BucketOrigin {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError> {
        let (origin, key) = self.locate(object)?;

        origin.get(&key, options).await
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> {
        let (origin, key) = self.locate(object)?;

        origin.head(&key).await
    }
}

pub(super) fn object_name(table: Uuid, path: &str, extent: Option<&Range<u64>>) -> String {
    match extent {
        Some(range) => format!("{table}/{path}#{}-{}", range.start, range.end),
        None => format!("{table}/{path}"),
    }
}

fn parse_object(warehouse: &str, object: &str) -> Option<(Uuid, File, Option<Range<u64>>)> {
    let (table, name) = object.split_once('/')?;
    let (path, extent) = match name.split_once('#') {
        None => (name, None),
        Some((path, extent)) => {
            let (start, end) = extent.split_once('-')?;
            let range: Range<u64> = start.parse().ok()?..end.parse().ok()?;
            if range.is_empty() {
                return None;
            }

            (path, Some(range))
        }
    };
    let file = TableLocation::parse_data_file(warehouse, path)?;

    Some((Uuid::parse_str(table).ok()?, file, extent))
}

async fn built(services: &Services, file: &File) -> Result<BuiltFile, OriginError> {
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
    fn object_names_parse_back_into_their_table_file_and_extent() {
        let warehouse = "s3://mink/warehouse";
        let path = "s3://mink/warehouse/orders/data/p=0/0-10.t0.s0.v1.parquet";
        let file = TableLocation::parse_data_file(warehouse, path).unwrap();
        let table = Uuid::new_v4();
        let extent = 4096..8192;
        let parsed = |object: &str| parse_object(warehouse, object);

        assert_eq!(
            parsed(&object_name(table, path, Some(&extent))),
            Some((table, file.clone(), Some(extent)))
        );
        assert_eq!(
            parsed(&object_name(table, path, None)),
            Some((table, file, None))
        );
        assert_eq!(parsed(&format!("{table}/{path}#8-4")), None);
        assert_eq!(parsed(&format!("{table}/{path}#8")), None);
        assert_eq!(parsed(&format!("{table}/{path}#a-b")), None);
        assert_eq!(parsed(&format!("orders/{path}")), None);
    }
}
