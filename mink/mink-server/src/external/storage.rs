use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::buckets::{Buckets, bucket_and_key};

const PARTS_IN_FLIGHT: usize = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct S3StorageFactory {
    buckets: Buckets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct S3Storage {
    buckets: Buckets,
}

struct S3FileRead {
    store: Arc<dyn ObjectStore>,
    key: Path,
}

struct S3FileWrite {
    writer: Option<BufWriter>,
}

impl S3StorageFactory {
    pub(crate) fn new(buckets: Buckets) -> S3StorageFactory {
        S3StorageFactory { buckets }
    }
}

#[typetag::serde]
impl StorageFactory for S3StorageFactory {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(S3Storage {
            buckets: self.buckets.clone(),
        }))
    }
}

impl S3Storage {
    fn locate(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
        let (bucket, key) = bucket_and_key(path).ok_or_else(|| not_s3(path))?;
        let store = self.buckets.client(bucket).map_err(failed)?;

        Ok((store, key))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for S3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (store, key) = self.locate(path)?;

        match store.head(&key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(failed(error)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (store, key) = self.locate(path)?;

        let meta = store.head(&key).await.map_err(failed)?;
        Ok(FileMetadata { size: meta.size })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (store, key) = self.locate(path)?;

        let read = store.get(&key).await.map_err(failed)?;
        read.bytes().await.map_err(failed)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (store, key) = self.locate(path)?;

        Ok(Box::new(S3FileRead { store, key }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (store, key) = self.locate(path)?;

        store
            .put(&key, PutPayload::from(bs))
            .await
            .map_err(failed)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (store, key) = self.locate(path)?;

        let writer = BufWriter::new(store, key).with_max_concurrency(PARTS_IN_FLIGHT);
        Ok(Box::new(S3FileWrite {
            writer: Some(writer),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (store, key) = self.locate(path)?;

        match store.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(failed(error)),
        }
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (store, key) = self.locate(path)?;

        let listed = store
            .list(Some(&key))
            .map(|object| object.map(|object| object.location))
            .boxed();
        let mut deleted = store.delete_stream(listed);
        while let Some(result) = deleted.next().await {
            result.map_err(failed)?;
        }

        Ok(())
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let mut keys: HashMap<String, (Arc<dyn ObjectStore>, Vec<Path>)> = HashMap::new();
        while let Some(path) = paths.next().await {
            let (bucket, key) = bucket_and_key(&path).ok_or_else(|| not_s3(&path))?;

            if !keys.contains_key(bucket) {
                let store = self.buckets.client(bucket).map_err(failed)?;
                keys.insert(bucket.to_owned(), (store, Vec::new()));
            }
            if let Some((_, bucket_keys)) = keys.get_mut(bucket) {
                bucket_keys.push(key);
            }
        }

        for (store, keys) in keys.into_values() {
            let listed = futures::stream::iter(keys.into_iter().map(Ok)).boxed();
            let mut deleted = store.delete_stream(listed);
            while let Some(result) = deleted.next().await {
                match result {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => return Err(failed(error)),
                }
            }
        }

        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}

#[async_trait]
impl FileRead for S3FileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store.get_range(&self.key, range).await.map_err(failed)
    }
}

impl Drop for S3FileWrite {
    fn drop(&mut self) {
        if let Some(mut writer) = self.writer.take() {
            tokio::spawn(async move {
                if let Err(error) = writer.abort().await {
                    tracing::warn!(%error, "an unfinished upload was not aborted");
                }
            });
        }
    }
}

#[async_trait]
impl FileWrite for S3FileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let writer = self.writer.as_mut().ok_or_else(closed)?;

        writer.put(bs).await.map_err(failed)
    }

    async fn close(&mut self) -> Result<()> {
        let mut writer = self.writer.take().ok_or_else(closed)?;

        writer.shutdown().await.map_err(failed)
    }
}

fn failed(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, error.to_string())
}

fn not_s3(path: &str) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        format!("{path} is not an s3:// path"),
    )
}

fn closed() -> Error {
    Error::new(ErrorKind::Unexpected, "the file is already closed")
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::StreamExt;
    use iceberg::io::{StorageConfig, StorageFactory};
    use uuid::Uuid;

    use super::S3StorageFactory;
    use crate::buckets::Buckets;
    use crate::testing::storage;

    #[tokio::test]
    async fn files_are_written_read_and_deleted_through_object_store() {
        let Some(endpoint) = mink_conformance::service("TEST_S3_ENDPOINT") else {
            return;
        };
        let factory = S3StorageFactory::new(Buckets::new(storage(endpoint)));
        let s3 = factory.build(&StorageConfig::new()).unwrap();
        let path = format!("s3://mink/storage/{}.bin", Uuid::new_v4().simple());

        let mut writer = s3.writer(&path).await.unwrap();
        writer.write(Bytes::from_static(b"hello ")).await.unwrap();
        writer.write(Bytes::from_static(b"world")).await.unwrap();
        writer.close().await.unwrap();

        let reader = s3.reader(&path).await.unwrap();
        assert_eq!(s3.read(&path).await.unwrap(), "hello world");
        assert_eq!(reader.read(6..11).await.unwrap(), "world");
        assert_eq!(s3.metadata(&path).await.unwrap().size, 11);
        assert!(s3.exists(&path).await.unwrap());

        s3.delete(&path).await.unwrap();
        assert!(!s3.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn many_files_are_deleted_in_bulk() {
        let Some(endpoint) = mink_conformance::service("TEST_S3_ENDPOINT") else {
            return;
        };
        let s3 = S3StorageFactory::new(Buckets::new(storage(endpoint)))
            .build(&StorageConfig::new())
            .unwrap();
        let prefix = format!("s3://mink/storage/{}", Uuid::new_v4().simple());
        let paths: Vec<String> = (0..3)
            .map(|index| format!("{prefix}/{index}.bin"))
            .collect();
        for path in &paths {
            s3.write(path, Bytes::from_static(b"x")).await.unwrap();
        }

        s3.delete_stream(futures::stream::iter(paths.clone()).boxed())
            .await
            .unwrap();

        for path in &paths {
            assert!(!s3.exists(path).await.unwrap());
        }
    }
}
