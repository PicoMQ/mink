use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use mink_core::{Object, Snapshot, Topic};
use mink_iceberg::{Change, ObjectReader, Table};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::Error;
use crate::config::Storage;

const CREDENTIALS: [(&str, AmazonS3ConfigKey); 9] = [
    ("AWS_ACCESS_KEY_ID", AmazonS3ConfigKey::AccessKeyId),
    ("AWS_SECRET_ACCESS_KEY", AmazonS3ConfigKey::SecretAccessKey),
    ("AWS_SESSION_TOKEN", AmazonS3ConfigKey::Token),
    (
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
    ),
    (
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        AmazonS3ConfigKey::ContainerCredentialsFullUri,
    ),
    (
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
    ),
    (
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        AmazonS3ConfigKey::WebIdentityTokenFile,
    ),
    ("AWS_ROLE_ARN", AmazonS3ConfigKey::RoleArn),
    ("AWS_ROLE_SESSION_NAME", AmazonS3ConfigKey::RoleSessionName),
];

#[derive(Clone)]
pub(crate) struct Warehouse {
    root: String,
    bucket: String,
    store: Arc<dyn ObjectStore>,
}

pub(crate) struct ListedObject {
    path: String,
    modified_ms: i64,
}

impl Warehouse {
    pub(crate) fn connect(root: &str, storage: &Storage) -> Result<Warehouse, Error> {
        let bucket = bucket(root)?;
        let builder = match storage.credentials() {
            Some((access_key_id, secret_access_key)) => AmazonS3Builder::new()
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key),
            None => environment_credentials(),
        };

        let builder = match &storage.endpoint {
            Some(endpoint) => builder.with_endpoint(endpoint).with_allow_http(true),
            None => builder.with_virtual_hosted_style_request(true),
        };

        let store = builder
            .with_bucket_name(&bucket)
            .with_region(&storage.region)
            .build()?;

        Ok(Warehouse {
            root: root.trim_end_matches('/').to_owned(),
            bucket,
            store: Arc::new(store),
        })
    }

    #[cfg(test)]
    pub(crate) fn in_memory(root: &str) -> Result<Warehouse, Error> {
        Ok(Warehouse {
            root: root.trim_end_matches('/').to_owned(),
            bucket: bucket(root)?,
            store: Arc::new(object_store::memory::InMemory::new()),
        })
    }

    pub(crate) fn root(&self) -> &str {
        &self.root
    }

    pub(crate) fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub(crate) async fn read(&self, path: &str) -> Result<Bytes, Error> {
        let key = self.key(path)?;

        Ok(self.store.get(&key).await?.bytes().await?)
    }

    pub(crate) async fn write(&self, path: &str, bytes: Bytes) -> Result<(), Error> {
        let key = self.key(path)?;

        self.store.put(&key, PutPayload::from(bytes)).await?;
        Ok(())
    }

    pub(crate) async fn write_all(&self, change: &Change) -> Result<Vec<Object>, Error> {
        let mut objects = Vec::new();

        for (object, bytes) in change.objects() {
            self.write(object.path(), bytes.clone()).await?;
            objects.push(object.clone());
        }

        Ok(objects)
    }

    pub(crate) async fn load_table(
        &self,
        topic: &Topic,
        snapshot: &Snapshot,
    ) -> Result<Table, Error> {
        Ok(Table::load(self, &self.root, topic, snapshot).await?)
    }

    pub(crate) async fn delete(&self, path: &str) -> Result<(), Error> {
        let key = self.key(path)?;

        match self.store.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn delete_under(&self, prefix: &str) -> Result<(), Error> {
        let key = self.key(prefix)?;
        let locations = self
            .store
            .list(Some(&key))
            .map_ok(|object| object.location)
            .boxed();

        let mut deleted = self.store.delete_stream(locations);
        while let Some(result) = deleted.next().await {
            result?;
        }

        Ok(())
    }

    pub(crate) async fn list(&self, prefix: &str) -> Result<Vec<ListedObject>, Error> {
        let key = self.key(prefix)?;
        let objects: Vec<_> = self.store.list(Some(&key)).try_collect().await?;

        Ok(objects
            .into_iter()
            .map(|object| ListedObject {
                path: format!("s3://{}/{}", self.bucket, object.location),
                modified_ms: object.last_modified.timestamp_millis(),
            })
            .collect())
    }

    pub(crate) fn key(&self, path: &str) -> Result<Path, Error> {
        let key = path
            .strip_prefix("s3://")
            .and_then(|rest| rest.strip_prefix(&self.bucket))
            .and_then(|rest| rest.strip_prefix('/'))
            .ok_or_else(|| Error::OutsideBucket(path.to_owned()))?;

        Ok(Path::from(key))
    }
}

impl ListedObject {
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn modified_ms(&self) -> i64 {
        self.modified_ms
    }
}

impl ObjectReader for Warehouse {
    async fn read(&self, path: &str) -> Result<Bytes, mink_iceberg::Error> {
        Warehouse::read(self, path)
            .await
            .map_err(|error| mink_iceberg::Error::ObjectRead {
                path: path.to_owned(),
                message: error.to_string(),
            })
    }
}

fn environment_credentials() -> AmazonS3Builder {
    CREDENTIALS
        .into_iter()
        .filter_map(|(variable, key)| Some((key, std::env::var(variable).ok()?)))
        .fold(AmazonS3Builder::new(), |builder, (key, value)| {
            builder.with_config(key, value)
        })
}

fn bucket(root: &str) -> Result<String, Error> {
    root.strip_prefix("s3://")
        .and_then(|rest| rest.split('/').next())
        .filter(|bucket| !bucket.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::InvalidWarehouse(root.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn objects_round_trip_and_list_under_a_prefix() {
        let warehouse = Warehouse::in_memory("s3://mink/warehouse").unwrap();
        let path = "s3://mink/warehouse/orders/metadata/v1-e1.metadata.json";

        warehouse
            .write(path, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        let read = warehouse.read(path).await.unwrap();
        let listed = warehouse
            .list("s3://mink/warehouse/orders/metadata/")
            .await
            .unwrap();
        warehouse.delete(path).await.unwrap();
        warehouse.delete(path).await.unwrap();

        assert_eq!(read, Bytes::from_static(b"{}"));
        assert_eq!(
            listed.iter().map(ListedObject::path).collect::<Vec<_>>(),
            [path]
        );
        assert!(warehouse.read(path).await.is_err());
        assert!(matches!(
            warehouse.read("s3://other/orders").await,
            Err(Error::OutsideBucket(_))
        ));
    }
}
