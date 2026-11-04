use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::config::Storage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Buckets {
    storage: Storage,
    #[serde(skip)]
    clients: Arc<Mutex<HashMap<String, Arc<dyn ObjectStore>>>>,
}

impl Buckets {
    pub(crate) fn new(storage: Storage) -> Buckets {
        Buckets {
            storage,
            clients: Arc::default(),
        }
    }

    pub(crate) fn client(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>, Error> {
        let mut clients = self
            .clients
            .lock()
            .expect("the bucket clients are never poisoned");

        match clients.get(bucket) {
            Some(store) => Ok(store.clone()),
            None => {
                let opened = client(bucket, &self.storage)?;
                clients.insert(bucket.to_owned(), opened.clone());
                Ok(opened)
            }
        }
    }
}

pub(crate) fn bucket_and_key(path: &str) -> Option<(&str, Path)> {
    let rest = path.strip_prefix("s3://")?;
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return None;
    }

    Some((bucket, Path::parse(key).ok()?))
}

fn client(bucket: &str, storage: &Storage) -> Result<Arc<dyn ObjectStore>, Error> {
    let builder = match &storage.credentials {
        Some(credentials) => AmazonS3Builder::new()
            .with_access_key_id(&credentials.access_key_id)
            .with_secret_access_key(&credentials.secret_access_key),
        None => AmazonS3Builder::from_env(),
    };

    let builder = match &storage.endpoint {
        Some(endpoint) => builder.with_endpoint(endpoint).with_allow_http(true),
        None => builder.with_virtual_hosted_style_request(true),
    };

    let store = builder
        .with_bucket_name(bucket)
        .with_region(&storage.region)
        .build()?;
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_taken_as_written() {
        let (bucket, key) = bucket_and_key("s3://b/t/region=us%20east/0.parquet").unwrap();

        assert_eq!(bucket, "b");
        assert_eq!(key.as_ref(), "t/region=us%20east/0.parquet");
        assert_eq!(
            bucket_and_key("s3://b").map(|(bucket, _)| bucket),
            Some("b")
        );
        assert!(bucket_and_key("s3:///t").is_none());
        assert!(bucket_and_key("s3://b/t/../0.parquet").is_none());
        assert!(bucket_and_key("/tmp/t").is_none());
    }
}
