use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{HeaderValue, Method};
use bytes::Bytes;
use mink_core::File;
use mink_metrics::{Forward, Metrics};
use nestor::{ReadRange, ReadStream};
use uuid::Uuid;

use super::cache::Cache;
use super::key::{Key, Object};
use crate::Error;
use crate::forward::{Forwarder, Wait};
use crate::services::Services;

const ANSWER_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) struct Reader {
    services: Arc<Services>,
    cache: Arc<Cache>,
    forwarder: Forwarder,
    tables: Mutex<HashMap<String, Uuid>>,
}

pub(super) struct Request<'a> {
    pub(super) path: &'a str,
    pub(super) range: Option<&'a HeaderValue>,
    pub(super) forwarded: bool,
}

pub(super) enum Found {
    Bytes(Bytes),
    Stream(ReadStream),
    Length(u64),
    Forwarded(reqwest::Response),
}

enum Route<'a> {
    Here,
    Forwarded(reqwest::Response),
    Unreachable(&'a File),
}

impl Reader {
    pub(super) fn new(services: Arc<Services>, cache: Arc<Cache>) -> Result<Reader, Error> {
        let forwarder = Forwarder::new(services.membership.id(), Wait::FirstByte(ANSWER_TIMEOUT))?;

        Ok(Reader {
            services,
            cache,
            forwarder,
            tables: Mutex::new(HashMap::new()),
        })
    }

    pub(super) fn id(&self) -> Uuid {
        self.services.membership.id()
    }

    pub(super) fn metrics(&self) -> &Metrics {
        &self.services.metrics
    }

    pub(super) fn key(&self, path: &str) -> Result<Key, Error> {
        Key::parse(self.services.warehouse.root(), path)
            .ok_or_else(|| Error::NoSuchKey(path.to_owned()))
    }

    pub(super) async fn get(
        &self,
        key: &Key,
        request: &Request<'_>,
        range: ReadRange,
    ) -> Result<Found, Error> {
        match self.route(key, Method::GET, request).await {
            Route::Here => {
                let table = self.table(key).await?;
                let range = self.sized(key, table, range).await?;

                Ok(Found::Stream(self.cache.read(key, table, range).await?))
            }
            Route::Forwarded(answer) => Ok(Found::Forwarded(answer)),
            Route::Unreachable(file) => {
                self.table(key).await?;

                Ok(Found::Bytes(self.cache.build(file).await?))
            }
        }
    }

    pub(super) async fn head(&self, key: &Key, request: &Request<'_>) -> Result<Found, Error> {
        if let Some(stored) = self.stored_length(key).await? {
            self.table(key).await?;

            return Ok(Found::Length(stored));
        }

        match self.route(key, Method::HEAD, request).await {
            Route::Here => {
                let table = self.table(key).await?;

                Ok(Found::Length(self.cache.length(key, table).await?))
            }
            Route::Forwarded(answer) => Ok(Found::Forwarded(answer)),
            Route::Unreachable(file) => {
                self.table(key).await?;

                Ok(Found::Length(self.cache.build(file).await?.len() as u64))
            }
        }
    }

    async fn sized(&self, key: &Key, table: Uuid, range: ReadRange) -> Result<ReadRange, Error> {
        let (ReadRange::Suffix(suffix), Object::Data(_)) = (&range, key.object()) else {
            return Ok(range);
        };

        let size = match self.stored_length(key).await? {
            Some(size) => size,
            None => self.cache.length(key, table).await?,
        };

        Ok(ReadRange::Bounded(size.saturating_sub(*suffix)..size))
    }

    async fn table(&self, key: &Key) -> Result<Uuid, Error> {
        let name = key.topic();

        match self.services.store.topics().get(name).await {
            Ok(Some(topic)) => {
                self.tables
                    .lock()
                    .expect("the table map is never poisoned")
                    .insert(name.to_owned(), topic.uuid());
                Ok(topic.uuid())
            }
            Ok(None) => {
                self.tables
                    .lock()
                    .expect("the table map is never poisoned")
                    .remove(name);

                Err(Error::NoSuchKey(key.path().to_owned()))
            }
            Err(error) => {
                let error = Error::from(error);
                let known = self
                    .tables
                    .lock()
                    .expect("the table map is never poisoned")
                    .get(name)
                    .copied();

                known.filter(|_| error.store_unavailable()).ok_or(error)
            }
        }
    }

    async fn stored_length(&self, key: &Key) -> Result<Option<u64>, Error> {
        let Object::Data(file) = key.object() else {
            return Ok(None);
        };

        match self.services.store.files().stats(file).await {
            Ok(stats) => Ok(stats.map(|stats| stats.bytes())),
            Err(error) => {
                let error = Error::from(error);
                match error.store_unavailable() {
                    true => Ok(None),
                    false => Err(error),
                }
            }
        }
    }

    async fn route<'a>(&self, key: &'a Key, method: Method, request: &Request<'_>) -> Route<'a> {
        let Object::Data(file) = key.object() else {
            return Route::Here;
        };

        if request.forwarded {
            return Route::Here;
        }

        let Some(owner) = self.services.membership.remote_owner(key.path()) else {
            return Route::Here;
        };

        let forwarded = match owner.address() {
            Some(address) => {
                self.forwarder
                    .forward(address, method, request.path, request.range)
                    .await
            }
            None => Err(Error::NoAddress(owner.id())),
        };
        match forwarded {
            Ok(answer) => {
                self.services.metrics.forwarded(Forward::Ok);
                Route::Forwarded(answer)
            }
            Err(error) => {
                self.services.metrics.forwarded(Forward::Unreachable);
                tracing::warn!(%error, path = key.path(), "the owner is unreachable; serving here");
                Route::Unreachable(file)
            }
        }
    }
}
