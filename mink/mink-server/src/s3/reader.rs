use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{HeaderValue, Method, StatusCode};
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use mink_core::{BuiltFile, File, TableLocation};
use mink_iceberg::Slice;
use mink_metrics::{Forward, Metrics};
use nestor::{ReadRange, ReadStream};
use uuid::Uuid;

use super::cache::Cache;
use super::key::{Key, Object};
use crate::Error;
use crate::forward::{Forwarder, Wait};
use crate::services::Services;

const ANSWER_TIMEOUT: Duration = Duration::from_secs(60);
const LOOKAHEAD: usize = 2;

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
    Stream(Ranged),
    Length(u64),
    Forwarded(reqwest::Response),
}

pub(super) struct Ranged {
    pub(super) range: Range<u64>,
    pub(super) size: u64,
    pub(super) body: ByteStream,
}

pub(super) type ByteStream = BoxStream<'static, io::Result<Bytes>>;

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
        self: &Arc<Self>,
        key: &Key,
        request: &Request<'_>,
        range: ReadRange,
    ) -> Result<Found, Error> {
        let table = self.table(key).await?;
        if let Some(built) = self.built(key, table, request).await? {
            return self.sliced(&built, table, range).await;
        }

        match self.route(key, Method::GET, request).await {
            Route::Here => {
                let range = self.sized(key, table, range).await?;

                Ok(Found::Stream(ranged(
                    self.cache.read(key, table, range).await?,
                )))
            }
            Route::Forwarded(answer) => Ok(Found::Forwarded(answer)),
            Route::Unreachable(file) => Ok(Found::Bytes(self.cache.build(file).await?)),
        }
    }

    pub(super) async fn head(&self, key: &Key, request: &Request<'_>) -> Result<Found, Error> {
        let table = self.table(key).await?;
        if let Some(built) = self.built(key, table, request).await? {
            return Ok(Found::Length(built.indexed().stats().bytes()));
        }

        match self.route(key, Method::HEAD, request).await {
            Route::Here => Ok(Found::Length(self.cache.length(key, table).await?)),
            Route::Forwarded(answer) => Ok(Found::Forwarded(answer)),
            Route::Unreachable(file) => {
                Ok(Found::Length(self.cache.build(file).await?.len() as u64))
            }
        }
    }

    async fn built(
        &self,
        key: &Key,
        table: Uuid,
        request: &Request<'_>,
    ) -> Result<Option<BuiltFile>, Error> {
        if request.forwarded {
            return Ok(None);
        }

        match self.cache.built(key, table).await {
            Err(error) if error.store_unavailable() => Ok(None),
            built => built,
        }
    }

    async fn sliced(
        self: &Arc<Self>,
        built: &BuiltFile,
        table: Uuid,
        range: ReadRange,
    ) -> Result<Found, Error> {
        let size = built.indexed().stats().bytes();
        let range = range
            .resolve(size)
            .map_err(|_| Error::Unsatisfiable(size))?;

        let slices = mink_iceberg::slices(built, range.clone())?;
        let built = slices
            .iter()
            .position(|slice| matches!(slice, Slice::Built { .. }));
        let mut slices = slices.into_iter();

        let mut opened = Vec::new();
        for slice in slices
            .by_ref()
            .take(built.map_or(usize::MAX, |index| index + 1))
        {
            opened.push(self.piece(slice, table).await?);
        }
        let first = stream::iter(opened).flatten();

        let reader = self.clone();
        let rest = stream::iter(slices)
            .map(move |slice| {
                let reader = reader.clone();
                async move { reader.piece(slice, table).await.map_err(io::Error::other) }
            })
            .buffered(LOOKAHEAD)
            .try_flatten();

        Ok(Found::Stream(Ranged {
            range,
            size,
            body: first.chain(rest).boxed(),
        }))
    }

    async fn piece(&self, slice: Slice, table: Uuid) -> Result<ByteStream, Error> {
        match slice {
            Slice::Bytes(bytes) => Ok(stream::once(async { Ok(bytes) }).boxed()),
            Slice::Built { file, range } => self.part(&file, range, table).await,
        }
    }

    async fn part(&self, file: &File, range: Range<u64>, table: Uuid) -> Result<ByteStream, Error> {
        let key = self.key(&TableLocation::data_file(
            self.services.warehouse.root(),
            file,
        ))?;
        let path = key.uri();
        let header = HeaderValue::from_str(&format!("bytes={}-{}", range.start, range.end - 1))
            .expect("a byte range is a header value");
        let request = Request {
            path: &path,
            range: Some(&header),
            forwarded: false,
        };

        match self.route(&key, Method::GET, &request).await {
            Route::Here => {
                let read = self.cache.read(&key, table, ReadRange::Bounded(range));

                Ok(ranged(read.await?).body)
            }
            Route::Forwarded(answer) if answer.status() == StatusCode::NOT_FOUND => {
                Err(Error::NoSuchKey(key.path().to_owned()))
            }
            Route::Forwarded(answer) => {
                let answer = answer.error_for_status().map_err(Error::Forward)?;

                Ok(answer.bytes_stream().map_err(io::Error::other).boxed())
            }
            Route::Unreachable(file) => {
                let bytes = self.cache.build(file).await?;
                let size = bytes.len() as u64;
                if range.end > size {
                    return Err(Error::Unsatisfiable(size));
                }

                let part = bytes.slice(range.start as usize..range.end as usize);
                Ok(stream::once(async { Ok(part) }).boxed())
            }
        }
    }

    async fn sized(&self, key: &Key, table: Uuid, range: ReadRange) -> Result<ReadRange, Error> {
        let (ReadRange::Suffix(suffix), Object::Data(_)) = (&range, key.object()) else {
            return Ok(range);
        };

        let size = self.cache.length(key, table).await?;

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

fn ranged(stream: ReadStream) -> Ranged {
    let range = stream.range();
    let size = stream.size().unwrap_or(range.end);

    Ranged {
        range,
        size,
        body: stream.map_err(io::Error::other).boxed(),
    }
}
