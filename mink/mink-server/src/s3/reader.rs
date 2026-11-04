use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{HeaderValue, Method};
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use mink_core::{BuiltFile, File, locations_holding};
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

#[derive(Clone)]
enum Source {
    Cache { key: Arc<Key>, table: Uuid },
    Build { table: Uuid, built: Arc<BuiltFile> },
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
        self: &Arc<Self>,
        key: &Key,
        request: &Request<'_>,
        range: ReadRange,
    ) -> Result<Found, Error> {
        if let Object::External = key.object() {
            return self.external(key, Method::GET, request, range).await;
        }

        let table = self.table(key).await?;
        let Some(built) = self.built(key, table).await? else {
            return self.whole(key, table, request, range).await;
        };

        let size = built.indexed().stats().bytes();
        let range = range
            .resolve(size)
            .map_err(|_| Error::Unsatisfiable(size))?;
        let slices = mink_iceberg::slices(&built, range.clone())?;

        if let Some(bytes) = bytes_only(&slices) {
            return Ok(Found::Stream(Ranged {
                range,
                size,
                body: stream::iter(bytes.into_iter().map(Ok)).boxed(),
            }));
        }

        let source = match self.route(key, Method::GET, request).await {
            Route::Here => Source::Cache {
                key: Arc::new(key.clone()),
                table,
            },
            Route::Forwarded(answer) => return Ok(Found::Forwarded(answer)),
            Route::Unreachable(_) => Source::Build {
                table,
                built: Arc::new(built),
            },
        };
        let body = self.body(source, slices).await?;

        Ok(Found::Stream(Ranged { range, size, body }))
    }

    pub(super) async fn head(&self, key: &Key, request: &Request<'_>) -> Result<Found, Error> {
        if let Object::External = key.object() {
            return self
                .external(key, Method::HEAD, request, ReadRange::Full)
                .await;
        }

        let table = self.table(key).await?;
        if let Some(built) = self.built(key, table).await? {
            return Ok(Found::Length(built.indexed().stats().bytes()));
        }

        match self.route(key, Method::HEAD, request).await {
            Route::Here => Ok(Found::Length(self.cache.length(key, table).await?)),
            Route::Forwarded(answer) => Ok(Found::Forwarded(answer)),
            Route::Unreachable(file) => Ok(Found::Length(
                self.cache.build(table, file).await?.len() as u64,
            )),
        }
    }

    async fn external(
        &self,
        key: &Key,
        method: Method,
        request: &Request<'_>,
        range: ReadRange,
    ) -> Result<Found, Error> {
        let head = method == Method::HEAD;
        if let Route::Forwarded(answer) = self.route(key, method, request).await {
            return Ok(Found::Forwarded(answer));
        }

        let table = self.table(key).await?;
        match head {
            true => Ok(Found::Length(self.cache.length(key, table).await?)),
            false => Ok(Found::Stream(ranged(
                self.cache.read(key, table, range).await?,
            ))),
        }
    }

    async fn built(&self, key: &Key, table: Uuid) -> Result<Option<BuiltFile>, Error> {
        match self.cache.built(key, table).await {
            Err(error) if error.store_unavailable() => Ok(None),
            built => built,
        }
    }

    async fn whole(
        &self,
        key: &Key,
        table: Uuid,
        request: &Request<'_>,
        range: ReadRange,
    ) -> Result<Found, Error> {
        match self.route(key, Method::GET, request).await {
            Route::Here => {
                let range = self.sized(key, table, range).await?;

                Ok(Found::Stream(ranged(
                    self.cache.read(key, table, range).await?,
                )))
            }
            Route::Forwarded(answer) => Ok(Found::Forwarded(answer)),
            Route::Unreachable(file) => Ok(Found::Bytes(self.cache.build(table, file).await?)),
        }
    }

    async fn body(
        self: &Arc<Self>,
        source: Source,
        slices: Vec<Slice>,
    ) -> Result<ByteStream, Error> {
        let first = slices
            .iter()
            .position(|slice| matches!(slice, Slice::Extent { .. }));
        let mut slices = slices.into_iter();

        let mut opened = Vec::new();
        for slice in slices
            .by_ref()
            .take(first.map_or(usize::MAX, |index| index + 1))
        {
            opened.push(self.open(&source, slice).await?);
        }

        let reader = self.clone();
        let rest = stream::iter(slices)
            .map(move |slice| {
                let (reader, source) = (reader.clone(), source.clone());
                async move { reader.open(&source, slice).await.map_err(io::Error::other) }
            })
            .buffered(LOOKAHEAD)
            .try_flatten();

        Ok(stream::iter(opened).flatten().chain(rest).boxed())
    }

    async fn open(&self, source: &Source, slice: Slice) -> Result<ByteStream, Error> {
        match (slice, source) {
            (Slice::Bytes(bytes), _) => Ok(stream::once(async { Ok(bytes) }).boxed()),
            (Slice::Extent { extent, within }, Source::Cache { key, table }) => {
                let read = self.cache.read_extent(key, *table, &extent, within);

                Ok(ranged(read.await?).body)
            }
            (Slice::Extent { extent, within }, Source::Build { table, built }) => {
                let extent = self.cache.build_extent(*table, built, extent).await?;
                let bytes = extent.slice(within.start as usize..within.end as usize);

                Ok(stream::once(async { Ok(bytes) }).boxed())
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
        let store = &self.services.store;
        let (names, found) = match key.object() {
            Object::Metadata { topic } => (vec![topic.as_str()], topic_table(store, topic).await),
            Object::Data(file) => {
                let topic = file.partition().topic();
                (vec![topic], topic_table(store, topic).await)
            }
            Object::External => {
                let holding = store.external_tables().holding(key.path()).await;
                let found = holding
                    .map(|table| table.map(|table| (table.location().to_owned(), table.uuid())));
                (locations_holding(key.path()), found)
            }
        };

        let mut tables = self.tables.lock().expect("the table map is never poisoned");
        match found {
            Ok(Some((name, uuid))) => {
                tables.insert(name, uuid);
                Ok(uuid)
            }
            Ok(None) => {
                for name in &names {
                    tables.remove(*name);
                }
                Err(Error::NoSuchKey(key.path().to_owned()))
            }
            Err(error) => {
                let error = Error::from(error);
                let known = names
                    .iter()
                    .rev()
                    .find_map(|name| tables.get(*name).copied());

                known.filter(|_| error.store_unavailable()).ok_or(error)
            }
        }
    }

    async fn route<'a>(&self, key: &'a Key, method: Method, request: &Request<'_>) -> Route<'a> {
        let file = match key.object() {
            Object::Metadata { .. } => return Route::Here,
            Object::Data(file) => Some(file),
            Object::External => None,
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
                file.map_or(Route::Here, Route::Unreachable)
            }
        }
    }
}

async fn topic_table(
    store: &mink_store::Store,
    topic: &str,
) -> Result<Option<(String, Uuid)>, mink_store::Error> {
    let found = store.topics().get(topic).await?;

    Ok(found.map(|found| (topic.to_owned(), found.uuid())))
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

fn bytes_only(slices: &[Slice]) -> Option<Vec<Bytes>> {
    slices
        .iter()
        .map(|slice| match slice {
            Slice::Bytes(bytes) => Some(bytes.clone()),
            Slice::Extent { .. } => None,
        })
        .collect()
}
