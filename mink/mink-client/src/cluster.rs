//! Connections to a cluster: bootstrap addresses, cached metadata and table leaders, and retry on redirect.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use mink_common::sync::{lock, read, write};
use mink_table::{Bucket, Path};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::Instant;

use crate::connection::Session;
use crate::proto::{self, action};
use crate::{Admin, Connection, Error, Table};

pub const MAX_REDIRECTS: usize = 8;
pub const DEFAULT_LEADER_WAIT: Duration = Duration::from_secs(60);
const RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

struct Inner {
    bootstrap: Vec<String>,
    session: Arc<Session>,
    connections: Mutex<HashMap<String, Connection>>,
    metadata: RwLock<Option<proto::Metadata>>,
    tables: RwLock<HashMap<Path, Arc<proto::TableInfo>>>,
    leader_wait: RwLock<Duration>,
}

impl Cluster {
    pub fn connect(bootstrap: impl Into<String>) -> Result<Self, Error> {
        Self::connect_all(vec![bootstrap.into()])
    }

    pub fn connect_all(bootstrap: Vec<String>) -> Result<Self, Error> {
        if bootstrap.is_empty() {
            return Err(Error::Address(String::new(), "no bootstrap address".into()));
        }
        let cluster = Cluster {
            inner: Arc::new(Inner {
                bootstrap,
                session: Arc::new(Session::default()),
                connections: Mutex::new(HashMap::new()),
                metadata: RwLock::new(None),
                tables: RwLock::new(HashMap::new()),
                leader_wait: RwLock::new(DEFAULT_LEADER_WAIT),
            }),
        };
        for address in &cluster.inner.bootstrap {
            cluster.connection(address)?;
        }

        Ok(cluster)
    }

    pub fn with_leader_wait(self, wait: Duration) -> Self {
        *write(&self.inner.leader_wait) = wait;
        self
    }

    pub fn leader_wait(&self) -> Duration {
        *read(&self.inner.leader_wait)
    }

    pub fn admin(&self) -> Admin {
        Admin::new(self.clone())
    }

    pub async fn table(&self, path: &Path) -> Result<Table, Error> {
        Table::open(self.clone(), path).await
    }

    pub fn connection(&self, address: &str) -> Result<Connection, Error> {
        let mut connections = lock(&self.inner.connections);
        if let Some(connection) = connections.get(address) {
            return Ok(connection.clone());
        }

        let connection = Connection::with_session(address, self.inner.session.clone())?;
        connections.insert(address.to_owned(), connection.clone());

        Ok(connection)
    }

    pub fn session(&self) -> &Arc<Session> {
        &self.inner.session
    }

    pub fn any(&self) -> Result<Connection, Error> {
        self.connection(&self.inner.bootstrap[0])
    }

    fn candidates(&self) -> Vec<String> {
        let mut out = self.inner.bootstrap.clone();
        if let Some(metadata) = read(&self.inner.metadata).as_ref() {
            for node in &metadata.nodes {
                if !out.contains(&node.address) {
                    out.push(node.address.clone());
                }
            }
        }

        out
    }

    pub(crate) async fn any_action_one<R: DeserializeOwned>(
        &self,
        name: &str,
        body: &impl Serialize,
    ) -> Result<R, Error> {
        let mut last = None;
        for address in self.candidates() {
            match self.connection(&address)?.action_one(name, body).await {
                Ok(value) => return Ok(value),
                Err(e) if e.is_retriable() && e.redirect().is_none() => last = Some(e),
                Err(e) => return Err(e),
            }
        }

        Err(last.expect("at least one bootstrap address"))
    }

    pub async fn metadata(&self) -> Result<proto::Metadata, Error> {
        if let Some(metadata) = read(&self.inner.metadata).clone() {
            return Ok(metadata);
        }

        self.refresh_metadata().await
    }

    pub async fn refresh_metadata(&self) -> Result<proto::Metadata, Error> {
        let metadata: proto::Metadata = self.any_action_one(action::METADATA, &()).await?;
        *write(&self.inner.metadata) = Some(metadata.clone());

        Ok(metadata)
    }

    pub async fn coordinator(&self) -> Result<Connection, Error> {
        let metadata = self.metadata().await?;
        match metadata.coordinator {
            Some(node) => self.connection(&node.address),
            None => Err(Error::NoCoordinator),
        }
    }

    pub(crate) async fn coordinator_action<R: DeserializeOwned>(
        &self,
        name: &str,
        body: &impl Serialize,
    ) -> Result<Vec<R>, Error> {
        let started = Instant::now();
        let mut redirects = 0;
        loop {
            let (asked, result) = match self.coordinator().await {
                Ok(connection) => (
                    Some(connection.address().to_owned()),
                    connection.action(name, body).await,
                ),
                Err(e) => (None, Err(e)),
            };
            let error = match result {
                Ok(results) => return Ok(results),
                Err(e) if e.is_retriable() => e,
                Err(e) => return Err(e),
            };
            let redirected_to = error.redirect().and_then(|r| r.to);
            let stale = redirected_to
                .as_ref()
                .is_some_and(|to| asked.as_deref() == Some(to.address.as_str()));
            if redirected_to.is_some() {
                redirects += 1;
                if redirects > MAX_REDIRECTS {
                    return Err(match error {
                        Error::Status(last) => Error::Redirects {
                            attempts: MAX_REDIRECTS,
                            last,
                        },
                        other => other,
                    });
                }
                if stale {
                    tokio::time::sleep(RETRY_DELAY * redirects as u32).await;
                    *write(&self.inner.metadata) = None;
                    continue;
                }
            } else if started.elapsed() >= self.leader_wait() {
                return Err(error);
            }
            if let Err(e) = self.follow_coordinator_redirect(&error).await
                && !e.is_retriable()
            {
                return Err(e);
            }
        }
    }

    async fn follow_coordinator_redirect(&self, error: &Error) -> Result<(), Error> {
        match error.redirect().and_then(|r| r.to) {
            Some(to) => {
                if let Some(metadata) = write(&self.inner.metadata).as_mut() {
                    metadata.coordinator = Some(to);
                } else {
                    *write(&self.inner.metadata) = Some(proto::Metadata {
                        nodes: vec![to.clone()],
                        coordinator: Some(to),
                    });
                }

                Ok(())
            }
            None => {
                tokio::time::sleep(RETRY_DELAY).await;
                self.refresh_metadata().await.map(|_| ())
            }
        }
    }

    pub(crate) async fn table_info(&self, path: &Path) -> Result<Arc<proto::TableInfo>, Error> {
        if let Some(info) = read(&self.inner.tables).get(path).cloned() {
            return Ok(info);
        }

        self.refresh_table(path).await
    }

    pub(crate) async fn refresh_table(&self, path: &Path) -> Result<Arc<proto::TableInfo>, Error> {
        let info: proto::TableInfo = self
            .any_action_one(action::GET_TABLE, &proto::TableRef { path: path.clone() })
            .await?;
        let info = Arc::new(info);
        write(&self.inner.tables).insert(path.clone(), info.clone());

        Ok(info)
    }

    pub(crate) fn forget_table(&self, path: &Path) {
        write(&self.inner.tables).remove(path);
    }

    pub(crate) async fn leader(&self, path: &Path, bucket: Bucket) -> Result<Connection, Error> {
        let mut info = self.table_info(path).await?;
        if leader_of(&info, bucket).is_none() {
            info = self.refresh_table(path).await?;
        }

        match leader_of(&info, bucket) {
            Some(Some(node)) => self.connection(&node.address),
            Some(None) => Err(Error::NoLeader(bucket)),
            None => Err(Error::UnknownBucket(bucket, path.clone())),
        }
    }

    pub(crate) async fn with_leader<T, F, Fut>(
        &self,
        path: &Path,
        bucket: Bucket,
        mut call: F,
    ) -> Result<T, Error>
    where
        F: FnMut(Connection) -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let started = Instant::now();
        let mut redirects = 0;
        loop {
            let result = match self.leader(path, bucket).await {
                Ok(connection) => call(connection).await,
                Err(e) => Err(e),
            };
            let error = match result {
                Ok(value) => return Ok(value),
                Err(e) if e.is_retriable() => e,
                Err(e) => return Err(e),
            };
            if error.redirect().is_some() {
                redirects += 1;
                if redirects > MAX_REDIRECTS {
                    return Err(match error {
                        Error::Status(last) => Error::Redirects {
                            attempts: MAX_REDIRECTS,
                            last,
                        },
                        other => other,
                    });
                }
            } else if started.elapsed() >= self.leader_wait() {
                return Err(error);
            }

            if let Err(e) = self.follow_leader_redirect(path, bucket, &error).await
                && !e.is_retriable()
            {
                return Err(e);
            }
        }
    }

    async fn follow_leader_redirect(
        &self,
        path: &Path,
        bucket: Bucket,
        error: &Error,
    ) -> Result<(), Error> {
        match error.redirect().and_then(|r| r.to) {
            Some(to) => {
                self.set_leader(path, bucket, Some(to));

                Ok(())
            }
            None => {
                tokio::time::sleep(RETRY_DELAY).await;
                self.refresh_table(path).await.map(|_| ())
            }
        }
    }

    fn set_leader(&self, path: &Path, bucket: Bucket, leader: Option<proto::NodeInfo>) {
        let mut tables = write(&self.inner.tables);
        let Some(info) = tables.get(path) else {
            return;
        };

        let mut patched = (**info).clone();
        match patched.buckets.iter_mut().find(|b| b.bucket == bucket) {
            Some(entry) => entry.leader = leader,
            None => patched.buckets.push(proto::BucketLeader { bucket, leader }),
        }

        tables.insert(path.clone(), Arc::new(patched));
    }
}

fn leader_of(info: &proto::TableInfo, bucket: Bucket) -> Option<Option<proto::NodeInfo>> {
    info.buckets
        .iter()
        .find(|b| b.bucket == bucket)
        .map(|b| b.leader.clone())
}
