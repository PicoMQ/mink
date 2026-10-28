mod admin;
mod catalog;
mod commands;
mod config;
mod error;
mod forward;
mod indexer;
mod membership;
mod owners;
mod registration;
mod registry;
mod runs;
mod s3;
mod schemas;
mod services;
mod supervisor;
#[cfg(test)]
mod testing;
mod warehouse;
mod writer;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use mink_core::Member;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub use commands::{add_table, list_tables, remove_table, set_granularity};
pub use config::{
    Config, ConfigError, History, SchemaRegistry, Storage, parse_granularity, parse_role,
};
pub use error::Error;
pub use mink_core::{Granularity, Role, TableState, Topic};

use crate::s3::Cache;
use crate::services::Services;

pub struct Node {
    services: Arc<Services>,
    cache: Option<Arc<Cache>>,
    stop: watch::Sender<bool>,
    servers: Vec<JoinHandle<io::Result<()>>>,
    membership: JoinHandle<()>,
    supervisor: Option<JoinHandle<()>>,
    catalog_address: Option<SocketAddr>,
    s3_address: Option<SocketAddr>,
    admin_address: Option<SocketAddr>,
}

impl Node {
    pub async fn start(config: Config) -> Result<Node, Error> {
        let services = Services::start(config).await?;

        Node::serve(services).await
    }

    pub(crate) async fn serve(services: Arc<Services>) -> Result<Node, Error> {
        let config = &services.config;
        config.check_admin_address()?;

        let (stop, _) = watch::channel(false);
        let mut servers = Vec::new();

        let cache = match config.has(Role::S3) {
            true => Some(Arc::new(Cache::open(services.clone()).await?)),
            false => None,
        };

        let catalog = match config.has(Role::Catalog) {
            true => Some(catalog::router(services.clone())),
            false => None,
        };
        let s3 = match &cache {
            Some(cache) => Some(s3::router(services.clone(), cache.clone())?),
            None => None,
        };

        let catalog_address = match &catalog {
            Some(router) => {
                Some(serve(config.catalog_address, router.clone(), &stop, &mut servers).await?)
            }
            None => None,
        };
        let s3_address = match &s3 {
            Some(router) => {
                Some(serve(config.s3_address, router.clone(), &stop, &mut servers).await?)
            }
            None => None,
        };

        let admin_address = match config.has(Role::Admin) {
            true => Some(
                serve(
                    config.admin_address,
                    admin::router(services.clone(), cache.clone(), catalog.zip(s3))?,
                    &stop,
                    &mut servers,
                )
                .await?,
            ),
            false => None,
        };

        let address = config
            .advertised_address
            .clone()
            .or_else(|| s3_address.map(|address| format!("http://{address}")));
        let advertised_admin = config
            .advertised_admin_address
            .clone()
            .or_else(|| admin_address.map(|address| format!("http://{address}")));
        let member = Member::new(
            services.membership.id(),
            config.roles.clone(),
            address,
            advertised_admin,
        );

        services
            .membership
            .renew(&services.store, &member, config.lease_ttl)
            .await?;

        let membership = tokio::spawn(membership::run(services.clone(), member, stop.subscribe()));

        let supervisor = config
            .has(Role::Indexer)
            .then(|| tokio::spawn(supervisor::supervise(services.clone(), stop.subscribe())));

        Ok(Node {
            services,
            cache,
            membership,
            supervisor,
            stop,
            servers,
            catalog_address,
            s3_address,
            admin_address,
        })
    }

    pub fn catalog_address(&self) -> Option<SocketAddr> {
        self.catalog_address
    }

    pub fn s3_address(&self) -> Option<SocketAddr> {
        self.s3_address
    }

    pub fn admin_address(&self) -> Option<SocketAddr> {
        self.admin_address
    }

    #[cfg(test)]
    pub(crate) fn cache(&self) -> Option<&Arc<Cache>> {
        self.cache.as_ref()
    }

    pub fn kill(self) {
        for server in &self.servers {
            server.abort();
        }

        self.membership.abort();
        if let Some(supervisor) = &self.supervisor {
            supervisor.abort();
        }
    }

    pub async fn shutdown(self) -> Result<(), Error> {
        self.stop.send_replace(true);

        for server in self.servers {
            server.await.map_err(io::Error::other)??;
        }
        if let Some(supervisor) = self.supervisor {
            supervisor.await.map_err(io::Error::other)?;
        }
        self.membership.await.map_err(io::Error::other)?;

        let closed = match &self.cache {
            Some(cache) => cache.close().await,
            None => Ok(()),
        };
        self.services.store.close().await;
        closed
    }
}

async fn serve(
    address: SocketAddr,
    router: Router,
    stop: &watch::Sender<bool>,
    servers: &mut Vec<JoinHandle<io::Result<()>>>,
) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let mut stopped = stop.subscribe();

    servers.push(tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = stopped.wait_for(|stop| *stop).await;
            })
            .await
    }));

    Ok(address)
}

#[cfg(test)]
mod tests {

    use reqwest::StatusCode;

    use super::*;
    use crate::testing;

    async fn config() -> Option<Config> {
        testing::config(&Role::ALL).await
    }

    #[tokio::test]
    async fn catalog_rejects_writes_with_iceberg_error() {
        let Some(config) = config().await else { return };
        let node = testing::node_with(config).await;
        let url = format!("http://{}/v1/namespaces", node.catalog_address().unwrap());

        let response = reqwest::Client::new()
            .post(url)
            .body("{}")
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(body["error"]["code"], 403);
        assert_eq!(body["error"]["type"], "ForbiddenException");
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn roles_choose_which_listeners_start() {
        let Some(config) = config().await else { return };

        let node = testing::node_with(config.with_roles(vec![Role::S3])).await;

        assert_eq!(node.catalog_address(), None);
        assert!(node.s3_address().is_some());
        node.shutdown().await.unwrap();
    }
}
