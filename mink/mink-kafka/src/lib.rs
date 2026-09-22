//! Kafka wire-protocol frontend over a node's service: topics are log tables in one database,
//! partitions are buckets, brokers are nodes, and the group coordinator lives on the coordinator node.

mod admin;
mod commit;
mod config;
mod dispatch;
mod error;
mod fetch;
mod group;
mod groups;
mod metadata;
mod offsets;
mod produce;
mod producer;
mod record;
mod server;
mod topic;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use mink_client::Admin;
use mink_coordinator::Coordinator;
use mink_server::Service;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub use config::Config;
pub use error::Error;

use crate::groups::Groups;
use crate::producer::Producers;

pub const PROTOCOL: &str = "kafka";

const SESSION_TICK: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct Kafka {
    inner: Arc<Inner>,
}

struct Inner {
    service: Service,
    coordinator: Arc<Coordinator>,
    admin: Admin,
    cluster_id: String,
    config: Config,
    groups: Groups,
    producers: Producers,
}

impl Kafka {
    pub fn new(
        service: Service,
        coordinator: Arc<Coordinator>,
        admin: Admin,
        cluster_id: String,
        config: Config,
    ) -> Self {
        Kafka {
            inner: Arc::new(Inner {
                service,
                coordinator,
                admin,
                cluster_id,
                config,
                groups: Groups::default(),
                producers: Producers::default(),
            }),
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }
}

pub struct Server {
    addr: SocketAddr,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Server {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn shutdown(self) {
        let _ = self.stop.send(true);
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

pub async fn serve(kafka: Kafka) -> Result<Server, io::Error> {
    let listener = TcpListener::bind(kafka.inner.config.listen).await?;
    let addr = listener.local_addr()?;
    let (stop, stopped) = watch::channel(false);
    let accept = tokio::spawn(server::accept(kafka.clone(), listener, stopped.clone()));
    let sessions = tokio::spawn(groups::sweep(kafka, SESSION_TICK, stopped));
    tracing::info!(%addr, "kafka listener started");

    Ok(Server {
        addr,
        stop,
        tasks: vec![accept, sessions],
    })
}
