//! Starts one node from configuration: metadata store and sink, storage, stream engine, node, coordinator,
//! lake worker and the Flight listener, and shuts them down in order.

pub mod config;
mod error;
mod membership;
mod tiering;

use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mink_client::Cluster;
use mink_common::{Clock, SystemClock};
use mink_coordinator::{Coordinator, Membership};
use mink_flight::{Config, Flight};
use mink_kafka::{self, Kafka};
use mink_lake::Worker;
use mink_lake::iceberg::Catalog;
use mink_metadata::{CommandSink, Handle, Lifecycle, ObjectCleaner};
use mink_read::Read;
use mink_server::{Cleaner, Failover, Node, Service, Source};
use mink_sql::{Heartbeat, HeartbeatConfig, Lease, LeaseConfig, PgStore, Sink, SqliteStore, Store};
use s3stream::{
    Client, IdUri, ObjectStorageTrait, ObjectStoreAdapter, ObjectWalConfig, ObjectWalService,
    S3StreamBuilder,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub use config::{ConfigError, MetaBackend, ServerConfig};
pub use error::Error;

const LIFECYCLE_TICK: Duration = Duration::from_secs(1);

pub struct Server {
    advertise: String,
    flight: Option<mink_flight::Server>,
    flight_service: Flight,
    kafka: Option<mink_kafka::Server>,
    node: Arc<Node>,
    coordinator: Arc<Coordinator>,
    lake: Option<Arc<Catalog>>,
    engine: Arc<dyn Client>,
    sink: Arc<Sink>,
    heartbeat: Option<Heartbeat>,
    lease: Option<Lease>,
    stop: watch::Sender<bool>,
    node_task: Option<JoinHandle<()>>,
    coordinator_task: JoinHandle<()>,
    lifecycle_task: JoinHandle<()>,
    tiering_task: Option<JoinHandle<()>>,
}

pub async fn start(config: ServerConfig) -> Result<Server, Error> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let node_epoch = clock.millis();

    let store = open_store(&config.meta_backend()?).await?;
    let (sink, views) = Sink::open(store.clone(), mink_sql::Config::default()).await?;
    let sink = Arc::new(sink);
    let commands: Arc<dyn CommandSink> = sink.clone();

    let data = open_bucket(&config.storage_uri)?;
    let wal = open_bucket(&config.wal_uri())?;
    let snapshots = data.object_store();
    let data: Arc<dyn ObjectStorageTrait> = Arc::new(data);
    let wal: Arc<dyn ObjectStorageTrait> = Arc::new(wal);
    create_dir(&config.data_dir)?;

    let metadata = Handle::new(config.node_id, node_epoch, commands.clone(), views.clone());
    metadata
        .register(
            &config.advertise,
            1,
            kafka_addresses(config.kafka.as_ref(), None),
        )
        .await?;

    let mut wal_config = ObjectWalConfig::from_uri(&config.wal_uri())?;
    wal_config.cluster_id = config.cluster_id.clone();
    wal_config.node_id = config.node_id as u32;
    wal_config.epoch = node_epoch as u64;
    let engine = S3StreamBuilder::new(s3stream::Config {
        cluster_id: config.cluster_id.clone(),
        node_id: config.node_id as u32,
        node_epoch: node_epoch as u64,
        wal_config: config.wal_uri(),
        data_buckets: vec![config.storage_uri.clone()],
        wal_upload_interval_ms: config.wal_upload_interval.as_millis() as u64,
        ..s3stream::Config::default()
    })
    .object_storage(data.clone())
    .write_ahead_log(Arc::new(ObjectWalService::new(wal.clone(), wal_config)))
    .stream_manager(Arc::new(metadata.stream_manager()))
    .object_manager(Arc::new(metadata.object_manager()))
    .kv_client(Arc::new(metadata.kv_client()))
    .failover_factory(Arc::new(Failover::new(
        metadata.clone(),
        config.cluster_id.clone(),
        wal,
    )))
    .build()
    .await?;
    let engine: Arc<dyn Client> = Arc::new(engine);

    let ttl_ms = i64::try_from(config.lease_ttl.as_millis()).unwrap_or(i64::MAX);
    let heartbeat = Heartbeat::spawn(
        store.clone(),
        config.node_id,
        HeartbeatConfig {
            ttl_ms,
            interval: config.lease_ttl / 4,
        },
    );
    let membership: Arc<dyn Membership> =
        Arc::new(membership::Store::new(store.clone(), clock.clone()));

    let mut node_config = mink_server::Config::new(
        config.cluster_id.clone(),
        config.wal_uri(),
        config.data_dir.clone(),
    );
    node_config.kv_snapshot_interval = config.kv_snapshot_interval;
    node_config.log_retention_interval = config.log_retention_interval;

    let node = Arc::new(Node::new(
        metadata,
        engine.clone(),
        snapshots.clone(),
        Arc::new(mink_kv::SurrealEngine),
        membership.clone(),
        clock.clone(),
        node_config,
    ));

    let lease = Lease::spawn(
        store,
        format!("node-{}-{node_epoch}", config.node_id),
        LeaseConfig {
            ttl_ms,
            check_interval: config.lease_ttl / 4,
        },
    );
    let lake = match &config.lake {
        Some(mink_lake::Config::Iceberg(iceberg)) => {
            Some(Arc::new(Catalog::connect(iceberg).await?))
        }
        None => None,
    };

    let mut coordinator = Coordinator::new(
        config.node_id,
        config.advertise.clone(),
        commands.clone(),
        views.clone(),
        membership,
        Arc::new(Cleaner::new(snapshots)),
        clock,
        mink_coordinator::Config {
            default_bucket_count: config.default_bucket_count,
            snapshots_retained: config.snapshots_retained,
            producer_offsets_ttl: config.producer_offsets_ttl,
            producer_offsets_cleanup_interval: config.producer_offsets_cleanup_interval,
            tick: config.coordinator_tick,
            ..mink_coordinator::Config::default()
        },
    );
    if let Some(lake) = &lake {
        coordinator = coordinator.with_lake_catalog(lake.clone());
    }

    let coordinator = Arc::new(coordinator);
    let coordinator_task = coordinator.clone().spawn(lease.leadership());
    let service = Service::new(node.clone());
    let tiering_task = lake.clone().map(|lake| {
        Arc::new(Worker::new(
            lake,
            coordinator.clone(),
            Arc::new(tiering::Source::new(service.clone())),
            mink_lake::tiering::Config {
                poll_interval: config.tiering_poll_interval,
                ..mink_lake::tiering::Config::default()
            },
        ))
        .spawn(lease.leadership())
    });

    let lifecycle_task = Arc::new(Lifecycle::new(
        commands.clone(),
        Arc::new(ObjectCleaner::new(commands, views, Some(data))),
        LIFECYCLE_TICK,
    ))
    .drive(lease.leadership());

    let (stop, shutdown) = watch::channel(false);
    let node_task = node.clone().spawn(shutdown);
    let mut flight_service = Flight::new(
        service.clone(),
        coordinator.clone(),
        Config {
            describe: config.describe(),
            ..Config::default()
        },
    );
    if let Some(lake) = &lake {
        flight_service = flight_service.with_union(Arc::new(Read::new(
            Some(lake.clone()),
            Arc::new(Source::new(service.clone())),
        )));
    }
    let flight = mink_flight::serve(config.listen, flight_service.clone())
        .await
        .map_err(|source| Error::Bind {
            addr: config.listen,
            source,
        })?;
    let kafka = match config.kafka.clone() {
        Some(kafka_config) => {
            let admin = Cluster::connect(flight.uri())
                .map_err(|source| Error::Kafka(source.to_string()))?
                .admin();
            let kafka = Kafka::new(
                service.clone(),
                coordinator.clone(),
                admin,
                config.cluster_id.clone(),
                kafka_config,
            );
            let listen = kafka.config().listen;
            let server = mink_kafka::serve(kafka)
                .await
                .map_err(|source| Error::Bind {
                    addr: listen,
                    source,
                })?;
            let advertised = config
                .kafka
                .as_ref()
                .expect("cloned from Some")
                .advertised(server.local_addr());
            node.metadata()
                .register(
                    &config.advertise,
                    1,
                    kafka_addresses(config.kafka.as_ref(), Some(advertised)),
                )
                .await?;
            Some(server)
        }
        None => None,
    };
    tracing::info!(
        node_id = config.node_id,
        node_epoch,
        cluster = %config.cluster_id,
        advertise = %config.advertise,
        listen = %flight.local_addr(),
        kafka = ?kafka.as_ref().map(|k| k.local_addr()),
        "mink node started"
    );

    Ok(Server {
        advertise: config.advertise.clone(),
        flight: Some(flight),
        flight_service,
        kafka,
        node,
        coordinator,
        lake,
        engine,
        sink,
        heartbeat: Some(heartbeat),
        lease: Some(lease),
        stop,
        node_task: Some(node_task),
        coordinator_task,
        lifecycle_task,
        tiering_task,
    })
}

impl Server {
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn service(&self) -> Service {
        Service::new(self.node.clone())
    }

    pub fn advertise(&self) -> &str {
        &self.advertise
    }

    pub fn flight_addr(&self) -> SocketAddr {
        self.flight
            .as_ref()
            .expect("set until shutdown")
            .local_addr()
    }

    pub fn kafka_addr(&self) -> Option<SocketAddr> {
        self.kafka.as_ref().map(|k| k.local_addr())
    }

    pub fn coordinator(&self) -> &Arc<Coordinator> {
        &self.coordinator
    }

    pub fn lake(&self) -> Option<&Arc<Catalog>> {
        self.lake.as_ref()
    }

    pub fn health(&self) -> mink_flight::proto::Health {
        self.flight_service.health()
    }

    pub async fn stats(&self) -> mink_flight::proto::NodeStats {
        self.flight_service.node_stats().await
    }

    pub async fn shutdown(mut self) {
        if let Some(kafka) = self.kafka.take() {
            kafka.shutdown().await;
        }
        if let Some(flight) = self.flight.take() {
            flight.shutdown().await;
        }
        let _ = self.stop.send(true);
        if let Some(task) = self.node_task.take() {
            let _ = task.await;
        }
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.shutdown().await;
        }
        self.coordinator_task.abort();
        self.lifecycle_task.abort();
        if let Some(task) = self.tiering_task.take() {
            task.abort();
        }
        if let Some(lease) = self.lease.take() {
            lease.shutdown().await;
        }

        self.engine.shutdown().await;
        drop(self.sink);
        tracing::info!("mink node stopped");
    }
}

fn kafka_addresses(
    kafka: Option<&mink_kafka::Config>,
    advertised: Option<String>,
) -> BTreeMap<String, String> {
    let Some(config) = kafka else {
        return BTreeMap::new();
    };
    let address = advertised.unwrap_or_else(|| config.advertised(config.listen));

    BTreeMap::from([(mink_kafka::PROTOCOL.to_owned(), address)])
}

fn open_bucket(uri: &str) -> Result<ObjectStoreAdapter, Error> {
    let parsed = IdUri::parse(uri)?;
    if parsed.protocol == "file" {
        create_dir(Path::new(&parsed.path))?;
    }

    Ok(ObjectStoreAdapter::from_bucket_uri(uri)?)
}

fn create_dir(path: &Path) -> Result<(), Error> {
    fs::create_dir_all(path).map_err(|source| Error::DataDir {
        path: path.to_path_buf(),
        source,
    })
}

async fn open_store(backend: &MetaBackend) -> Result<Arc<dyn Store>, Error> {
    Ok(match backend {
        MetaBackend::Sqlite(None) => Arc::new(SqliteStore::memory().await?),
        MetaBackend::Sqlite(Some(path)) => {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                create_dir(parent)?;
            }

            Arc::new(SqliteStore::open(path).await?)
        }
        MetaBackend::Postgres(url) => Arc::new(PgStore::connect(url).await?),
    })
}
