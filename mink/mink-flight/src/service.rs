//! The Flight service: request context, table and flight descriptions, view synchronization, and the
//! protocol entry points dispatching to the read, write, action and stats handlers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::ArrowError;
use bytes::Bytes;
use futures::Stream;
use mink_coordinator::Coordinator;
use mink_metadata::{BucketRow, TableRow, View};
use mink_read::Read;
use mink_server::Service;
use mink_table::{Bucket, PartitionId, Path, SchemaId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::codec;
use crate::config::Config;
use crate::error::Error;
use crate::forward::Forwarder;
use crate::proto::{self, action, header};

pub(crate) type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct Flight {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) service: Service,
    pub(crate) coordinator: Arc<Coordinator>,
    pub(crate) config: Config,
    pub(crate) forwarder: Forwarder,
    pub(crate) union: Option<Arc<Read>>,
    pub(crate) started_ms: i64,
}

const SCHEMA_WAIT: Duration = Duration::from_secs(5);
const VIEW_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
pub(crate) enum Locator<'a> {
    Bucket(Bucket),
    Path(&'a Path),
}

impl Flight {
    pub fn new(service: Service, coordinator: Arc<Coordinator>, config: Config) -> Self {
        let started_ms = service.node().clock().millis();
        Flight {
            inner: Arc::new(Inner {
                service,
                coordinator,
                config,
                forwarder: Forwarder::default(),
                union: None,
                started_ms,
            }),
        }
    }

    pub fn with_union(mut self, union: Arc<Read>) -> Self {
        let inner = Arc::get_mut(&mut self.inner).expect("not yet shared");
        inner.union = Some(union);
        self
    }

    pub fn service(&self) -> &Service {
        &self.inner.service
    }

    pub(crate) fn status(&self, error: Error) -> Status {
        match error {
            Error::Server(mink_server::Error::NotLeader { bucket, leader }) => {
                let view = self.inner.service.view();
                Error::NotLeader {
                    bucket,
                    to: leader.and_then(|id| node_info(&view, id)),
                }
                .into()
            }
            other => other.into(),
        }
    }

    pub(crate) fn coordinator(&self) -> Result<&Arc<Coordinator>, Error> {
        if self.inner.coordinator.is_leader() {
            return Ok(&self.inner.coordinator);
        }

        Err(Error::NotCoordinator {
            coordinator: coordinator_info(&self.inner.service.view()),
        })
    }

    pub(crate) fn table_info(&self, view: &View, path: &Path) -> Result<proto::TableInfo, Error> {
        let row = table_at(view, path)?;
        let catalog = &view.state.catalog;
        let leader = |(bucket, brow): (&Bucket, &BucketRow)| proto::BucketLeader {
            bucket: *bucket,
            leader: (brow.leader >= 0)
                .then(|| node_info(view, brow.leader))
                .flatten(),
        };
        let buckets = if row.descriptor.is_partitioned() {
            catalog
                .partitions_of(row.table_id)
                .flat_map(|p| catalog.buckets_of(row.table_id, Some(p.partition_id)))
                .map(leader)
                .collect()
        } else {
            catalog.buckets_of(row.table_id, None).map(leader).collect()
        };

        Ok(proto::TableInfo {
            table_id: row.table_id,
            path: path.clone(),
            descriptor: row.descriptor.clone(),
            schemas: row.schemas.clone(),
            created_ms: row.created_ms,
            modified_ms: row.modified_ms,
            buckets,
        })
    }

    fn flight_info(&self, view: &View, path: &Path) -> Result<FlightInfo, Error> {
        let info = self.table_info(view, path)?;
        let schema = codec::arrow(info.schemas.last().expect("a table has a schema"));
        let mut flight = FlightInfo::new()
            .try_with_schema(&schema)?
            .with_descriptor(FlightDescriptor::new_path(proto::descriptor_path(path)))
            .with_app_metadata(serde_json::to_vec(&info)?);
        let has_primary_key = info.descriptor.schema().primary_key().is_some();
        let lake = if self.inner.union.is_some() && info.descriptor.options().lake.is_some() {
            view.state.catalog.lake.get(&info.table_id)
        } else {
            None
        };
        let keyless = info.descriptor.bucket_keys().is_empty();
        if let Some(lake) = lake.filter(|_| keyless) {
            let mut partitions: Vec<Option<PartitionId>> =
                info.buckets.iter().map(|b| b.bucket.partition()).collect();
            partitions.sort_unstable();
            partitions.dedup();
            let served_by = info.buckets.iter().find_map(|b| b.leader.clone());
            for partition in partitions {
                let name = partition
                    .and_then(|id| view.state.catalog.partition_names.get(&id))
                    .map(|(_, name)| name.clone());
                let ticket = proto::Read::Lake {
                    path: path.clone(),
                    partition: name,
                    snapshot_id: lake.snapshot_id,
                    columns: None,
                };
                let mut endpoint =
                    FlightEndpoint::new().with_ticket(Ticket::new(serde_json::to_vec(&ticket)?));
                if let Some(node) = &served_by {
                    endpoint = endpoint.with_location(node.address.clone());
                }
                flight = flight.with_endpoint(endpoint);
            }
            for bucket in &info.buckets {
                let ticket = proto::Read::Scan {
                    bucket: bucket.bucket,
                    offset: lake
                        .bucket_log_end_offset
                        .get(&bucket.bucket)
                        .copied()
                        .unwrap_or(0),
                    max_bytes: None,
                    columns: None,
                };
                let mut endpoint =
                    FlightEndpoint::new().with_ticket(Ticket::new(serde_json::to_vec(&ticket)?));
                if let Some(leader) = &bucket.leader {
                    endpoint = endpoint.with_location(leader.address.clone());
                }
                flight = flight.with_endpoint(endpoint);
            }

            return Ok(flight);
        }
        for bucket in &info.buckets {
            let ticket = if lake.is_some() {
                proto::Read::Union {
                    bucket: bucket.bucket,
                    columns: None,
                }
            } else if has_primary_key {
                proto::Read::Snapshot {
                    bucket: bucket.bucket,
                    columns: None,
                    batch_rows: None,
                }
            } else {
                proto::Read::Scan {
                    bucket: bucket.bucket,
                    offset: 0,
                    max_bytes: None,
                    columns: None,
                }
            };
            let mut endpoint =
                FlightEndpoint::new().with_ticket(Ticket::new(serde_json::to_vec(&ticket)?));
            if let Some(leader) = &bucket.leader {
                endpoint = endpoint.with_location(leader.address.clone());
            }
            flight = flight.with_endpoint(endpoint);
        }

        Ok(flight)
    }

    async fn sync<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(value) = request.metadata().get(header::MIN_APPLIED_INDEX) else {
            return Ok(());
        };
        let min: u64 = value
            .to_str()
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                Status::invalid_argument(format!("{} is not a number", header::MIN_APPLIED_INDEX))
            })?;
        let views = self.inner.service.node().metadata().views();
        if views.load().applied_index >= min {
            return Ok(());
        }

        match tokio::time::timeout(VIEW_WAIT, views.wait_applied(min)).await {
            Ok(_) => Ok(()),
            Err(_) => Err(Status::unavailable(format!(
                "metadata view at {} has not reached {min}",
                views.load().applied_index
            ))),
        }
    }

    fn respond<T>(&self, inner: T) -> Response<T> {
        let mut response = Response::new(inner);
        let applied = self.inner.service.view().applied_index;
        if let Ok(value) = applied.to_string().parse() {
            response.metadata_mut().insert(header::APPLIED_INDEX, value);
        }

        response
    }

    pub(crate) fn spawn_stream<T, F, Fut>(&self, run: F) -> BoxStream<T>
    where
        T: Send + 'static,
        F: FnOnce(Flight, mpsc::Sender<Result<T, Status>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Error>> + Send,
    {
        let this = self.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            if let Err(error) = run(this.clone(), tx.clone()).await {
                let _ = tx.send(Err(this.status(error))).await;
            }
        });

        Box::pin(ReceiverStream::new(rx))
    }

    pub(crate) async fn with_schema(
        &self,
        locator: Locator<'_>,
        schema_id: SchemaId,
    ) -> Result<TableRow, Error> {
        let views = self.inner.service.node().metadata().views();
        let deadline = Instant::now() + SCHEMA_WAIT;
        loop {
            let view = views.load();
            let table = match locator {
                Locator::Bucket(bucket) => self.table(&view, bucket)?,
                Locator::Path(path) => table_at(&view, path)?.clone(),
            };
            if table.schemas.len() > schema_id.0 as usize {
                return Ok(table);
            }
            let newer = views.wait_applied(view.applied_index + 1);
            if tokio::time::timeout_at(deadline, newer).await.is_err() {
                return Err(Error::SchemaNotExist(schema_id.0));
            }
        }
    }

    pub(crate) fn table(&self, view: &View, bucket: Bucket) -> Result<TableRow, Error> {
        view.state
            .catalog
            .table_by_id(bucket.table())
            .cloned()
            .ok_or(Error::Server(mink_server::Error::BucketNotExist(bucket)))
    }
}

pub(crate) fn node_info(view: &View, node_id: i32) -> Option<proto::NodeInfo> {
    view.state.nodes.get(&node_id).map(|n| proto::NodeInfo {
        node_id,
        address: n.http_address.clone(),
    })
}

pub(crate) fn coordinator_info(view: &View) -> Option<proto::NodeInfo> {
    view.state
        .catalog
        .coordinator
        .as_ref()
        .map(|c| proto::NodeInfo {
            node_id: c.node_id,
            address: c.address.clone(),
        })
}

pub(crate) fn table_at<'v>(view: &'v View, path: &Path) -> Result<&'v TableRow, Error> {
    view.state
        .catalog
        .tables
        .get(path)
        .ok_or_else(|| Error::TableNotExist(path.to_string()))
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    Ok(serde_json::from_slice(bytes)?)
}

pub(crate) fn one<T: Serialize>(value: &T) -> Result<Vec<Bytes>, Error> {
    Ok(vec![serde_json::to_vec(value)?.into()])
}

fn parse(descriptor: &FlightDescriptor) -> Result<Path, Error> {
    match descriptor.path.as_slice() {
        [database, table] => format!("{database}.{table}")
            .parse()
            .map_err(Error::request),
        _ => Err(Error::Request(
            "descriptor path must be [database, table]".into(),
        )),
    }
}

#[tonic::async_trait]
impl FlightService for Flight {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;
    type DoExchangeStream = BoxStream<FlightData>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "handshake: authentication is not served yet",
        ))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.sync(&request).await?;
        let database = String::from_utf8(request.into_inner().expression.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let view = self.inner.service.view();
        let mut flights = Vec::new();
        for path in view.state.catalog.tables.keys() {
            if !database.is_empty() && path.database().as_str() != database {
                continue;
            }
            flights.push(self.flight_info(&view, path).map_err(|e| self.status(e))?);
        }

        Ok(self.respond(Box::pin(futures::stream::iter(flights.into_iter().map(Ok)))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.sync(&request).await?;
        let path = parse(&request.into_inner()).map_err(|e| self.status(e))?;
        let view = self.inner.service.view();
        let info = self.flight_info(&view, &path).map_err(|e| self.status(e))?;

        Ok(self.respond(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.sync(&request).await?;
        let path = parse(&request.into_inner()).map_err(|e| self.status(e))?;
        let view = self.inner.service.view();
        let row = table_at(&view, &path).map_err(|e| self.status(e))?;
        let schema = codec::arrow(row.schemas.last().expect("a table has a schema"));
        let result = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

        Ok(self.respond(result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.sync(&request).await?;
        let read: proto::Read = decode(&request.into_inner().ticket).map_err(|e| self.status(e))?;
        let stream = match read {
            proto::Read::Scan {
                bucket,
                offset,
                max_bytes,
                columns,
            } => self.scan(bucket, offset, max_bytes, columns),
            proto::Read::LimitScan {
                bucket,
                limit,
                columns,
            } => self.limit_scan(bucket, limit, columns).await,
            proto::Read::Snapshot {
                bucket,
                columns,
                batch_rows,
            } => self.snapshot(bucket, columns, batch_rows).await,
            proto::Read::Union { bucket, columns } => self.union(bucket, columns).await,
            proto::Read::Lake {
                path,
                partition,
                snapshot_id,
                columns,
            } => self.lake(path, partition, snapshot_id, columns).await,
        }
        .map_err(|e| self.status(e))?;

        Ok(self.respond(stream))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.sync(&request).await?;

        Ok(self.respond(self.write(request.into_inner())))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.sync(&request).await?;

        Ok(self.respond(self.tail(request.into_inner())))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.sync(&request).await?;
        let results = self
            .action(request.into_inner())
            .await
            .map_err(|e| self.status(e))?;

        Ok(self.respond(Box::pin(futures::stream::iter(
            results
                .into_iter()
                .map(|body| Ok(arrow_flight::Result { body })),
        ))))
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.sync(&request).await?;
        let actions = action::ALL.iter().map(|(name, description)| {
            Ok(ActionType {
                r#type: (*name).to_owned(),
                description: (*description).to_owned(),
            })
        });

        Ok(self.respond(Box::pin(futures::stream::iter(actions))))
    }
}
