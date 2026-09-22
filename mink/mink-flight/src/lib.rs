//! The Arrow Flight server: binds a listener and serves the Flight protocol over a node's service and coordinator.

mod action;
mod codec;
mod config;
mod error;
mod forward;
mod read;
mod service;
mod stats;
mod write;

use std::io;
use std::net::SocketAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport;

pub use config::Config;
pub use error::Error;
pub use mink_protocol as proto;
pub use service::Flight;

pub struct Server {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), transport::Error>>,
}

impl Server {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn uri(&self) -> String {
        format!("grpc://{}", self.addr)
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = self.task.await;
    }
}

pub async fn serve(addr: SocketAddr, flight: Flight) -> Result<Server, io::Error> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let (stop, stopped) = oneshot::channel();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let task = tokio::spawn(
        transport::Server::builder()
            .add_service(
                FlightServiceServer::new(flight)
                    .max_decoding_message_size(proto::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(proto::MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    tracing::info!(%addr, "flight listener started");

    Ok(Server {
        addr,
        stop: Some(stop),
        task,
    })
}
