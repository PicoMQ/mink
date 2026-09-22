//! One Flight connection to a node, carrying the applied-index session across requests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData, FlightDescriptor, FlightInfo, PutResult, Ticket};
use bytes::Bytes;
use futures::TryStreamExt;
use mink_table::Path;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::Sender;
use tonic::transport::Channel;
use tonic::{Request, Response, Streaming};

use crate::Error;
use crate::proto::{self, MAX_MESSAGE_BYTES, Read, Tail, Write, header, http_uri};

#[derive(Debug, Default)]
pub struct Session {
    seen: AtomicU64,
}

impl Session {
    pub fn seen(&self) -> u64 {
        self.seen.load(Ordering::Acquire)
    }

    fn observe(&self, applied: u64) {
        self.seen.fetch_max(applied, Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub struct Connection {
    address: String,
    channel: Channel,
    session: Arc<Session>,
}

impl Connection {
    pub fn new(address: &str) -> Result<Self, Error> {
        Self::with_session(address, Arc::new(Session::default()))
    }

    pub(crate) fn with_session(address: &str, session: Arc<Session>) -> Result<Self, Error> {
        let channel = Channel::from_shared(http_uri(address))
            .map_err(|e| Error::Address(address.to_owned(), e.to_string()))?
            .connect_lazy();

        Ok(Connection {
            address: address.to_owned(),
            channel,
            session,
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }

    fn client(&self) -> FlightServiceClient<Channel> {
        FlightServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        let seen = self.session.seen();
        if seen > 0
            && let Ok(value) = seen.to_string().parse()
        {
            request
                .metadata_mut()
                .insert(header::MIN_APPLIED_INDEX, value);
        }

        request
    }

    fn learn<T>(&self, response: Response<T>) -> T {
        if let Some(applied) = response
            .metadata()
            .get(header::APPLIED_INDEX)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
        {
            self.session.observe(applied);
        }

        response.into_inner()
    }

    pub async fn get(&self, read: &Read) -> Result<Streaming<FlightData>, Error> {
        let ticket = Ticket::new(serde_json::to_vec(read)?);
        Ok(self.learn(self.client().do_get(self.request(ticket)).await?))
    }

    pub async fn flight_info(&self, path: &Path) -> Result<FlightInfo, Error> {
        let descriptor = FlightDescriptor::new_path(proto::descriptor_path(path));
        Ok(self.learn(
            self.client()
                .get_flight_info(self.request(descriptor))
                .await?,
        ))
    }

    pub async fn put(
        &self,
        write: &Write,
        batches: Vec<(RecordBatch, proto::WriteBatch)>,
    ) -> Result<Vec<Bytes>, Error> {
        let Some(first) = batches.first() else {
            return Ok(Vec::new());
        };
        let schema = first.0.schema();
        let mut frames: Vec<FlightData> = Vec::with_capacity(batches.len() + 1);
        for (batch, meta) in batches {
            let encoded: Vec<FlightData> = FlightDataEncoderBuilder::new()
                .with_schema(schema.clone())
                .build(futures::stream::once(async move { Ok(batch) }))
                .try_collect()
                .await?;
            // The encoder emits schema + batch per call; only the first schema frame is kept.
            for frame in encoded {
                if !frame.data_body.is_empty() {
                    frames.push(FlightData {
                        app_metadata: serde_json::to_vec(&meta)?.into(),
                        ..frame
                    });
                } else if frames.is_empty() {
                    frames.push(frame);
                }
            }
        }
        frames[0].flight_descriptor = Some(FlightDescriptor::new_cmd(serde_json::to_vec(write)?));

        let acks: Vec<PutResult> = self
            .learn(
                self.client()
                    .do_put(self.request(futures::stream::iter(frames)))
                    .await?,
            )
            .try_collect()
            .await?;

        Ok(acks.into_iter().map(|ack| ack.app_metadata).collect())
    }

    pub(crate) async fn exchange(
        &self,
        tail: &Tail,
    ) -> Result<(Handle, Streaming<FlightData>), Error> {
        let (tx, rx) = tokio::sync::mpsc::channel::<FlightData>(1);
        tx.send(FlightData {
            flight_descriptor: Some(FlightDescriptor::new_cmd(serde_json::to_vec(tail)?)),
            ..Default::default()
        })
        .await
        .map_err(|_| Error::Protocol("exchange closed before it opened".into()))?;
        let frames = self.learn(
            self.client()
                .do_exchange(self.request(tokio_stream::wrappers::ReceiverStream::new(rx)))
                .await?,
        );

        Ok((Handle { _open: tx }, frames))
    }

    pub async fn action_raw(&self, name: &str, body: &impl Serialize) -> Result<Vec<Bytes>, Error> {
        let action = Action::new(name, serde_json::to_vec(body)?);
        Ok(self
            .learn(self.client().do_action(self.request(action)).await?)
            .map_ok(|r| r.body)
            .try_collect()
            .await?)
    }

    pub async fn action<R: DeserializeOwned>(
        &self,
        name: &str,
        body: &impl Serialize,
    ) -> Result<Vec<R>, Error> {
        self.action_raw(name, body)
            .await?
            .iter()
            .map(|body| Ok(serde_json::from_slice(body)?))
            .collect()
    }

    pub async fn action_one<R: DeserializeOwned>(
        &self,
        name: &str,
        body: &impl Serialize,
    ) -> Result<R, Error> {
        self.action(name, body)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Protocol(format!("{name} returned no result")))
    }
}

pub(crate) struct Handle {
    _open: Sender<FlightData>,
}
