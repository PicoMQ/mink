//! Forwards a bucket write or a coordinator action to another node over its own Flight endpoint.

use std::collections::HashMap;
use std::sync::Mutex;

use mink_common::sync::lock;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData, FlightDescriptor};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;
use tonic::transport::Channel;

use crate::error::Error;
use crate::proto::{self, Write, http_uri};

#[derive(Default)]
pub struct Forwarder {
    channels: Mutex<HashMap<String, Channel>>,
}

impl Forwarder {
    fn client(&self, address: &str) -> Result<FlightServiceClient<Channel>, Error> {
        let mut channels = lock(&self.channels);
        let channel = match channels.get(address) {
            Some(channel) => channel.clone(),
            None => {
                let channel = Channel::from_shared(http_uri(address))
                    .map_err(|e| Error::Internal(format!("bad node address {address:?}: {e}")))?
                    .connect_lazy();
                channels.insert(address.to_owned(), channel.clone());
                channel
            }
        };

        Ok(FlightServiceClient::new(channel)
            .max_decoding_message_size(proto::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(proto::MAX_MESSAGE_BYTES))
    }

    pub async fn put(
        &self,
        address: &str,
        write: &Write,
        batch: RecordBatch,
        meta: &proto::WriteBatch,
    ) -> Result<proto::Written, Error> {
        let mut client = self.client(address)?;
        let descriptor = FlightDescriptor::new_cmd(serde_json::to_vec(write)?);
        let app_metadata = Bytes::from(serde_json::to_vec(meta)?);
        let schema = batch.schema();
        let mut frames: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { Ok(batch) }))
            .try_collect()
            .await
            .map_err(Error::request)?;
        if let Some(first) = frames.first_mut() {
            first.flight_descriptor = Some(descriptor);
        }
        for frame in frames.iter_mut().filter(|f| !f.data_body.is_empty()) {
            frame.app_metadata = app_metadata.clone();
        }

        let mut acks = client
            .do_put(futures::stream::iter(frames))
            .await
            .map_err(Error::Remote)?
            .into_inner();
        let ack = acks
            .next()
            .await
            .ok_or_else(|| Error::Internal(format!("{address} acked nothing")))?
            .map_err(Error::Remote)?;

        Ok(serde_json::from_slice(&ack.app_metadata)?)
    }

    pub async fn action(
        &self,
        address: &str,
        name: &str,
        body: &impl Serialize,
    ) -> Result<Vec<Bytes>, Error> {
        let mut client = self.client(address)?;
        let results = client
            .do_action(Action::new(name, serde_json::to_vec(body)?))
            .await
            .map_err(Error::Remote)?
            .into_inner();

        results
            .map_ok(|r| r.body)
            .try_collect()
            .await
            .map_err(Error::Remote)
    }
}
