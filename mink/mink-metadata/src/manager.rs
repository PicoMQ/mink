//! A node's handle to the metadata sink, and the stream, object and key-value manager adapters the
//! stream engine drives through it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use mink_common::{Clock, SystemClock};
use mink_table::Bucket;
use s3stream::{
    CommitStreamSetObjectRequest, CommitStreamSetObjectResponse, CompactStreamObjectRequest,
    Error as StreamError, KVClient, KeyValue, ObjectManagerTrait, S3ObjectMetadata,
    StreamManagerTrait, StreamMetadata,
};

use crate::catalog::{Counter, KvSnapshotRow};
use crate::command::{Command, Outcome};
use crate::error::Error;
use crate::sink::{CommandSink, SinkStats};
use crate::view::ViewPublisher;

#[derive(Clone)]
pub struct Handle {
    node_id: i32,
    node_epoch: i64,
    sink: Arc<dyn CommandSink>,
    views: Arc<ViewPublisher>,
}

impl Handle {
    pub fn new(
        node_id: i32,
        node_epoch: i64,
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
    ) -> Self {
        Self {
            node_id,
            node_epoch,
            sink,
            views,
        }
    }

    pub async fn register(
        &self,
        http_address: &str,
        slots: u32,
        protocol_addresses: BTreeMap<String, String>,
    ) -> Result<(), Error> {
        self.sink
            .propose(Command::RegisterNode {
                node_id: self.node_id,
                node_epoch: self.node_epoch,
                http_address: http_address.to_owned(),
                slots,
                protocol_addresses,
            })
            .await?;

        Ok(())
    }

    pub fn as_node(&self, node_id: i32, node_epoch: i64) -> Self {
        Self {
            node_id,
            node_epoch,
            sink: self.sink.clone(),
            views: self.views.clone(),
        }
    }

    pub fn views(&self) -> &Arc<ViewPublisher> {
        &self.views
    }

    pub fn stream_manager(&self) -> Streams {
        Streams { node: self.clone() }
    }

    pub fn object_manager(&self) -> Objects {
        Objects { node: self.clone() }
    }

    pub fn kv_client(&self) -> Kv {
        Kv { node: self.clone() }
    }

    pub fn sink_stats(&self) -> Arc<SinkStats> {
        self.sink.stats()
    }

    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    pub fn node_epoch(&self) -> i64 {
        self.node_epoch
    }

    pub async fn allocate_producer_ids(&self, count: u32) -> Result<u64, Error> {
        self.propose_id(Command::AllocateProducerIds {
            node_id: self.node_id,
            node_epoch: self.node_epoch,
            count,
        })
        .await
    }

    pub async fn allocate(&self, counter: Counter, count: u64) -> Result<u64, Error> {
        self.propose_id(Command::Allocate { counter, count }).await
    }

    pub async fn lead_bucket(&self, bucket: Bucket, coordinator_epoch: i32) -> Result<i32, Error> {
        let epoch = self
            .propose_id(Command::LeadBucket {
                bucket,
                node_id: self.node_id,
                coordinator_epoch,
            })
            .await?;

        Ok(epoch as i32)
    }

    pub async fn commit_kv_snapshot(
        &self,
        bucket: Bucket,
        snapshot: KvSnapshotRow,
        leader_epoch: i32,
        coordinator_epoch: i32,
    ) -> Result<(), Error> {
        self.sink
            .propose(Command::CommitKvSnapshot {
                bucket,
                snapshot,
                leader_epoch,
                coordinator_epoch,
            })
            .await?;

        Ok(())
    }

    pub async fn update_node_slots(
        &self,
        node_id: i32,
        node_epoch: i64,
        slots: u32,
    ) -> Result<(), Error> {
        self.sink
            .propose(Command::RegisterNode {
                node_id,
                node_epoch,
                http_address: String::new(),
                slots,
                protocol_addresses: Default::default(),
            })
            .await?;

        Ok(())
    }

    pub async fn propose_transfer(
        &self,
        stream_id: u64,
        from_node: i32,
        to_node: i32,
    ) -> Result<(), Error> {
        self.sink
            .propose(Command::TransferStream {
                stream_id,
                from_node,
                to_node,
            })
            .await?;

        Ok(())
    }

    pub async fn complete_transfer(&self, stream_id: u64, epoch: i64) -> Result<(), Error> {
        self.propose_unless_redundant(Command::CompleteTransfer { stream_id, epoch })
            .await?;

        Ok(())
    }

    pub async fn close_stream(&self, stream_id: u64, epoch: i64) -> Result<(), Error> {
        self.sink
            .propose(Command::CloseStream {
                node_id: self.node_id,
                node_epoch: self.node_epoch,
                stream_id,
                epoch,
            })
            .await?;

        Ok(())
    }

    async fn propose_id(&self, command: Command) -> Result<u64, Error> {
        match self.sink.propose(command).await?.result {
            Outcome::Id(id) => Ok(id),
            other => Err(Error::Unexpected {
                message: format!("unexpected metadata result {other:?}"),
            }),
        }
    }

    async fn propose_unless_redundant(&self, command: Command) -> Result<Option<Outcome>, Error> {
        match self.sink.propose(command).await {
            Ok(proposed) => Ok(Some(proposed.result)),
            Err(e) if e.is_redundant() => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn propose(&self, command: Command) -> Result<Outcome, StreamError> {
        Ok(self
            .sink
            .propose(command)
            .await
            .map_err(|e| e.to_stream_error())?
            .result)
    }
}

fn unexpected_result(result: Outcome) -> StreamError {
    StreamError::Unexpected(format!("unexpected metadata result {result:?}"))
}

fn id_of(result: Outcome) -> Result<u64, StreamError> {
    match result {
        Outcome::Id(id) => Ok(id),
        other => Err(unexpected_result(other)),
    }
}

fn value_of(result: Outcome) -> Result<Option<Bytes>, StreamError> {
    match result {
        Outcome::Value(value) => Ok(value),
        other => Err(unexpected_result(other)),
    }
}

fn now_ms() -> i64 {
    SystemClock.millis()
}

#[derive(Clone)]
pub struct Streams {
    node: Handle,
}

#[async_trait]
impl StreamManagerTrait for Streams {
    async fn get_opening_streams(&self) -> Result<Vec<StreamMetadata>, StreamError> {
        Ok(self
            .node
            .views
            .load()
            .state
            .get_opening_streams(self.node.node_id))
    }

    async fn get_streams(&self, stream_ids: &[u64]) -> Result<Vec<StreamMetadata>, StreamError> {
        Ok(self.node.views.load().state.get_streams(stream_ids))
    }

    async fn create_stream(&self, _tags: HashMap<String, String>) -> Result<u64, StreamError> {
        let result = self
            .node
            .propose(Command::CreateStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
            })
            .await?;

        id_of(result)
    }

    async fn open_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        _tags: HashMap<String, String>,
    ) -> Result<StreamMetadata, StreamError> {
        match self
            .node
            .propose(Command::OpenStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                stream_id,
                epoch: epoch as i64,
            })
            .await?
        {
            Outcome::Stream(metadata) => Ok(metadata),
            other => Err(unexpected_result(other)),
        }
    }

    async fn trim_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        new_start_offset: u64,
    ) -> Result<(), StreamError> {
        self.node
            .propose(Command::TrimStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                stream_id,
                epoch: epoch as i64,
                new_start_offset,
            })
            .await?;

        Ok(())
    }

    async fn close_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        self.node
            .propose(Command::CloseStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                stream_id,
                epoch: epoch as i64,
            })
            .await?;

        Ok(())
    }

    async fn delete_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        self.node
            .propose(Command::DeleteStream {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                stream_id,
                epoch: epoch as i64,
            })
            .await?;

        Ok(())
    }
}

pub struct Objects {
    node: Handle,
}

#[async_trait]
impl ObjectManagerTrait for Objects {
    async fn prepare_object(&self, count: usize, ttl_ms: u64) -> Result<u64, StreamError> {
        let result = self
            .node
            .propose(Command::PrepareObject {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                count: count as u32,
                ttl_ms: ttl_ms as i64,
                now_ms: now_ms(),
            })
            .await?;

        id_of(result)
    }

    async fn commit_stream_set_object(
        &self,
        request: CommitStreamSetObjectRequest,
    ) -> Result<CommitStreamSetObjectResponse, StreamError> {
        self.node
            .propose_unless_redundant(Command::CommitStreamSetObject {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                request,
                now_ms: now_ms(),
            })
            .await
            .map_err(|e| e.to_stream_error())?;

        Ok(CommitStreamSetObjectResponse {})
    }

    async fn compact_stream_object(
        &self,
        request: CompactStreamObjectRequest,
    ) -> Result<(), StreamError> {
        self.node
            .propose_unless_redundant(Command::CompactStreamObject {
                node_id: self.node.node_id,
                node_epoch: self.node.node_epoch,
                request,
                now_ms: now_ms(),
            })
            .await
            .map_err(|e| e.to_stream_error())?;

        Ok(())
    }

    async fn get_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        Ok(self
            .node
            .views
            .load()
            .state
            .get_objects(stream_id, start_offset, end_offset, limit))
    }

    async fn get_server_objects(&self) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        Ok(self
            .node
            .views
            .load()
            .state
            .get_server_objects(self.node.node_id))
    }

    async fn get_stream_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        Ok(self.node.views.load().state.get_stream_objects(
            stream_id,
            start_offset,
            end_offset,
            limit,
        ))
    }

    async fn is_object_exist(&self, object_id: u64) -> Result<bool, StreamError> {
        Ok(self.node.views.load().state.is_object_exist(object_id))
    }
}

pub struct Kv {
    node: Handle,
}

#[async_trait]
impl KVClient for Kv {
    async fn put_kv_if_absent(&self, kv: KeyValue) -> Result<Bytes, StreamError> {
        match self
            .node
            .propose(Command::PutKvIfAbsent {
                key: kv.key,
                value: kv.value,
            })
            .await?
        {
            Outcome::Value(Some(value)) => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    async fn put_kv(&self, kv: KeyValue) -> Result<Bytes, StreamError> {
        match self
            .node
            .propose(Command::PutKv {
                key: kv.key,
                value: kv.value,
            })
            .await?
        {
            Outcome::Value(Some(value)) => Ok(value),
            other => Err(unexpected_result(other)),
        }
    }

    async fn get_kv(&self, key: &str) -> Result<Option<Bytes>, StreamError> {
        Ok(self.node.views.load().state.get_kv(key))
    }

    async fn del_kv(&self, key: &str) -> Result<Option<Bytes>, StreamError> {
        let result = self
            .node
            .propose(Command::DeleteKv {
                key: key.to_owned(),
            })
            .await?;

        value_of(result)
    }

    async fn del_kv_if(&self, key: &str, expected: &Bytes) -> Result<Option<Bytes>, StreamError> {
        let result = self
            .node
            .propose_unless_redundant(Command::DeleteKvIfMatches {
                key: key.to_owned(),
                expected: expected.clone(),
            })
            .await
            .map_err(|e| e.to_stream_error())?;

        match result {
            Some(result) => value_of(result),
            None => Ok(None),
        }
    }

    async fn list_kv(&self, prefix: &str) -> Result<Vec<KeyValue>, StreamError> {
        Ok(self
            .node
            .views
            .load()
            .state
            .list_kv(prefix)
            .into_iter()
            .map(|(key, value)| KeyValue { key, value })
            .collect())
    }
}
