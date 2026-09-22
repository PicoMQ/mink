//! Takes over the streams of a dead node: fences its epoch, replays its write-ahead log and closes its streams.

use std::sync::Arc;

use async_trait::async_trait;
use mink_metadata::Handle;
use s3stream::{
    Client, DefaultFailoverFactory, FailoverFactory, FailoverRequest, ObjectManagerTrait,
    ObjectStorageTrait, StreamManagerTrait,
};

use crate::error::Error;

pub struct Failover {
    metadata: Handle,
    wal: DefaultFailoverFactory,
}

impl Failover {
    pub fn new(
        metadata: Handle,
        cluster_id: impl Into<String>,
        wal_storage: Arc<dyn ObjectStorageTrait>,
    ) -> Self {
        let stream_manager: Arc<dyn StreamManagerTrait> = Arc::new(metadata.stream_manager());
        let object_manager: Arc<dyn ObjectManagerTrait> = Arc::new(metadata.object_manager());
        Failover {
            wal: DefaultFailoverFactory::with_storage(
                cluster_id,
                wal_storage,
                stream_manager,
                object_manager,
            ),
            metadata,
        }
    }
}

#[async_trait]
impl FailoverFactory for Failover {
    fn get_stream_manager(&self, node_id: u32, epoch: u64) -> Arc<dyn StreamManagerTrait> {
        Arc::new(
            self.metadata
                .as_node(node_id as i32, epoch as i64)
                .stream_manager(),
        )
    }

    fn get_object_manager(&self, node_id: u32, epoch: u64) -> Arc<dyn ObjectManagerTrait> {
        Arc::new(
            self.metadata
                .as_node(node_id as i32, epoch as i64)
                .object_manager(),
        )
    }

    async fn get_wal(
        &self,
        request: &FailoverRequest,
    ) -> Result<Arc<dyn s3stream::WriteAheadLogTrait>, s3stream::Error> {
        self.wal.get_wal(request).await
    }
}

pub async fn take_over(
    metadata: &Handle,
    engine: &dyn Client,
    wal_config: &str,
    dead: i32,
) -> Result<(), Error> {
    let view = metadata.views().load();
    let Some(node) = view.state.nodes.get(&dead) else {
        return Ok(());
    };

    let epoch = node.epoch + 1;
    let address = node.http_address.clone();
    drop(view);

    let fenced = metadata.as_node(dead, epoch);
    fenced.register(&address, 1, Default::default()).await?;
    engine
        .failover(FailoverRequest {
            node_id: dead as u32,
            node_epoch: epoch as u64,
            wal_config: wal_config.to_owned(),
        })
        .await?;
    let manager = fenced.stream_manager();
    for stream in manager.get_opening_streams().await? {
        manager.close_stream(stream.stream_id, stream.epoch).await?;
    }

    Ok(())
}
