//! The live-node source the coordinator consults, with a static implementation for tests.

use std::collections::BTreeSet;
use std::sync::RwLock;

use async_trait::async_trait;
use mink_common::sync::{read, write};

use crate::Error;

#[async_trait]
pub trait Membership: Send + Sync {
    async fn live_nodes(&self) -> Result<BTreeSet<i32>, Error>;
}

#[derive(Debug, Default)]
pub struct StaticMembership {
    nodes: RwLock<BTreeSet<i32>>,
}

impl StaticMembership {
    pub fn new(nodes: impl IntoIterator<Item = i32>) -> Self {
        StaticMembership {
            nodes: RwLock::new(nodes.into_iter().collect()),
        }
    }

    pub fn set(&self, nodes: impl IntoIterator<Item = i32>) {
        *write(&self.nodes) = nodes.into_iter().collect();
    }

    pub fn add(&self, node: i32) {
        write(&self.nodes).insert(node);
    }

    pub fn remove(&self, node: i32) {
        write(&self.nodes).remove(&node);
    }
}

#[async_trait]
impl Membership for StaticMembership {
    async fn live_nodes(&self) -> Result<BTreeSet<i32>, Error> {
        Ok(read(&self.nodes).clone())
    }
}
