use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

use mink_metrics::Metrics;

use crate::Error;
use crate::cluster::Cluster;
use crate::config::Config;

pub struct Pool {
    config: Config,
    metrics: Metrics,
    idle: Mutex<Vec<Cluster>>,
}

pub struct PooledCluster<'a> {
    pool: &'a Pool,
    cluster: Option<Cluster>,
}

impl Pool {
    pub fn new(config: Config, metrics: Metrics) -> Pool {
        Pool {
            config,
            metrics,
            idle: Mutex::new(Vec::new()),
        }
    }

    pub async fn cluster(&self) -> Result<PooledCluster<'_>, Error> {
        let idle = self
            .idle
            .lock()
            .expect("the cluster pool is never poisoned")
            .pop();

        let cluster = match idle {
            Some(cluster) => cluster,
            None => Cluster::connect(self.config.clone(), self.metrics.clone()).await?,
        };

        Ok(PooledCluster {
            pool: self,
            cluster: Some(cluster),
        })
    }
}

impl Deref for PooledCluster<'_> {
    type Target = Cluster;

    fn deref(&self) -> &Cluster {
        self.cluster
            .as_ref()
            .expect("a pooled cluster is held until dropped")
    }
}

impl DerefMut for PooledCluster<'_> {
    fn deref_mut(&mut self) -> &mut Cluster {
        self.cluster
            .as_mut()
            .expect("a pooled cluster is held until dropped")
    }
}

impl Drop for PooledCluster<'_> {
    fn drop(&mut self) {
        if let Some(cluster) = self.cluster.take() {
            self.pool
                .idle
                .lock()
                .expect("the cluster pool is never poisoned")
                .push(cluster);
        }
    }
}
