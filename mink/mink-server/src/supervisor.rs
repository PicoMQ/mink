use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mink_core::{Scope, Topic};
use mink_metrics::Metrics;
use mink_store::Lease;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::Error;
use crate::indexer::Indexer;
use crate::owners::{owner, preference, scope_key, share};
use crate::registration::{delete_removed, register};
use crate::services::Services;
use crate::writer::Writer;

struct Supervisor {
    services: Arc<Services>,
    holder: Uuid,
    running: HashMap<Scope, Assignment>,
    stopping: HashMap<Scope, Assignment>,
}

struct Assignment {
    lease: Lease,
    stop: watch::Sender<bool>,
    task: JoinHandle<Result<(), Error>>,
}

enum Worker {
    Indexer(Indexer),
    Writer(Writer),
}

pub(crate) async fn supervise(services: Arc<Services>, mut stop: watch::Receiver<bool>) {
    let interval = services.config.lease_ttl / 3;
    let mut supervisor = Supervisor {
        holder: services.membership.id(),
        services,
        running: HashMap::new(),
        stopping: HashMap::new(),
    };

    while !*stop.borrow() {
        tokio::select! {
            supervised = supervisor.supervise_once() => {
                if let Err(error) = supervised {
                    tracing::warn!(%error, "supervision failed");
                }

                supervisor.count_leases();
            }
            _ = stop.changed() => break,
        }

        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            changed = stop.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }

    supervisor.stop_all().await;
}

impl Supervisor {
    async fn supervise_once(&mut self) -> Result<(), Error> {
        self.collect_finished().await;
        self.renew().await;

        let topics = register(&self.services).await?;
        self.stop_unlisted(&topics);

        let mut indexing = self.services.membership.indexers();
        if !indexing.contains(&self.holder) {
            indexing.push(self.holder);
        }

        for topic in &topics {
            self.balance_writer(topic, &indexing).await?;

            let partitions: Vec<Scope> = topic.partitions().map(Scope::Partition).collect();
            let share = share(partitions.len(), indexing.len());
            self.balance(&partitions, share).await?;
        }

        if let Err(error) = delete_removed(&self.services, &indexing).await {
            tracing::warn!(%error, "removed tables were not deleted");
        }
        Ok(())
    }

    async fn balance_writer(&mut self, topic: &Topic, indexing: &[Uuid]) -> Result<(), Error> {
        let scope = Scope::Topic(topic.name().to_owned());
        let writer = owner(indexing, &scope_key(&scope));

        if writer != Some(self.services.membership.id()) {
            self.stop(&scope);
        } else if !self.holds(&scope) {
            self.claim(scope).await?;
        }

        Ok(())
    }

    async fn balance(&mut self, scopes: &[Scope], share: usize) -> Result<(), Error> {
        let preferred =
            |scope: &Scope| preference(self.services.membership.id(), &scope_key(scope));
        let held: Vec<&Scope> = scopes
            .iter()
            .filter(|scope| self.running.contains_key(*scope))
            .collect();

        if held.len() > share {
            let least = held
                .into_iter()
                .min_by_key(|scope| preferred(scope))
                .cloned();

            if let Some(scope) = least {
                self.stop(&scope);
            }
            return Ok(());
        }

        let mut free: Vec<&Scope> = scopes.iter().filter(|scope| !self.holds(scope)).collect();
        free.sort_by_key(|scope| std::cmp::Reverse(preferred(scope)));
        let mut holding = held.len();

        for scope in free {
            if holding >= share || !self.room_for_partition() {
                break;
            }

            if self.claim(scope.clone()).await? {
                holding += 1;
            }
        }

        Ok(())
    }

    fn stop_unlisted(&mut self, topics: &[Topic]) {
        let unlisted: Vec<Scope> = self
            .running
            .keys()
            .filter(|scope| !topics.iter().any(|topic| topic.name() == scope.topic()))
            .cloned()
            .collect();

        for scope in unlisted {
            self.stop(&scope);
        }
    }

    fn count_leases(&self) {
        let held = self.running.len() + self.stopping.len();
        let partitions = self.held_partitions();

        let metrics = &self.services.metrics;
        metrics.leases(mink_metrics::Scope::Partition, partitions as u64);
        metrics.leases(mink_metrics::Scope::Topic, (held - partitions) as u64);
    }

    fn held_partitions(&self) -> usize {
        self.running
            .keys()
            .chain(self.stopping.keys())
            .filter(|scope| matches!(scope, Scope::Partition(_)))
            .count()
    }

    fn holds(&self, scope: &Scope) -> bool {
        self.running.contains_key(scope) || self.stopping.contains_key(scope)
    }

    fn room_for_partition(&self) -> bool {
        let held = self.held_partitions();

        self.services
            .config
            .max_partitions
            .is_none_or(|max| held < max)
    }

    fn stop(&mut self, scope: &Scope) {
        if let Some(assignment) = self.running.remove(scope) {
            assignment.stop.send_replace(true);
            self.stopping.insert(scope.clone(), assignment);
        }
    }

    async fn collect_finished(&mut self) {
        let failed: Vec<Scope> = self
            .running
            .iter()
            .filter(|(_, assignment)| assignment.task.is_finished())
            .map(|(scope, _)| scope.clone())
            .collect();
        for scope in failed {
            self.stop(&scope);
        }

        let stopped: Vec<Scope> = self
            .stopping
            .iter()
            .filter(|(_, assignment)| assignment.task.is_finished())
            .map(|(scope, _)| scope.clone())
            .collect();
        for scope in stopped {
            if let Some(assignment) = self.stopping.remove(&scope) {
                self.release(&scope, assignment).await;
            }
        }
    }

    async fn renew(&mut self) {
        let ttl = self.services.config.lease_ttl;
        let mut lost = Vec::new();

        for (scope, assignment) in self.running.iter().chain(&self.stopping) {
            if let Err(error) = self
                .services
                .store
                .leases()
                .renew(&assignment.lease, ttl)
                .await
            {
                tracing::warn!(?scope, %error, "lease not renewed; worker stopping");
                lost.push(scope.clone());
            }
        }

        for scope in lost {
            self.stop(&scope);
        }
    }

    async fn claim(&mut self, scope: Scope) -> Result<bool, Error> {
        let ttl = self.services.config.lease_ttl;
        let Some(lease) = self
            .services
            .store
            .leases()
            .acquire(&scope, self.holder, ttl)
            .await?
        else {
            return Ok(false);
        };

        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(self.services.clone(), lease.clone(), stopped));

        self.running.insert(scope, Assignment { lease, stop, task });
        Ok(true)
    }

    async fn release(&self, scope: &Scope, assignment: Assignment) {
        record_end(&self.services.metrics, scope, assignment.task.await);

        if let Err(error) = self
            .services
            .store
            .leases()
            .release(&assignment.lease)
            .await
        {
            tracing::warn!(%error, ?scope, "a lease was not released");
        }
    }

    async fn stop_all(mut self) {
        let running: Vec<Scope> = self.running.keys().cloned().collect();
        for scope in running {
            self.stop(&scope);
        }

        for (scope, assignment) in std::mem::take(&mut self.stopping) {
            self.release(&scope, assignment).await;
        }

        self.count_leases();
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        for assignment in self.running.values().chain(self.stopping.values()) {
            assignment.task.abort();
        }
    }
}

impl Worker {
    async fn start(services: Arc<Services>, lease: Lease) -> Result<Worker, Error> {
        match lease.scope().clone() {
            Scope::Partition(partition) => Ok(Worker::Indexer(
                Indexer::start(services, lease, partition).await?,
            )),
            Scope::Topic(name) => Ok(Worker::Writer(Writer::start(services, lease, name).await?)),
        }
    }

    fn interval(&self) -> Duration {
        match self {
            Worker::Indexer(indexer) => indexer.interval(),
            Worker::Writer(writer) => writer.interval(),
        }
    }

    async fn tick(&mut self) -> Result<(), Error> {
        match self {
            Worker::Indexer(indexer) => indexer.tick().await,
            Worker::Writer(writer) => writer.tick().await,
        }
    }
}

async fn run(
    services: Arc<Services>,
    lease: Lease,
    mut stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    let mut worker = Worker::start(services, lease).await?;
    let interval = worker.interval();

    while !*stop.borrow() {
        worker.tick().await?;

        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            changed = stop.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn record_end(
    metrics: &Metrics,
    scope: &Scope,
    ended: Result<Result<(), Error>, tokio::task::JoinError>,
) {
    let worker = match scope {
        Scope::Partition(_) => mink_metrics::Worker::Indexer,
        Scope::Topic(_) => mink_metrics::Worker::Writer,
    };

    if !matches!(ended, Ok(Ok(()))) {
        metrics.worker_failed(worker);
    }

    match ended {
        Ok(Err(error)) => tracing::warn!(?scope, %error, "worker failed"),
        Err(error) => tracing::warn!(?scope, %error, "worker panicked"),
        Ok(Ok(())) => {}
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use mink_core::Scope;
    use mink_store::LeaseHolder;
    use uuid::Uuid;

    use crate::Role;
    use crate::testing::{self, kafka};

    #[tokio::test]
    async fn a_node_indexes_and_commits_its_tables() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let name = format!("s{}", uuid::Uuid::new_v4().simple());
        let Some(mut config) = testing::config(&[Role::Indexer]).await else {
            return;
        };
        config.file_window = Duration::from_millis(200);
        config.snapshot_interval = Duration::from_millis(200);
        config.lease_ttl = Duration::from_secs(3);
        mink_conformance::create_topic(&bootstrap, &name, 2).await;
        crate::add_table(&config, &name, None).await.unwrap();
        mink_conformance::produce_values(&bootstrap, &name, 0, &["a", "b", "c"]);

        let node = testing::node_with(config).await;
        mink_conformance::produce_values(&bootstrap, &name, 1, &["d", "e"]);
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut covered = 0;
        while covered < 5 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Some(latest) = node.services.store.snapshots().latest(&name).await.unwrap() {
                let files = node
                    .services
                    .store
                    .snapshots()
                    .files(&latest)
                    .await
                    .unwrap();
                covered = files.iter().map(|indexed| indexed.stats().records()).sum();
            }
        }
        node.shutdown().await.unwrap();

        assert_eq!(covered, 5);
    }

    #[tokio::test]
    async fn a_removed_table_stops_and_is_deleted_once_its_leases_end() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let name = format!("s{}", uuid::Uuid::new_v4().simple());
        let Some(mut config) = testing::config(&[Role::Indexer]).await else {
            return;
        };
        config.lease_ttl = Duration::from_secs(1);
        mink_conformance::create_topic(&bootstrap, &name, 2).await;
        crate::add_table(&config, &name, None).await.unwrap();

        let node = testing::node_with(config.clone()).await;
        let deadline = Instant::now() + Duration::from_secs(30);
        while holders(&node, &name).await.len() < 3 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let held = holders(&node, &name).await.len();

        crate::remove_table(&config, &name).await.unwrap();
        let topics = node.services.store.topics();
        while topics.table(&name).await.unwrap().is_some() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let gone = topics.table(&name).await.unwrap();
        let left = holders(&node, &name).await;
        node.shutdown().await.unwrap();

        assert_eq!(held, 3);
        assert_eq!(gone, None);
        assert!(left.is_empty(), "{left:?}");
    }

    async fn holders(node: &crate::Node, topic: &str) -> Vec<LeaseHolder> {
        node.services.store.leases().holders(topic).await.unwrap()
    }

    fn partitions_of(leases: &[LeaseHolder], holder: Uuid) -> usize {
        leases
            .iter()
            .filter(|lease| {
                matches!(lease.scope(), Scope::Partition(_)) && lease.holder() == holder
            })
            .count()
    }

    #[tokio::test]
    async fn two_nodes_split_four_partitions_evenly() {
        let Some(bootstrap) = kafka() else {
            return;
        };
        let name = format!("s{}", uuid::Uuid::new_v4().simple());
        let Some(mut config) = testing::config(&[Role::Indexer]).await else {
            return;
        };
        config.lease_ttl = Duration::from_secs(2);
        mink_conformance::create_topic(&bootstrap, &name, 4).await;
        crate::add_table(&config, &name, None).await.unwrap();

        let first = testing::node_with(config.clone()).await;
        let first_id = first.services.membership.id();
        let deadline = Instant::now() + Duration::from_secs(30);
        while partitions_of(&holders(&first, &name).await, first_id) < 4
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let alone = partitions_of(&holders(&first, &name).await, first_id);

        let second = testing::node_with(config).await;
        let second_id = second.services.membership.id();
        let writer = crate::owners::owner(&[first_id, second_id], &name);
        let state = |leases: Vec<LeaseHolder>| {
            let holding = (
                partitions_of(&leases, first_id),
                partitions_of(&leases, second_id),
            );
            let writers: Vec<Uuid> = leases
                .iter()
                .filter(|lease| matches!(lease.scope(), Scope::Topic(_)))
                .map(LeaseHolder::holder)
                .collect();

            (holding, writers)
        };
        let settled = ((2, 2), writer.into_iter().collect::<Vec<Uuid>>());
        while state(holders(&first, &name).await) != settled && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let mut samples = Vec::new();
        for _ in 0..60 {
            samples.push(state(holders(&first, &name).await));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        second.shutdown().await.unwrap();
        first.shutdown().await.unwrap();

        assert_eq!(alone, 4);
        assert!(
            samples.iter().all(|sample| *sample == settled),
            "{samples:?}"
        );
    }
}
