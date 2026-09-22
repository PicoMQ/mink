//! Topics as log tables of one database: name checks, the fixed descriptor, topic ids, bucket
//! addressing and creation through the cluster admin.

use std::time::Duration;

use mink_metadata::{TableRow, View};
use mink_table::{Bucket, BucketId, Column, Descriptor, Id, Name, Options, Path, Schema};
use mink_types::{DataType, Precision};
use tokio::time::Instant;
use uuid::Uuid;

use crate::Kafka;
use crate::error::Error;

pub(crate) const KEY: &str = "key";
pub(crate) const VALUE: &str = "value";
pub(crate) const HEADERS: &str = "headers";
pub(crate) const TIMESTAMP: &str = "timestamp";

const ID_MARKER: u64 = 0x6d6f_612d_746f_7069;
const CREATE_WAIT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Topic {
    pub name: String,
    pub id: Id,
    pub partitions: u32,
}

impl Topic {
    pub fn of(path: &Path, row: &TableRow) -> Option<Topic> {
        is_topic(row).then(|| Topic {
            name: path.table().as_str().to_owned(),
            id: row.table_id,
            partitions: row.descriptor.bucket_count().unwrap_or(1),
        })
    }

    pub fn uuid(&self) -> Uuid {
        uuid(self.id)
    }

    pub fn bucket(&self, partition: i32) -> Result<Bucket, Error> {
        u32::try_from(partition)
            .ok()
            .filter(|p| *p < self.partitions)
            .map(|p| Bucket::new(self.id, BucketId(p)))
            .ok_or_else(|| Error::UnknownPartition(self.name.clone(), partition))
    }

    pub fn buckets(&self) -> impl Iterator<Item = (i32, Bucket)> + '_ {
        (0..self.partitions).map(|p| (p as i32, Bucket::new(self.id, BucketId(p))))
    }
}

pub(crate) fn path(database: &str, topic: &str) -> Result<Path, Error> {
    let table = Name::new(topic).map_err(|_| Error::InvalidTopic(topic.to_owned()))?;
    let database = Name::new(database)?;

    Ok(Path::new(database, table))
}

pub(crate) fn schema() -> Result<Schema, Error> {
    Ok(Schema::builder()
        .column(Column::new(KEY, DataType::bytes())?)
        .column(Column::new(VALUE, DataType::bytes())?)
        .column(Column::new(HEADERS, DataType::bytes())?)
        .column(Column::new(
            TIMESTAMP,
            DataType::timestamp_ltz(Precision::MILLIS).with_nullable(false),
        )?)
        .build()?)
}

pub(crate) fn descriptor(
    partitions: u32,
    retention: Option<Duration>,
) -> Result<Descriptor, Error> {
    let mut options = Options::default();
    if let Some(retention) = retention {
        options.log_ttl = Some(retention);
    }

    Ok(Descriptor::builder(schema()?)
        .bucket_count(partitions)
        .options(options)
        .build()?)
}

fn is_topic(row: &TableRow) -> bool {
    let Ok(expected) = schema() else {
        return false;
    };
    let columns = row.descriptor.schema().columns();
    !row.descriptor.is_partitioned()
        && !row.descriptor.has_primary_key()
        && columns.len() == expected.columns().len()
        && columns
            .iter()
            .zip(expected.columns())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
}

fn uuid(id: Id) -> Uuid {
    Uuid::from_u64_pair(ID_MARKER, id.0)
}

pub(crate) fn table_id(uuid: Uuid) -> Option<Id> {
    let (high, low) = uuid.as_u64_pair();
    (high == ID_MARKER).then_some(Id(low))
}

impl Kafka {
    pub(crate) fn topic(&self, view: &View, name: &str) -> Result<Topic, Error> {
        let path = path(&self.inner.config.database, name)?;
        view.state
            .catalog
            .tables
            .get(&path)
            .and_then(|row| Topic::of(&path, row))
            .ok_or_else(|| Error::UnknownTopic(name.to_owned()))
    }

    pub(crate) fn topic_by_id(&self, view: &View, id: Uuid) -> Result<Topic, Error> {
        let table = table_id(id).ok_or(Error::UnknownTopicId(id))?;
        let path = view
            .state
            .catalog
            .table_paths
            .get(&table)
            .filter(|path| path.database().as_str() == self.inner.config.database)
            .ok_or(Error::UnknownTopicId(id))?;
        view.state
            .catalog
            .tables
            .get(path)
            .and_then(|row| Topic::of(path, row))
            .ok_or(Error::UnknownTopicId(id))
    }

    pub(crate) fn topics(&self, view: &View) -> Vec<Topic> {
        view.state
            .catalog
            .tables
            .iter()
            .filter(|(path, _)| path.database().as_str() == self.inner.config.database)
            .filter_map(|(path, row)| Topic::of(path, row))
            .collect()
    }

    pub(crate) fn leader(&self, view: &View, bucket: Bucket) -> Option<(i32, i32)> {
        view.state
            .catalog
            .buckets
            .get(&bucket)
            .filter(|row| row.leader >= 0)
            .map(|row| (row.leader, row.leader_epoch))
    }

    pub(crate) async fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        retention: Option<Duration>,
    ) -> Result<Topic, Error> {
        let path = path(&self.inner.config.database, name)?;
        self.ensure_database(path.database().as_str()).await?;
        self.create_table(&path, &descriptor(partitions, retention)?)
            .await?;

        let views = self.inner.service.node().metadata().views();
        let mut applied = views.subscribe();
        let deadline = Instant::now() + CREATE_WAIT;
        loop {
            if let Ok(topic) = self.topic(&views.load(), name) {
                return Ok(topic);
            }
            if tokio::time::timeout_at(deadline, applied.changed())
                .await
                .is_err()
            {
                return Err(Error::UnknownTopic(name.to_owned()));
            }
        }
    }

    pub(crate) async fn delete_topic(&self, name: &str) -> Result<(), Error> {
        let path = path(&self.inner.config.database, name)?;
        self.topic(&self.inner.service.view(), name)?;
        if self.inner.coordinator.is_leader() {
            self.inner.coordinator.drop_table(&path, false).await?;
        } else {
            self.inner.admin.drop_table(&path, false).await?;
        }

        Ok(())
    }

    pub(crate) async fn ensure_database(&self, name: &str) -> Result<(), Error> {
        if self.inner.coordinator.is_leader() {
            self.inner
                .coordinator
                .create_database(name, None, Default::default(), true)
                .await?;
        } else {
            self.inner
                .admin
                .create_database(name, None, Default::default(), true)
                .await?;
        }

        Ok(())
    }

    async fn create_table(&self, path: &Path, descriptor: &Descriptor) -> Result<(), Error> {
        if self.inner.coordinator.is_leader() {
            self.inner
                .coordinator
                .create_table(path, descriptor, false)
                .await?;
        } else {
            self.inner
                .admin
                .create_table(path, descriptor, false)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_ids_round_trip_through_table_ids() {
        let id = Id(42);
        assert_eq!(table_id(uuid(id)), Some(id));
        assert_eq!(table_id(Uuid::nil()), None);
    }

    #[test]
    fn names_follow_table_naming() {
        assert!(path("kafka", "orders-v2").is_ok());
        assert!(matches!(
            path("kafka", "orders.v2"),
            Err(Error::InvalidTopic(_))
        ));
        assert!(matches!(path("kafka", ""), Err(Error::InvalidTopic(_))));
    }

    #[test]
    fn partitions_map_to_buckets() {
        let topic = Topic {
            name: "t".into(),
            id: Id(7),
            partitions: 2,
        };
        assert_eq!(topic.bucket(1).unwrap(), Bucket::new(Id(7), BucketId(1)));
        assert!(matches!(topic.bucket(2), Err(Error::UnknownPartition(..))));
        assert!(matches!(topic.bucket(-1), Err(Error::UnknownPartition(..))));
    }
}
