use uuid::Uuid;

use crate::Error;

const MAX_NAME_LENGTH: usize = 249;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    name: String,
    uuid: Uuid,
    partition_count: i32,
}

impl Topic {
    pub fn new(name: impl Into<String>, uuid: Uuid, partition_count: i32) -> Result<Topic, Error> {
        let name = name.into();
        if !valid_name(&name) {
            return Err(Error::TopicName(name));
        }
        if partition_count < 1 {
            return Err(Error::PartitionCount(partition_count));
        }

        Ok(Topic {
            name,
            uuid,
            partition_count,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn partition_count(&self) -> i32 {
        self.partition_count
    }

    pub fn partitions(&self) -> impl Iterator<Item = Partition> + '_ {
        (0..self.partition_count).map(|index| Partition {
            topic: self.name.clone(),
            index,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableState {
    Indexing,
    Removing,
    Replaced,
    Missing,
}

impl TableState {
    pub const ALL: [TableState; 4] = [
        TableState::Indexing,
        TableState::Removing,
        TableState::Replaced,
        TableState::Missing,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            TableState::Indexing => "indexing",
            TableState::Removing => "removing",
            TableState::Replaced => "replaced",
            TableState::Missing => "missing",
        }
    }

    pub fn from_name(name: &str) -> Option<TableState> {
        TableState::ALL
            .into_iter()
            .find(|state| state.name() == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Partition {
    topic: String,
    index: i32,
}

impl Partition {
    pub fn new(topic: impl Into<String>, index: i32) -> Result<Partition, Error> {
        let topic = topic.into();
        if !valid_name(&topic) {
            return Err(Error::TopicName(topic));
        }
        if index < 0 {
            return Err(Error::PartitionIndex(index));
        }

        Ok(Partition { topic, index })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn index(&self) -> i32 {
        self.index
    }
}

fn valid_name(name: &str) -> bool {
    let legal = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');

    (1..=MAX_NAME_LENGTH).contains(&name.len())
        && name != "."
        && name != ".."
        && name.chars().all(legal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_states_parse_from_their_names() {
        for state in TableState::ALL {
            assert_eq!(TableState::from_name(state.name()), Some(state));
        }

        assert_eq!(TableState::from_name("indexed"), None);
    }

    #[test]
    fn names_follow_kafka_topic_rules() {
        for name in ["orders", "orders.v2", "a_b-c", "x", &"t".repeat(249)] {
            assert!(Topic::new(name, Uuid::nil(), 1).is_ok(), "{name}");
        }

        for name in ["", ".", "..", "a/b", "a b", "ordérs", &"t".repeat(250)] {
            assert_eq!(
                Topic::new(name, Uuid::nil(), 1),
                Err(Error::TopicName(name.to_owned())),
                "{name}"
            );
        }
    }

    #[test]
    fn a_topic_needs_at_least_one_partition() {
        assert_eq!(
            Topic::new("orders", Uuid::nil(), 0),
            Err(Error::PartitionCount(0))
        );
    }

    #[test]
    fn partitions_are_rejected_with_an_illegal_topic_or_a_negative_index() {
        assert_eq!(
            Partition::new("a b", 0),
            Err(Error::TopicName("a b".to_owned()))
        );
        assert_eq!(Partition::new("orders", -1), Err(Error::PartitionIndex(-1)));
    }

    #[test]
    fn a_topic_lists_every_one_of_its_partitions() {
        let topic = Topic::new("orders", Uuid::nil(), 3).unwrap();

        let indexes: Vec<_> = topic
            .partitions()
            .map(|partition| partition.index())
            .collect();

        assert_eq!(indexes, [0, 1, 2]);
        assert!(
            topic
                .partitions()
                .all(|partition| partition.topic() == "orders")
        );
    }
}
