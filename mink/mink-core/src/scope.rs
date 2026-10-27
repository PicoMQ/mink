use crate::topic::Partition;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    Partition(Partition),
    Topic(String),
}

impl Scope {
    pub fn topic(&self) -> &str {
        match self {
            Scope::Partition(partition) => partition.topic(),
            Scope::Topic(topic) => topic,
        }
    }

    pub fn partition_index(&self) -> Option<i32> {
        match self {
            Scope::Partition(partition) => Some(partition.index()),
            Scope::Topic(_) => None,
        }
    }
}
