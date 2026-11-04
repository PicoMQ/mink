use crate::Error;
use crate::topic::Partition;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    Partition(Partition),
    Topic(String),
    Tier(Partition),
}

impl Scope {
    pub fn from_name(name: &str, topic: &str, partition: Option<i32>) -> Result<Scope, Error> {
        match (name, partition) {
            ("partition", Some(index)) => Ok(Scope::Partition(Partition::new(topic, index)?)),
            ("tier", Some(index)) => Ok(Scope::Tier(Partition::new(topic, index)?)),
            ("topic", None) => Ok(Scope::Topic(topic.to_owned())),
            _ => Err(Error::Scope(name.to_owned())),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Scope::Partition(_) => "partition",
            Scope::Topic(_) => "topic",
            Scope::Tier(_) => "tier",
        }
    }

    pub fn topic(&self) -> &str {
        match self {
            Scope::Partition(partition) | Scope::Tier(partition) => partition.topic(),
            Scope::Topic(topic) => topic,
        }
    }

    pub fn partition_index(&self) -> Option<i32> {
        match self {
            Scope::Partition(partition) | Scope::Tier(partition) => Some(partition.index()),
            Scope::Topic(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_round_trip_through_their_names() {
        let partition = Partition::new("orders", 3).unwrap();
        let scopes = [
            Scope::Partition(partition.clone()),
            Scope::Topic("orders".to_owned()),
            Scope::Tier(partition),
        ];

        for scope in scopes {
            let parsed = Scope::from_name(scope.name(), scope.topic(), scope.partition_index());

            assert_eq!(parsed, Ok(scope));
        }
        assert_eq!(
            Scope::from_name("topic", "orders", Some(3)),
            Err(Error::Scope("topic".to_owned()))
        );
    }
}
