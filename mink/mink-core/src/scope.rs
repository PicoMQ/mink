use crate::Error;
use crate::topic::Partition;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    Indexer(Partition),
    Writer(String),
    Tier(Partition),
}

impl Scope {
    pub fn from_name(name: &str, topic: &str, partition: Option<i32>) -> Result<Scope, Error> {
        match (name, partition) {
            ("indexer", Some(index)) => Ok(Scope::Indexer(Partition::new(topic, index)?)),
            ("tier", Some(index)) => Ok(Scope::Tier(Partition::new(topic, index)?)),
            ("writer", None) => Ok(Scope::Writer(topic.to_owned())),
            _ => Err(Error::Scope(name.to_owned())),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Scope::Indexer(_) => "indexer",
            Scope::Writer(_) => "writer",
            Scope::Tier(_) => "tier",
        }
    }

    pub fn topic(&self) -> &str {
        match self {
            Scope::Indexer(partition) | Scope::Tier(partition) => partition.topic(),
            Scope::Writer(topic) => topic,
        }
    }

    pub fn partition_index(&self) -> Option<i32> {
        match self {
            Scope::Indexer(partition) | Scope::Tier(partition) => Some(partition.index()),
            Scope::Writer(_) => None,
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
            Scope::Indexer(partition.clone()),
            Scope::Writer("orders".to_owned()),
            Scope::Tier(partition),
        ];

        for scope in scopes {
            let parsed = Scope::from_name(scope.name(), scope.topic(), scope.partition_index());

            assert_eq!(parsed, Ok(scope));
        }
        assert_eq!(
            Scope::from_name("writer", "orders", Some(3)),
            Err(Error::Scope("writer".to_owned()))
        );
    }
}
