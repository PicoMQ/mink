//! Partition names as joined values, and partition specs as key-value pairs resolved against partition keys.

use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{Error, Name};

const SEPARATOR: char = '$';

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionName(String);

impl PartitionName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PartitionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PartitionName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for value in s.split(SEPARATOR) {
            Name::new(value)?;
        }
        Ok(PartitionName(s.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Vec<(String, Name)>", into = "Vec<(String, Name)>")]
pub struct PartitionSpec {
    entries: Vec<(String, Name)>,
}

impl PartitionSpec {
    pub fn new(entries: Vec<(String, Name)>) -> Result<Self, Error> {
        let mut seen = HashSet::with_capacity(entries.len());
        for (key, _) in &entries {
            if !seen.insert(key.as_str()) {
                return Err(Error::DuplicatePartitionKey(key.clone()));
            }
        }

        Ok(PartitionSpec { entries })
    }

    pub fn from_name(partition_keys: &[String], name: &PartitionName) -> Result<Self, Error> {
        let values = name.0.split(SEPARATOR).collect::<Vec<_>>();
        if values.len() != partition_keys.len() {
            return Err(Error::PartitionValueCount {
                name: name.0.clone(),
                found: values.len(),
                expected: partition_keys.len(),
            });
        }

        let entries = partition_keys
            .iter()
            .zip(values)
            .map(|(key, value)| Ok((key.clone(), Name::new(value)?)))
            .collect::<Result<_, Error>>()?;
        PartitionSpec::new(entries)
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|(key, _)| key.as_str())
    }

    pub fn values(&self) -> impl Iterator<Item = &Name> {
        self.entries.iter().map(|(_, value)| value)
    }

    pub fn value(&self, key: &str) -> Option<&Name> {
        self.entries
            .iter()
            .find(|(entry, _)| entry == key)
            .map(|(_, value)| value)
    }

    pub fn resolve(&self, partition_keys: &[String]) -> Result<Self, Error> {
        let mismatch = || Error::PartitionSpecKeys {
            spec: self.keys().map(str::to_owned).collect(),
            keys: partition_keys.to_vec(),
        };
        if self.entries.len() != partition_keys.len() {
            return Err(mismatch());
        }

        let entries = partition_keys
            .iter()
            .map(|key| {
                self.value(key)
                    .map(|value| (key.clone(), value.clone()))
                    .ok_or_else(mismatch)
            })
            .collect::<Result<_, _>>()?;

        Ok(PartitionSpec { entries })
    }

    pub fn name(&self) -> PartitionName {
        let mut name = String::new();
        for (index, value) in self.values().enumerate() {
            if index > 0 {
                name.push(SEPARATOR);
            }
            name.push_str(value.as_str());
        }

        PartitionName(name)
    }
}

impl TryFrom<Vec<(String, Name)>> for PartitionSpec {
    type Error = Error;

    fn try_from(entries: Vec<(String, Name)>) -> Result<Self, Self::Error> {
        PartitionSpec::new(entries)
    }
}

impl From<PartitionSpec> for Vec<(String, Name)> {
    fn from(spec: PartitionSpec) -> Self {
        spec.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(entries: &[(&str, &str)]) -> PartitionSpec {
        PartitionSpec::new(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), Name::new(*value).unwrap()))
                .collect(),
        )
        .unwrap()
    }

    fn keys(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|key| (*key).to_owned()).collect()
    }

    #[test]
    fn keys_are_unique() {
        let err = PartitionSpec::new(vec![
            ("dt".into(), Name::new("2024").unwrap()),
            ("dt".into(), Name::new("2025").unwrap()),
        ])
        .unwrap_err();
        assert_eq!(err, Error::DuplicatePartitionKey("dt".into()));
    }

    #[test]
    fn resolve_reorders_into_table_order() {
        let resolved = spec(&[("region", "eu"), ("dt", "2024-01-01")])
            .resolve(&keys(&["dt", "region"]))
            .unwrap();
        assert_eq!(resolved.keys().collect::<Vec<_>>(), ["dt", "region"]);
        assert_eq!(resolved.name().as_str(), "2024-01-01$eu");
    }

    #[test]
    fn resolve_requires_exactly_the_partition_keys() {
        let partial = spec(&[("dt", "2024-01-01")]);
        let err = partial.resolve(&keys(&["dt", "region"])).unwrap_err();
        assert_eq!(
            err,
            Error::PartitionSpecKeys {
                spec: keys(&["dt"]),
                keys: keys(&["dt", "region"]),
            }
        );
        let wrong = spec(&[("dt", "2024-01-01"), ("zone", "eu")]);
        assert!(matches!(
            wrong.resolve(&keys(&["dt", "region"])),
            Err(Error::PartitionSpecKeys { .. })
        ));
    }

    #[test]
    fn name_round_trips_through_from_name() {
        let table_keys = keys(&["dt", "region"]);
        let resolved = spec(&[("dt", "2024-01-01"), ("region", "eu")]);
        let name = resolved.name();
        assert_eq!(
            PartitionSpec::from_name(&table_keys, &name).unwrap(),
            resolved
        );
    }

    #[test]
    fn from_name_checks_arity_and_values() {
        let table_keys = keys(&["dt", "region"]);
        let err = PartitionSpec::from_name(&table_keys, &PartitionName("2024".into())).unwrap_err();
        assert_eq!(
            err,
            Error::PartitionValueCount {
                name: "2024".into(),
                found: 1,
                expected: 2,
            }
        );
        let err = PartitionSpec::from_name(&table_keys, &PartitionName("2024$eu west".into()))
            .unwrap_err();
        assert_eq!(err, Error::NameCharacter("eu west".into()));
    }

    #[test]
    fn serde_round_trip_validates() {
        let spec = spec(&[("dt", "2024-01-01"), ("region", "eu")]);
        let json = serde_json::to_string(&spec).unwrap();
        assert_eq!(json, r#"[["dt","2024-01-01"],["region","eu"]]"#);
        assert_eq!(serde_json::from_str::<PartitionSpec>(&json).unwrap(), spec);
        assert!(serde_json::from_str::<PartitionSpec>(r#"[["dt","a"],["dt","b"]]"#).is_err());
    }
}
