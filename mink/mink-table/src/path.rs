//! A fully qualified table path made of a database name and a table name.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, Name};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Path {
    database: Name,
    table: Name,
}

impl Path {
    pub fn new(database: Name, table: Name) -> Self {
        Path { database, table }
    }

    pub fn database(&self) -> &Name {
        &self.database
    }

    pub fn table(&self) -> &Name {
        &self.table
    }

    pub fn is_internal(&self) -> bool {
        self.database.is_internal() || self.table.is_internal()
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

impl FromStr for Path {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (database, table) = s.split_once('.').ok_or_else(|| Error::Path(s.to_owned()))?;
        Ok(Path::new(Name::new(database)?, Name::new(table)?))
    }
}

impl Serialize for Path {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Path {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_round_trip() {
        let path: Path = "shop.orders".parse().unwrap();
        assert_eq!(path.database().as_str(), "shop");
        assert_eq!(path.table().as_str(), "orders");
        assert_eq!(path.to_string(), "shop.orders");
    }

    #[test]
    fn requires_exactly_two_valid_segments() {
        assert_eq!(
            "orders".parse::<Path>().unwrap_err(),
            Error::Path("orders".into())
        );
        assert_eq!(
            "a.b.c".parse::<Path>().unwrap_err(),
            Error::NameCharacter("b.c".into())
        );
        assert_eq!(".orders".parse::<Path>().unwrap_err(), Error::EmptyName);
    }

    #[test]
    fn internal_if_either_segment_is() {
        assert!("__sys.orders".parse::<Path>().unwrap().is_internal());
        assert!("shop.__offsets".parse::<Path>().unwrap().is_internal());
        assert!(!"shop.orders".parse::<Path>().unwrap().is_internal());
    }

    #[test]
    fn serializes_as_a_string() {
        let path: Path = "shop.orders".parse().unwrap();
        let json = serde_json::to_string(&path).unwrap();
        assert_eq!(json, "\"shop.orders\"");
        assert_eq!(serde_json::from_str::<Path>(&json).unwrap(), path);
    }
}
