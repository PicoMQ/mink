//! A validated database, table or partition value name in the allowed alphabet and length.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

pub const MAX_NAME_LENGTH: usize = 200;

const INTERNAL_PREFIX: &str = "__";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Name(String);

impl Name {
    pub fn new(name: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::EmptyName);
        }
        if name == "." || name == ".." {
            return Err(Error::ReservedName(name));
        }
        if name.len() > MAX_NAME_LENGTH {
            return Err(Error::NameLength(name));
        }

        let valid = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '-';
        if !name.chars().all(valid) {
            return Err(Error::NameCharacter(name));
        }

        Ok(Name(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_internal(&self) -> bool {
        self.0.starts_with(INTERNAL_PREFIX)
    }
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Name {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Name::new(s)
    }
}

impl Serialize for Name {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Name::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_allowed_alphabet() {
        for name in ["a", "A", "0", "_", "-", "user_events-v2", "__internal"] {
            assert_eq!(Name::new(name).unwrap().as_str(), name);
        }
        assert_eq!(Name::new("a".repeat(MAX_NAME_LENGTH)).unwrap().0.len(), 200);
    }

    #[test]
    fn rejects_everything_else() {
        assert_eq!(Name::new("").unwrap_err(), Error::EmptyName);
        assert_eq!(Name::new(".").unwrap_err(), Error::ReservedName(".".into()));
        assert_eq!(
            Name::new("..").unwrap_err(),
            Error::ReservedName("..".into())
        );
        assert!(matches!(
            Name::new("a".repeat(MAX_NAME_LENGTH + 1)).unwrap_err(),
            Error::NameLength(_)
        ));
        for name in ["a.b", "a b", "a$b", "é", "a/b", "..."] {
            assert_eq!(
                Name::new(name).unwrap_err(),
                Error::NameCharacter(name.into())
            );
        }
    }

    #[test]
    fn internal_prefix() {
        assert!(Name::new("__meta").unwrap().is_internal());
        assert!(!Name::new("_meta").unwrap().is_internal());
    }

    #[test]
    fn serde_validates() {
        let name: Name = serde_json::from_str("\"orders\"").unwrap();
        assert_eq!(serde_json::to_string(&name).unwrap(), "\"orders\"");
        assert!(serde_json::from_str::<Name>("\"a.b\"").is_err());
    }
}
