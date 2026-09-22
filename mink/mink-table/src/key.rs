//! A named primary key constraint over one or more columns.

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Repr", into = "Repr")]
pub struct PrimaryKey {
    name: String,
    columns: Vec<String>,
}

impl PrimaryKey {
    pub fn new(columns: Vec<String>) -> Result<Self, Error> {
        let name = format!("PK_{}", columns.join("_"));
        PrimaryKey::named(name, columns)
    }

    pub fn named(name: impl Into<String>, columns: Vec<String>) -> Result<Self, Error> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::BlankConstraintName);
        }
        if columns.is_empty() {
            return Err(Error::EmptyPrimaryKey);
        }

        let mut seen = HashSet::with_capacity(columns.len());
        for column in &columns {
            if !seen.insert(column.as_str()) {
                return Err(Error::DuplicatePrimaryKeyColumn(column.clone()));
            }
        }

        Ok(PrimaryKey { name, columns })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn contains(&self, column: &str) -> bool {
        self.columns.iter().any(|c| c == column)
    }
}

impl fmt::Display for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CONSTRAINT {} PRIMARY KEY ({})",
            self.name,
            self.columns.join(", ")
        )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Repr {
    name: String,
    columns: Vec<String>,
}

impl TryFrom<Repr> for PrimaryKey {
    type Error = Error;

    fn try_from(repr: Repr) -> Result<Self, Self::Error> {
        PrimaryKey::named(repr.name, repr.columns)
    }
}

impl From<PrimaryKey> for Repr {
    fn from(key: PrimaryKey) -> Self {
        Repr {
            name: key.name,
            columns: key.columns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn generated_name_lists_the_columns() {
        let key = PrimaryKey::new(columns(&["tenant", "id"])).unwrap();
        assert_eq!(key.name(), "PK_tenant_id");
        assert_eq!(
            key.to_string(),
            "CONSTRAINT PK_tenant_id PRIMARY KEY (tenant, id)"
        );
        assert!(key.contains("id"));
        assert!(!key.contains("ts"));
    }

    #[test]
    fn rejects_empty_blank_and_duplicate() {
        assert_eq!(PrimaryKey::new(vec![]).unwrap_err(), Error::EmptyPrimaryKey);
        assert_eq!(
            PrimaryKey::named(" ", columns(&["id"])).unwrap_err(),
            Error::BlankConstraintName
        );
        assert_eq!(
            PrimaryKey::new(columns(&["id", "id"])).unwrap_err(),
            Error::DuplicatePrimaryKeyColumn("id".into())
        );
    }

    #[test]
    fn serde_validates() {
        let key = PrimaryKey::new(columns(&["id"])).unwrap();
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, r#"{"name":"PK_id","columns":["id"]}"#);
        assert_eq!(serde_json::from_str::<PrimaryKey>(&json).unwrap(), key);
        assert!(serde_json::from_str::<PrimaryKey>(r#"{"name":"PK","columns":[]}"#).is_err());
    }
}
