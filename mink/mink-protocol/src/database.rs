//! Requests and replies for creating, dropping, listing and probing databases.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateDatabase {
    pub name: String,
    pub comment: Option<String>,
    #[serde(default)]
    pub custom: BTreeMap<String, String>,
    #[serde(default)]
    pub ignore_if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropDatabase {
    pub name: String,
    #[serde(default)]
    pub ignore_if_not_exists: bool,
    #[serde(default)]
    pub cascade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseName {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Names {
    pub names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exists {
    pub exists: bool,
}
