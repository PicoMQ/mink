//! The effective configuration of a node as flat key-value entries.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigEntries {
    pub entries: BTreeMap<String, String>,
}
