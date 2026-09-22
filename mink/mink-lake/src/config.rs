//! Lake configuration: which format and catalog to use, with connection properties.

use std::collections::BTreeMap;

use mink_table::LakeFormat;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum Config {
    Iceberg(Iceberg),
}

impl Config {
    pub fn format(&self) -> LakeFormat {
        match self {
            Config::Iceberg(_) => LakeFormat::Iceberg,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Iceberg {
    #[serde(default)]
    pub catalog: CatalogKind,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default)]
    pub warehouse: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogKind {
    #[default]
    Rest,
    Memory,
}

impl Iceberg {
    pub fn rest(uri: impl Into<String>, warehouse: impl Into<String>) -> Self {
        Iceberg {
            catalog: CatalogKind::Rest,
            uri: Some(uri.into()),
            warehouse: Some(warehouse.into()),
            properties: BTreeMap::new(),
        }
    }

    pub fn memory(warehouse: impl Into<String>) -> Self {
        Iceberg {
            catalog: CatalogKind::Memory,
            uri: None,
            warehouse: Some(warehouse.into()),
            properties: BTreeMap::new(),
        }
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}
