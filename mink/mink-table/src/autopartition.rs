//! Settings for automatically creating and expiring time-based partitions.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    Hour,
    #[default]
    Day,
    Month,
    Quarter,
    Year,
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TimeUnit::Hour => "hour",
            TimeUnit::Day => "day",
            TimeUnit::Month => "month",
            TimeUnit::Quarter => "quarter",
            TimeUnit::Year => "year",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AutoPartition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub time_unit: TimeUnit,
    pub time_zone: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_precreate: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_retention: Option<u32>,
}

impl Default for AutoPartition {
    fn default() -> Self {
        AutoPartition {
            key: None,
            time_unit: TimeUnit::Day,
            time_zone: "UTC".to_owned(),
            num_precreate: None,
            num_retention: Some(7),
        }
    }
}

impl AutoPartition {
    pub fn precreate(&self, partition_keys: &[String]) -> u32 {
        self.num_precreate
            .unwrap_or(if partition_keys.len() > 1 { 0 } else { 2 })
    }

    pub fn key_index(&self, partition_keys: &[String]) -> usize {
        match &self.key {
            Some(key) => partition_keys.iter().position(|k| k == key).unwrap_or(0),
            None => 0,
        }
    }
}
