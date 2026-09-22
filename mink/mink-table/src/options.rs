//! Table options with their defaults and the JSON encoding that omits default values.

use std::ops::Not;
use std::time::Duration;

use mink_common::serde::{millis, option_millis};
use serde::{Deserialize, Serialize};

use crate::{
    AutoPartition, ChangelogImage, DeleteBehavior, KvFormat, LakeFormat, LogFormat, MergeEngine,
};

pub const DEFAULT_LAKE_FRESHNESS: Duration = Duration::from_secs(3 * 60);
pub const DEFAULT_LOG_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    pub log_format: LogFormat,
    pub kv_format: KvFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge_engine: Option<MergeEngine>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_behavior: Option<DeleteBehavior>,
    pub changelog_image: ChangelogImage,
    #[serde(
        rename = "log_ttl_ms",
        with = "option_millis",
        skip_serializing_if = "is_default_log_ttl"
    )]
    pub log_ttl: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lake: Option<LakeFormat>,
    #[serde(
        rename = "lake_freshness_ms",
        with = "millis",
        skip_serializing_if = "is_default_freshness"
    )]
    pub lake_freshness: Duration,
    #[serde(skip_serializing_if = "Not::not")]
    pub lake_auto_compaction: bool,
    #[serde(skip_serializing_if = "Not::not")]
    pub lake_attach: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_partition: Option<AutoPartition>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            log_format: LogFormat::default(),
            kv_format: KvFormat::default(),
            merge_engine: None,
            delete_behavior: None,
            changelog_image: ChangelogImage::default(),
            log_ttl: Some(DEFAULT_LOG_TTL),
            lake: None,
            lake_freshness: DEFAULT_LAKE_FRESHNESS,
            lake_auto_compaction: false,
            lake_attach: false,
            auto_partition: None,
        }
    }
}

fn is_default_freshness(freshness: &Duration) -> bool {
    *freshness == DEFAULT_LAKE_FRESHNESS
}

fn is_default_log_ttl(ttl: &Option<Duration>) -> bool {
    *ttl == Some(DEFAULT_LOG_TTL)
}
