//! Listener settings: addresses, the topic database, creation defaults, request limits and group timeouts.

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    pub advertise: Option<String>,
    pub database: String,
    pub auto_create_topics: bool,
    pub default_partitions: u32,
    pub max_request_bytes: usize,
    pub max_fetch_bytes: usize,
    pub max_in_flight: usize,
    #[serde(with = "mink_common::serde::humantime")]
    pub max_wait: Duration,
    #[serde(with = "mink_common::serde::humantime")]
    pub min_session_timeout: Duration,
    #[serde(with = "mink_common::serde::humantime")]
    pub max_session_timeout: Duration,
    #[serde(with = "mink_common::serde::humantime")]
    pub group_offsets_ttl: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: SocketAddr::from(([127, 0, 0, 1], 9092)),
            advertise: None,
            database: "kafka".to_owned(),
            auto_create_topics: true,
            default_partitions: 1,
            max_request_bytes: 100 * 1024 * 1024,
            max_fetch_bytes: 50 * 1024 * 1024,
            max_in_flight: 64,
            max_wait: Duration::from_secs(30),
            min_session_timeout: Duration::from_secs(6),
            max_session_timeout: Duration::from_secs(30 * 60),
            group_offsets_ttl: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

impl Config {
    pub fn advertised(&self, bound: SocketAddr) -> String {
        self.advertise.clone().unwrap_or_else(|| bound.to_string())
    }
}
