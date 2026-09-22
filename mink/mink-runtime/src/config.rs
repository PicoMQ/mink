//! Node configuration loaded from defaults, a TOML file and environment, with the metadata backend URL parsed.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use mink_lake::Config;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaBackend {
    Sqlite(Option<PathBuf>),
    Postgres(String),
}

impl MetaBackend {
    pub fn parse(url: &str) -> Result<Self, ConfigError> {
        if url == "sqlite::memory:" || url == "sqlite://:memory:" {
            return Ok(Self::Sqlite(None));
        }
        if let Some(path) = url
            .strip_prefix("sqlite://")
            .or_else(|| url.strip_prefix("sqlite:"))
        {
            if path.is_empty() {
                return Err(ConfigError::MetaUrl(url.to_owned()));
            }

            return Ok(Self::Sqlite(Some(PathBuf::from(path))));
        }
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            return Ok(Self::Postgres(url.to_owned()));
        }

        Err(ConfigError::MetaUrl(url.to_owned()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("unsupported meta_url {0:?}: expected sqlite::memory:, sqlite:<path> or postgres://…")]
    MetaUrl(String),
    #[error("{0}")]
    Load(#[from] Box<figment::Error>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub node_id: i32,
    pub cluster_id: String,
    pub meta_url: String,
    pub storage_uri: String,
    pub wal_uri: Option<String>,
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    pub advertise: String,
    #[serde(with = "mink_common::serde::humantime")]
    pub wal_upload_interval: Duration,
    #[serde(with = "mink_common::serde::humantime")]
    pub kv_snapshot_interval: Duration,
    #[serde(with = "mink_common::serde::humantime")]
    pub log_retention_interval: Duration,
    #[serde(with = "mink_common::serde::humantime")]
    pub lease_ttl: Duration,
    #[serde(with = "mink_common::serde::humantime")]
    pub coordinator_tick: Duration,
    pub default_bucket_count: u32,
    pub snapshots_retained: usize,
    #[serde(with = "mink_common::serde::humantime")]
    pub producer_offsets_ttl: Duration,
    #[serde(with = "mink_common::serde::humantime")]
    pub producer_offsets_cleanup_interval: Duration,
    pub lake: Option<Config>,
    #[serde(with = "mink_common::serde::humantime")]
    pub tiering_poll_interval: Duration,
    pub kafka: Option<mink_kafka::Config>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            node_id: 1,
            cluster_id: "mink".to_owned(),
            meta_url: "sqlite:./data/meta.db".to_owned(),
            storage_uri: "-2@file://./data/objects".to_owned(),
            wal_uri: None,
            data_dir: PathBuf::from("./data"),
            listen: SocketAddr::from(([127, 0, 0, 1], 9123)),
            advertise: "grpc://127.0.0.1:9123".to_owned(),
            wal_upload_interval: Duration::from_secs(10),
            kv_snapshot_interval: Duration::from_secs(10 * 60),
            log_retention_interval: Duration::from_secs(5 * 60),
            lease_ttl: Duration::from_secs(30),
            coordinator_tick: Duration::from_secs(5),
            default_bucket_count: 1,
            snapshots_retained: 1,
            producer_offsets_ttl: Duration::from_secs(24 * 60 * 60),
            producer_offsets_cleanup_interval: Duration::from_secs(60 * 60),
            lake: None,
            tiering_poll_interval: Duration::from_secs(30),
            kafka: None,
        }
    }
}

impl ServerConfig {
    pub fn load(file: Option<&Path>) -> Result<Self, ConfigError> {
        let mut figment = Figment::from(Serialized::defaults(ServerConfig::default()));
        if let Some(file) = file {
            figment = figment.merge(Toml::file(file));
        }

        figment
            .merge(Env::prefixed("MINK_"))
            .extract()
            .map_err(|e| ConfigError::Load(Box::new(e)))
    }

    pub fn meta_backend(&self) -> Result<MetaBackend, ConfigError> {
        MetaBackend::parse(&self.meta_url)
    }

    pub fn wal_uri(&self) -> String {
        self.wal_uri
            .clone()
            .unwrap_or_else(|| derive_wal_uri(&self.storage_uri))
    }

    pub fn describe(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        let value = serde_json::to_value(self).expect("config serializes");
        mink_common::json::flatten(&value, String::new(), &mut out);
        out.insert("wal_uri".to_owned(), self.wal_uri());
        if let Some(url) = out.get_mut("meta_url") {
            *url = mink_common::url::redact(url);
        }
        for (key, value) in out.iter_mut() {
            let leaf = key.rsplit(['.', '-', '_']).next().unwrap_or(key);
            if matches!(
                leaf.to_ascii_lowercase().as_str(),
                "password" | "secret" | "token" | "key" | "credential" | "credentials"
            ) {
                *value = REDACTED.to_owned();
            }
        }

        out
    }
}

const REDACTED: &str = "<redacted>";

fn derive_wal_uri(storage_uri: &str) -> String {
    match storage_uri.split_once('@') {
        Some((id, rest)) => match id.trim().parse::<i16>() {
            Ok(id) => format!("{}@{rest}", id.saturating_sub(1)),
            Err(_) => storage_uri.to_owned(),
        },
        None => storage_uri.to_owned(),
    }
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;

    #[test]
    fn parses_meta_urls() {
        assert_eq!(
            MetaBackend::parse("sqlite::memory:").unwrap(),
            MetaBackend::Sqlite(None)
        );
        assert_eq!(
            MetaBackend::parse("sqlite:/tmp/meta.db").unwrap(),
            MetaBackend::Sqlite(Some(PathBuf::from("/tmp/meta.db")))
        );
        assert!(matches!(
            MetaBackend::parse("postgres://user@host/mink").unwrap(),
            MetaBackend::Postgres(_)
        ));
        assert!(MetaBackend::parse("mysql://host/mink").is_err());
        assert!(MetaBackend::parse("sqlite:").is_err());
    }

    #[test]
    fn wal_uri_defaults_to_the_next_bucket_id() {
        let config = ServerConfig::default();
        assert_eq!(config.wal_uri(), "-3@file://./data/objects");
        let explicit = ServerConfig {
            wal_uri: Some("7@s3://wal".into()),
            ..Default::default()
        };
        assert_eq!(explicit.wal_uri(), "7@s3://wal");
    }

    #[test]
    fn file_then_environment_override_defaults() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "mink.toml",
                r#"
                node_id = 7
                kv_snapshot_interval = "2m"
                log_retention_interval = "1m"
                lease_ttl = "45s"
                "#,
            )?;
            jail.set_env("MINK_NODE_ID", "9");
            jail.set_env("MINK_CLUSTER_ID", "prod");
            let config = ServerConfig::load(Some(Path::new("mink.toml"))).unwrap();
            assert_eq!(config.node_id, 9);
            assert_eq!(config.cluster_id, "prod");
            assert_eq!(config.kv_snapshot_interval, Duration::from_secs(120));
            assert_eq!(config.log_retention_interval, Duration::from_secs(60));
            assert_eq!(config.lease_ttl, Duration::from_secs(45));
            assert_eq!(config.default_bucket_count, 1);
            Ok(())
        });
    }

    #[test]
    fn lake_block_reads_format_keys() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "mink.toml",
                r#"
                tiering_poll_interval = "5s"
                [lake]
                format = "iceberg"
                uri = "http://rest:8181"
                warehouse = "s3://lake"
                [lake.properties]
                "s3.endpoint" = "http://minio:9000"
                "#,
            )?;
            let config = ServerConfig::load(Some(Path::new("mink.toml"))).unwrap();
            let Some(Config::Iceberg(iceberg)) = &config.lake else {
                panic!("expected an iceberg lake, got {:?}", config.lake);
            };
            assert_eq!(iceberg.uri.as_deref(), Some("http://rest:8181"));
            assert_eq!(iceberg.warehouse.as_deref(), Some("s3://lake"));
            assert_eq!(iceberg.properties["s3.endpoint"], "http://minio:9000");
            assert_eq!(config.tiering_poll_interval, Duration::from_secs(5));
            assert_eq!(ServerConfig::default().lake, None);
            Ok(())
        });
    }

    #[test]
    fn describe_flattens_and_redacts() {
        let config = ServerConfig {
            meta_url: "postgres://mink:hunter2@db:5432/mink".into(),
            lake: Some(Config::Iceberg(mink_lake::iceberg::Config {
                uri: Some("http://rest:8181".into()),
                warehouse: Some("s3://lake".into()),
                properties: [
                    ("s3.endpoint".to_owned(), "http://minio:9000".to_owned()),
                    ("s3.secret-access-key".to_owned(), "shh".to_owned()),
                ]
                .into(),
                catalog: Default::default(),
            })),
            ..Default::default()
        };
        let described = config.describe();
        assert_eq!(
            described["meta_url"],
            "postgres://mink:<redacted>@db:5432/mink"
        );
        assert_eq!(described["kv_snapshot_interval"], "10m");
        assert_eq!(described["wal_uri"], "-3@file://./data/objects");
        assert_eq!(described["lake.format"], "iceberg");
        assert_eq!(
            described["lake.properties.s3.endpoint"],
            "http://minio:9000"
        );
        assert_eq!(described["lake.properties.s3.secret-access-key"], REDACTED);
        assert!(
            !described.contains_key("lake"),
            "objects flatten, not stringify"
        );
    }

    #[test]
    fn kafka_block_is_optional() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "mink.toml",
                r#"
                [kafka]
                listen = "0.0.0.0:9092"
                advertise = "broker-1:9092"
                database = "topics"
                "#,
            )?;
            let config = ServerConfig::load(Some(Path::new("mink.toml"))).unwrap();
            let kafka = config.kafka.expect("kafka block");
            assert_eq!(kafka.listen, "0.0.0.0:9092".parse().unwrap());
            assert_eq!(kafka.advertise.as_deref(), Some("broker-1:9092"));
            assert_eq!(kafka.database, "topics");
            assert!(ServerConfig::default().kafka.is_none());
            Ok(())
        });
    }

    #[test]
    fn unknown_keys_are_rejected() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("mink.toml", "zookeeper_address = \"zk:2181\"\n")?;
            assert!(ServerConfig::load(Some(Path::new("mink.toml"))).is_err());
            Ok(())
        });
    }
}
