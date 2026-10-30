use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use mink_core::{Granularity, Role};
use mink_kafka::{Sasl, Tls};
use nestor::DiskAdmission;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub(crate) postgres: String,
    pub(crate) catalog_address: SocketAddr,
    pub(crate) s3_address: SocketAddr,
    pub(crate) s3_endpoint: String,
    pub(crate) advertised_address: Option<String>,
    pub(crate) admin_address: SocketAddr,
    pub(crate) admin_remote: bool,
    pub(crate) advertised_admin_address: Option<String>,
    pub(crate) catalog_endpoint: String,
    pub(crate) duckdb_engine: Option<String>,
    pub(crate) duckdb_extensions: Option<String>,
    #[serde(with = "roles")]
    pub(crate) roles: Vec<Role>,
    pub(crate) kafka: Kafka,
    pub(crate) warehouse: String,
    pub(crate) storage: Storage,
    pub(crate) schema_registry: Option<SchemaRegistry>,
    pub(crate) namespace: String,
    pub(crate) history: History,
    pub(crate) file_bytes: u64,
    #[serde(with = "duration")]
    pub(crate) file_window: Duration,
    pub(crate) backfill_offsets: i64,
    #[serde(with = "duration")]
    pub(crate) retention_check: Duration,
    pub(crate) max_partitions: Option<usize>,
    #[serde(with = "duration")]
    pub(crate) lease_ttl: Duration,
    #[serde(with = "duration")]
    pub(crate) snapshot_interval: Duration,
    #[serde(with = "duration")]
    pub(crate) snapshot_retention: Duration,
    #[serde(with = "duration")]
    pub(crate) collection_interval: Duration,
    #[serde(with = "duration")]
    pub(crate) collection_grace: Duration,
    pub(crate) cache_bytes: u64,
    pub(crate) cache_dir: Option<PathBuf>,
    pub(crate) cache_disk_bytes: u64,
    pub(crate) cache_admission: DiskAdmission,
    pub(crate) max_builds: usize,
    #[serde(with = "granularity")]
    pub(crate) granularity: Granularity,
    #[serde(with = "duration")]
    pub(crate) assembly_delay: Duration,
    pub(crate) assembled_bytes: u64,
    #[serde(with = "duration")]
    pub(crate) statistics_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Kafka {
    pub(crate) bootstrap: Vec<String>,
    pub(crate) tls: Option<Tls>,
    pub(crate) sasl: Option<Sasl>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    pub(crate) endpoint: Option<String>,
    pub(crate) region: String,
    pub(crate) credentials: Option<Credentials>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistry {
    pub(crate) url: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum History {
    All,
    None,
    Since(#[serde(with = "rfc3339")] i64),
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Load(#[from] Box<figment::Error>),
    #[error("{0} must be positive")]
    NotPositive(&'static str),
    #[error("{0:?} is not a granularity: hour, day, month or none")]
    Granularity(String),
    #[error("{0:?} is not a role: catalog, s3, indexer or admin")]
    Role(String),
    #[error("statistics_interval must be shorter than snapshot_retention")]
    StatisticsInterval,
    #[error("admin_address {0} is not a loopback address; set admin_remote to allow it")]
    RemoteAdmin(SocketAddr),
}

impl Default for Config {
    fn default() -> Self {
        Config {
            postgres: "postgres://mink:mink@localhost:5432/mink".to_owned(),
            catalog_address: SocketAddr::from(([127, 0, 0, 1], 8181)),
            s3_address: SocketAddr::from(([127, 0, 0, 1], 9100)),
            s3_endpoint: "http://127.0.0.1:9100".to_owned(),
            advertised_address: None,
            admin_address: SocketAddr::from(([127, 0, 0, 1], 9180)),
            admin_remote: false,
            advertised_admin_address: None,
            catalog_endpoint: "http://127.0.0.1:8181".to_owned(),
            duckdb_engine: None,
            duckdb_extensions: None,
            roles: Role::ALL.to_vec(),
            kafka: Kafka {
                bootstrap: vec!["localhost:9092".to_owned()],
                tls: None,
                sasl: None,
            },
            warehouse: "s3://mink/warehouse".to_owned(),
            storage: Storage {
                endpoint: None,
                region: "us-east-1".to_owned(),
                credentials: None,
            },
            schema_registry: None,
            namespace: "kafka".to_owned(),
            history: History::All,
            file_bytes: 128 * 1024 * 1024,
            file_window: Duration::from_secs(60),
            backfill_offsets: 100_000,
            retention_check: Duration::from_secs(60),
            max_partitions: None,
            lease_ttl: Duration::from_secs(30),
            snapshot_interval: Duration::from_secs(10),
            snapshot_retention: Duration::from_secs(24 * 60 * 60),
            collection_interval: Duration::from_secs(10 * 60),
            collection_grace: Duration::from_secs(60 * 60),
            cache_bytes: 1024 * 1024 * 1024,
            cache_dir: None,
            cache_disk_bytes: 50 * 1024 * 1024 * 1024,
            cache_admission: DiskAdmission::Reused,
            max_builds: std::thread::available_parallelism().map_or(1, |cpus| cpus.get()),
            granularity: Granularity::Hour,
            assembly_delay: Duration::from_secs(10 * 60),
            assembled_bytes: 1024 * 1024 * 1024,
            statistics_interval: Duration::from_secs(60 * 60),
        }
    }
}

impl Config {
    pub fn load(file: Option<&Path>) -> Result<Config, ConfigError> {
        let mut figment = Figment::from(Serialized::defaults(Config::default()));
        if let Some(file) = file {
            figment = figment.merge(Toml::file(file));
        }

        let config: Config = figment
            .merge(Env::prefixed("MINK_").split("__"))
            .extract()
            .map_err(Box::new)?;

        config.validate()?;
        Ok(config)
    }

    pub fn with_roles(mut self, roles: Vec<Role>) -> Config {
        self.roles = roles;
        self
    }

    pub fn has(&self, role: Role) -> bool {
        self.roles.contains(&role)
    }

    pub fn describe(&self) -> BTreeMap<&'static str, String> {
        let roles: Vec<_> = self.roles.iter().map(Role::name).collect();

        BTreeMap::from([
            ("postgres", redact(&self.postgres)),
            ("catalog_address", self.catalog_address.to_string()),
            ("s3_address", self.s3_address.to_string()),
            ("s3_endpoint", self.s3_endpoint.clone()),
            ("roles", roles.join(",")),
            ("kafka_bootstrap", self.kafka.bootstrap.join(",")),
            ("kafka_security", self.kafka.security()),
            ("warehouse", self.warehouse.clone()),
            (
                "storage",
                self.storage
                    .endpoint
                    .clone()
                    .unwrap_or_else(|| format!("AWS S3 in {}", self.storage.region)),
            ),
            (
                "schema_registry",
                self.schema_registry
                    .as_ref()
                    .map_or("none".to_owned(), |registry| registry.url.clone()),
            ),
            ("namespace", self.namespace.clone()),
            ("admin_address", self.admin_address.to_string()),
            ("catalog_endpoint", self.catalog_endpoint.clone()),
            ("granularity", self.granularity.name().to_owned()),
            (
                "cache_dir",
                self.cache_dir
                    .as_ref()
                    .map_or("none".to_owned(), |dir| dir.display().to_string()),
            ),
        ])
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let durations = [
            ("file_window", self.file_window),
            ("retention_check", self.retention_check),
            ("lease_ttl", self.lease_ttl),
            ("snapshot_interval", self.snapshot_interval),
            ("snapshot_retention", self.snapshot_retention),
            ("collection_interval", self.collection_interval),
            ("collection_grace", self.collection_grace),
            ("assembly_delay", self.assembly_delay),
            ("statistics_interval", self.statistics_interval),
        ];
        let sizes = [
            ("file_bytes", self.file_bytes),
            ("cache_bytes", self.cache_bytes),
            ("cache_disk_bytes", self.cache_disk_bytes),
            ("max_builds", self.max_builds as u64),
            ("assembled_bytes", self.assembled_bytes),
            (
                "backfill_offsets",
                u64::try_from(self.backfill_offsets).unwrap_or(0),
            ),
            (
                "max_partitions",
                self.max_partitions.map_or(1, |count| count as u64),
            ),
        ];

        if let Some((name, _)) = durations.iter().find(|(_, value)| value.is_zero()) {
            return Err(ConfigError::NotPositive(name));
        }
        if let Some((name, _)) = sizes.iter().find(|(_, value)| *value == 0) {
            return Err(ConfigError::NotPositive(name));
        }

        if self.statistics_interval >= self.snapshot_retention {
            return Err(ConfigError::StatisticsInterval);
        }

        Ok(())
    }

    pub(crate) fn check_admin_address(&self) -> Result<(), ConfigError> {
        let remote = !self.admin_address.ip().is_loopback();

        if self.has(Role::Admin) && remote && !self.admin_remote {
            return Err(ConfigError::RemoteAdmin(self.admin_address));
        }
        Ok(())
    }
}

impl Kafka {
    fn security(&self) -> String {
        let protocol = match (&self.tls, &self.sasl) {
            (None, None) => "PLAINTEXT",
            (Some(_), None) => "SSL",
            (None, Some(_)) => "SASL_PLAINTEXT",
            (Some(_), Some(_)) => "SASL_SSL",
        };

        match &self.sasl {
            Some(sasl) => format!("{protocol} {}", sasl.mechanism().name()),
            None => protocol.to_owned(),
        }
    }

    pub(crate) fn client_config(&self) -> Result<mink_kafka::Config, mink_kafka::Error> {
        let mut config = mink_kafka::Config::new(self.bootstrap.clone())?;

        if let Some(tls) = &self.tls {
            config = config.with_tls(tls.clone());
        }
        if let Some(sasl) = &self.sasl {
            config = config.with_sasl(sasl.clone());
        }

        Ok(config)
    }
}

fn redact(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let Some((userinfo, host)) = rest.split_once('@') else {
        return url.to_owned();
    };

    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:<redacted>@{host}"),
        None => url.to_owned(),
    }
}

mod rfc3339 {
    use chrono::{DateTime, SecondsFormat};
    use serde::{Deserialize, Deserializer, Serializer, de};

    pub(super) fn serialize<S: Serializer>(
        timestamp_ms: &i64,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let time = DateTime::from_timestamp_millis(*timestamp_ms).ok_or_else(|| {
            serde::ser::Error::custom(format!("{timestamp_ms} ms is out of range"))
        })?;

        serializer.serialize_str(&time.to_rfc3339_opts(SecondsFormat::AutoSi, true))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let text = String::deserialize(deserializer)?;

        DateTime::parse_from_rfc3339(&text)
            .map(|time| time.timestamp_millis())
            .map_err(|_| de::Error::custom(format!("{text:?} is not an RFC 3339 time")))
    }
}

pub fn parse_role(text: &str) -> Result<Role, ConfigError> {
    Role::from_name(text).ok_or_else(|| ConfigError::Role(text.to_owned()))
}

pub fn parse_granularity(text: &str) -> Result<Granularity, ConfigError> {
    Granularity::from_name(text).ok_or_else(|| ConfigError::Granularity(text.to_owned()))
}

mod roles {
    use mink_core::Role;
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

    pub(super) fn serialize<S: Serializer>(
        roles: &[Role],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let names: Vec<&str> = roles.iter().map(Role::name).collect();

        names.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Role>, D::Error> {
        let names = Vec::<String>::deserialize(deserializer)?;

        names
            .iter()
            .map(|name| super::parse_role(name).map_err(de::Error::custom))
            .collect()
    }
}

mod granularity {
    use mink_core::Granularity;
    use serde::{Deserialize, Deserializer, Serializer, de};

    pub(super) fn serialize<S: Serializer>(
        granularity: &Granularity,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(granularity.name())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Granularity, D::Error> {
        let text = String::deserialize(deserializer)?;

        super::parse_granularity(&text).map_err(de::Error::custom)
    }
}

mod duration {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer, de};

    const UNITS: [(&str, u64); 4] = [("ms", 1), ("s", 1000), ("m", 60_000), ("h", 3_600_000)];

    pub(super) fn serialize<S: Serializer>(
        duration: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        let (unit, size) = UNITS
            .iter()
            .rev()
            .find(|(_, size)| millis % size == 0)
            .unwrap_or(&UNITS[0]);

        serializer.serialize_str(&format!("{}{unit}", millis / size))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        let text = String::deserialize(deserializer)?;

        parse(&text).ok_or_else(|| {
            de::Error::custom(format!(
                "{text:?} is not a number followed by ms, s, m or h"
            ))
        })
    }

    pub(super) fn parse(text: &str) -> Option<Duration> {
        let split = text.find(|character: char| !character.is_ascii_digit())?;
        let (number, unit) = text.split_at(split);
        let number: u64 = number.parse().ok()?;
        let (_, size) = UNITS.iter().find(|(name, _)| *name == unit)?;

        number.checked_mul(*size).map(Duration::from_millis)
    }
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
mod tests {
    use mink_kafka::Mechanism;

    use super::*;

    #[test]
    fn defaults_apply_without_file_or_environment() {
        figment::Jail::expect_with(|_| {
            let config = Config::load(None).unwrap();

            assert_eq!(config, Config::default());
            assert_eq!(config.roles, Role::ALL);
            Ok(())
        });
    }

    #[test]
    fn environment_overrides_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "mink.toml",
                r#"
                postgres = "postgres://file@db/mink"
                catalog_address = "0.0.0.0:8181"
                roles = ["catalog"]
                file_window = "5m"
                history = { since = "2026-10-01T00:00:00Z" }
                "#,
            )?;
            jail.set_env("MINK_POSTGRES", "postgres://env@db/mink");

            let config = Config::load(Some(Path::new("mink.toml"))).unwrap();

            assert_eq!(config.postgres, "postgres://env@db/mink");
            assert_eq!(config.catalog_address, "0.0.0.0:8181".parse().unwrap());
            assert_eq!(config.roles, [Role::Catalog]);
            assert_eq!(config.file_window, Duration::from_secs(300));
            assert_eq!(config.history, History::Since(1_790_812_800_000));
            Ok(())
        });
    }

    #[test]
    fn granularity_loads_by_name_and_rejects_other_names() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("mink.toml", "granularity = \"day\"")?;
            jail.create_file("week.toml", "granularity = \"week\"")?;

            let file = Config::load(Some(Path::new("mink.toml"))).unwrap();
            let week = Config::load(Some(Path::new("week.toml")));
            jail.set_env("MINK_GRANULARITY", "none");
            let env = Config::load(Some(Path::new("mink.toml"))).unwrap();

            assert_eq!(file.granularity, Granularity::Day);
            assert_eq!(env.granularity, Granularity::None);
            assert!(
                matches!(&week, Err(ConfigError::Load(error)) if error.to_string().contains("not a granularity")),
                "{week:?}"
            );
            assert!(matches!(
                parse_granularity("week"),
                Err(ConfigError::Granularity(_))
            ));
            Ok(())
        });
    }

    #[test]
    fn kafka_loads_tls_and_sasl_by_kafka_mechanism_names() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "mink.toml",
                r#"
                [kafka]
                bootstrap = ["b-1.msk:9096"]
                tls = {}
                sasl = { mechanism = "SCRAM-SHA-512", username = "mink", password = "secret" }
                "#,
            )?;
            jail.create_file(
                "lower.toml",
                "[kafka]\nbootstrap = [\"b-1:9096\"]\nsasl = { mechanism = \"scram-sha-512\", username = \"a\", password = \"b\" }",
            )?;

            let config = Config::load(Some(Path::new("mink.toml"))).unwrap();
            let lower = Config::load(Some(Path::new("lower.toml")));

            assert_eq!(config.kafka.bootstrap, ["b-1.msk:9096"]);
            assert_eq!(config.kafka.tls, Some(Tls::new(None)));
            assert_eq!(
                config.kafka.sasl.as_ref().map(Sasl::mechanism),
                Some(Mechanism::ScramSha512)
            );
            assert!(
                matches!(&lower, Err(ConfigError::Load(error)) if error.to_string().contains("not a SASL mechanism")),
                "{lower:?}"
            );
            Ok(())
        });
    }

    #[test]
    fn describe_shows_kafka_security_without_the_password() {
        let config = Config {
            kafka: Kafka {
                bootstrap: vec!["b-1:9096".to_owned()],
                tls: Some(Tls::new(None)),
                sasl: Some(Sasl::new(
                    Mechanism::Plain,
                    "key".to_owned(),
                    "hunter2".to_owned(),
                )),
            },
            ..Config::default()
        };

        let described = config.describe();

        assert_eq!(described["kafka_security"], "SASL_SSL PLAIN");
        assert!(described.values().all(|value| !value.contains("hunter2")));
    }

    #[test]
    fn storage_defaults_to_aws_s3_and_credentials_come_from_a_file_or_nested_variables() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "keys.toml",
                "[storage]\ncredentials = { access_key_id = \"mink\", secret_access_key = \"minkminkmink\" }",
            )?;

            let chain = Config::load(None).unwrap();
            let file = Config::load(Some(Path::new("keys.toml"))).unwrap();
            jail.set_env("MINK_STORAGE__CREDENTIALS__ACCESS_KEY_ID", "env");
            jail.set_env("MINK_STORAGE__CREDENTIALS__SECRET_ACCESS_KEY", "secret");
            let env = Config::load(None).unwrap();

            let credentials = |access_key_id: &str, secret_access_key: &str| {
                Some(Credentials {
                    access_key_id: access_key_id.to_owned(),
                    secret_access_key: secret_access_key.to_owned(),
                })
            };
            assert_eq!(chain.storage.endpoint, None);
            assert_eq!(chain.describe()["storage"], "AWS S3 in us-east-1");
            assert_eq!(chain.storage.credentials, None);
            assert_eq!(
                file.storage.credentials,
                credentials("mink", "minkminkmink")
            );
            assert_eq!(env.storage.credentials, credentials("env", "secret"));
            Ok(())
        });
    }

    #[test]
    fn a_remote_admin_address_needs_admin_remote() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("remote.toml", "admin_address = \"0.0.0.0:9180\"")?;
            jail.create_file(
                "allowed.toml",
                "admin_address = \"0.0.0.0:9180\"\nadmin_remote = true",
            )?;
            jail.create_file(
                "other.toml",
                "admin_address = \"0.0.0.0:9180\"\nroles = [\"catalog\"]",
            )?;

            let remote = Config::load(Some(Path::new("remote.toml"))).unwrap();
            let allowed = Config::load(Some(Path::new("allowed.toml"))).unwrap();
            let other = Config::load(Some(Path::new("other.toml"))).unwrap();
            let added = other.clone().with_roles(vec![Role::Admin]);

            assert!(matches!(
                remote.check_admin_address(),
                Err(ConfigError::RemoteAdmin(_))
            ));
            assert!(allowed.check_admin_address().is_ok());
            assert!(other.check_admin_address().is_ok());
            assert!(matches!(
                added.check_admin_address(),
                Err(ConfigError::RemoteAdmin(_))
            ));
            Ok(())
        });
    }

    #[test]
    fn unknown_keys_are_rejected() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("mink.toml", "listen = \"0.0.0.0:9123\"")?;

            assert!(Config::load(Some(Path::new("mink.toml"))).is_err());
            Ok(())
        });
    }

    #[test]
    fn durations_and_sizes_must_be_positive_and_times_must_parse() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("zero.toml", "lease_ttl = \"0s\"")?;
            jail.create_file("time.toml", "history = { since = \"yesterday\" }")?;
            jail.create_file("unit.toml", "file_window = \"5 minutes\"")?;

            let zero = Config::load(Some(Path::new("zero.toml")));
            let time = Config::load(Some(Path::new("time.toml")));
            let unit = Config::load(Some(Path::new("unit.toml")));

            assert!(
                matches!(zero, Err(ConfigError::NotPositive("lease_ttl"))),
                "{zero:?}"
            );
            assert!(matches!(time, Err(ConfigError::Load(_))), "{time:?}");
            assert!(matches!(unit, Err(ConfigError::Load(_))), "{unit:?}");
            Ok(())
        });
    }

    #[test]
    fn durations_are_written_in_their_largest_whole_unit() {
        let cases = [
            ("250ms", "250ms"),
            ("60s", "1m"),
            ("90s", "90s"),
            ("1440m", "24h"),
        ];

        for (read, written) in cases {
            let duration = duration::parse(read).unwrap();
            let config = serde_json::to_value(Config {
                file_window: duration,
                ..Config::default()
            })
            .unwrap();

            assert_eq!(config["file_window"], written);
            assert_eq!(duration::parse(written), Some(duration));
        }
    }

    #[test]
    fn describe_redacts_postgres_password() {
        let config = Config {
            postgres: "postgres://mink:hunter2@db:5432/mink".to_owned(),
            ..Config::default()
        };

        let described = config.describe();

        assert_eq!(
            described["postgres"],
            "postgres://mink:<redacted>@db:5432/mink"
        );
        assert_eq!(described["roles"], "catalog,s3,indexer,admin");
    }

    #[test]
    fn describe_keeps_urls_without_password() {
        let config = Config {
            postgres: "postgres://db:5432/mink".to_owned(),
            ..Config::default()
        };

        assert_eq!(config.describe()["postgres"], "postgres://db:5432/mink");
    }
}
