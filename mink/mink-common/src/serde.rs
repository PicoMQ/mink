//! Serde adapters for durations: integer milliseconds on the wire and human-readable strings in config.

pub mod millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        u64::try_from(value.as_millis())
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        u64::deserialize(deserializer).map(Duration::from_millis)
    }
}

pub mod option_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => i64::try_from(value.as_millis())
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer),
            None => (-1i64).serialize(serializer),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        let millis = i64::deserialize(deserializer)?;
        Ok(u64::try_from(millis).ok().map(Duration::from_millis))
    }
}

pub mod humantime {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        ::humantime::format_duration(*value)
            .to_string()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let text = String::deserialize(deserializer)?;
        ::humantime::parse_duration(&text).map_err(serde::de::Error::custom)
    }
}

pub mod option_humantime {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .map(|d| ::humantime::format_duration(d).to_string())
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|text| ::humantime::parse_duration(&text).map_err(serde::de::Error::custom))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Wire {
        #[serde(with = "super::millis")]
        every: Duration,
        #[serde(with = "super::option_millis")]
        ttl: Option<Duration>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Config {
        #[serde(with = "super::humantime")]
        every: Duration,
        #[serde(with = "super::option_humantime")]
        ttl: Option<Duration>,
    }

    #[test]
    fn millis_round_trip_with_minus_one_as_none() {
        let wire = Wire {
            every: Duration::from_millis(1500),
            ttl: None,
        };
        let json = serde_json::to_string(&wire).unwrap();
        assert_eq!(json, r#"{"every":1500,"ttl":-1}"#);
        assert_eq!(serde_json::from_str::<Wire>(&json).unwrap(), wire);
        let some: Wire = serde_json::from_str(r#"{"every":0,"ttl":10}"#).unwrap();
        assert_eq!(some.ttl, Some(Duration::from_millis(10)));
    }

    #[test]
    fn humantime_round_trip() {
        let config = Config {
            every: Duration::from_secs(90),
            ttl: Some(Duration::from_secs(7 * 24 * 3600)),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(json, r#"{"every":"1m 30s","ttl":"7days"}"#);
        assert_eq!(serde_json::from_str::<Config>(&json).unwrap(), config);
        let none: Config = serde_json::from_str(r#"{"every":"10ms","ttl":null}"#).unwrap();
        assert_eq!(none.ttl, None);
    }
}
