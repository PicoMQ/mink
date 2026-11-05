use std::time::Duration;

use uuid::Uuid;

use crate::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTable {
    topic: String,
    table: CatalogTable,
    uuid: Uuid,
    location: String,
    schedule: TierSchedule,
    generation: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogTable {
    catalog: String,
    namespace: String,
    name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierSchedule {
    after: Duration,
    interval: Duration,
}

impl TierSchedule {
    pub fn new(after: Duration, interval: Duration) -> Result<TierSchedule, Error> {
        if after.is_zero() || interval.is_zero() {
            return Err(Error::TierSchedule);
        }

        Ok(TierSchedule { after, interval })
    }

    pub fn after(&self) -> Duration {
        self.after
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn given(
        after: Option<Duration>,
        interval: Option<Duration>,
    ) -> Result<TierSchedule, Error> {
        let defaults = TierSchedule::default();

        TierSchedule::new(
            after.unwrap_or(defaults.after),
            interval.unwrap_or(defaults.interval),
        )
    }
}

impl Default for TierSchedule {
    fn default() -> TierSchedule {
        TierSchedule {
            after: Duration::from_secs(60 * 60),
            interval: Duration::from_secs(60 * 60),
        }
    }
}

impl CatalogTable {
    pub fn new(catalog: &str, namespace: &str, name: &str) -> CatalogTable {
        CatalogTable {
            catalog: catalog.to_owned(),
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ExternalTable {
    pub fn new(
        topic: &str,
        table: CatalogTable,
        uuid: Uuid,
        location: &str,
        schedule: TierSchedule,
        generation: i32,
    ) -> Result<ExternalTable, Error> {
        let named = [topic, &table.catalog, &table.namespace, &table.name]
            .iter()
            .all(|part| !part.is_empty());
        if !named || !location.starts_with("s3://") {
            return Err(Error::ExternalTable(topic.to_owned()));
        }

        Ok(ExternalTable {
            topic: topic.to_owned(),
            table,
            uuid,
            location: location.trim_end_matches('/').to_owned(),
            schedule,
            generation,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn table(&self) -> &CatalogTable {
        &self.table
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn schedule(&self) -> TierSchedule {
        self.schedule
    }

    pub fn generation(&self) -> i32 {
        self.generation
    }
}

pub fn locations_overlap(first: &str, second: &str) -> bool {
    let (first, second) = (first.trim_end_matches('/'), second.trim_end_matches('/'));
    let under = |outer: &str, inner: &str| {
        inner
            .strip_prefix(outer)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    };

    under(first, second) || under(second, first)
}

pub fn locations_holding(path: &str) -> Vec<&str> {
    let scheme = "s3://";
    if !path.starts_with(scheme) {
        return Vec::new();
    }

    path.match_indices('/')
        .map(|(index, _)| &path[..index])
        .filter(|location| location.len() > scheme.len())
        .collect()
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn external_tables_need_names_and_an_s3_location() {
        let uuid = Uuid::new_v4();
        let schedule = TierSchedule::default();
        let table = |catalog, location| {
            ExternalTable::new(
                "orders",
                CatalogTable::new(catalog, "kafka", "orders"),
                uuid,
                location,
                schedule,
                1,
            )
        };

        assert_eq!(
            table("s3tables", "s3://b/t/").unwrap().location(),
            "s3://b/t"
        );
        assert_eq!(
            TierSchedule::given(None, Some(Duration::from_secs(60))),
            TierSchedule::new(Duration::from_secs(3600), Duration::from_secs(60))
        );
        assert_eq!(
            table("", "s3://b/t"),
            Err(Error::ExternalTable("orders".to_owned()))
        );
        assert_eq!(
            table("s3tables", "/tmp/t"),
            Err(Error::ExternalTable("orders".to_owned()))
        );
    }

    #[test]
    fn a_path_is_held_by_each_location_it_falls_under() {
        assert_eq!(
            locations_holding("s3://b/t/data/0.parquet"),
            ["s3://b", "s3://b/t", "s3://b/t/data"]
        );
        assert!(locations_holding("s3://b").is_empty());
        assert!(locations_overlap("s3://b/wh/", "s3://b/wh/t"));
        assert!(locations_overlap("s3://b/wh/t", "s3://b/wh"));
        assert!(locations_overlap("s3://b/t", "s3://b/t/"));
        assert!(!locations_overlap("s3://b/t", "s3://b/tx"));
        assert!(locations_holding("/tmp/t/0.parquet").is_empty());
    }

    #[test]
    fn tier_schedules_need_positive_durations() {
        let hour = Duration::from_secs(3600);

        assert!(TierSchedule::new(hour, hour).is_ok());
        assert_eq!(
            TierSchedule::new(Duration::ZERO, hour),
            Err(Error::TierSchedule)
        );
        assert_eq!(
            TierSchedule::new(hour, Duration::ZERO),
            Err(Error::TierSchedule)
        );
    }
}
