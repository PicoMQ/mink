use std::time::Duration;

use uuid::Uuid;

use crate::Error;
use crate::location::ExternalLocation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTable {
    topic: String,
    table: CatalogTable,
    uuid: Uuid,
    location: ExternalLocation,
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
        location: ExternalLocation,
        schedule: TierSchedule,
        generation: i32,
    ) -> Result<ExternalTable, Error> {
        let named = [topic, &table.catalog, &table.namespace, &table.name]
            .iter()
            .all(|part| !part.is_empty());
        if !named {
            return Err(Error::ExternalTable(topic.to_owned()));
        }
        if generation < 1 {
            return Err(Error::Generation(generation));
        }

        Ok(ExternalTable {
            topic: topic.to_owned(),
            table,
            uuid,
            location,
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

    pub fn location(&self) -> &ExternalLocation {
        &self.location
    }

    pub fn schedule(&self) -> TierSchedule {
        self.schedule
    }

    pub fn generation(&self) -> i32 {
        self.generation
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn external_tables_need_names() {
        let table = |catalog| {
            ExternalTable::new(
                "orders",
                CatalogTable::new(catalog, "kafka", "orders"),
                Uuid::new_v4(),
                ExternalLocation::parse("s3://b/t").unwrap(),
                TierSchedule::default(),
                1,
            )
        };

        assert!(table("s3tables").is_ok());
        assert_eq!(
            TierSchedule::given(None, Some(Duration::from_secs(60))),
            TierSchedule::new(Duration::from_secs(3600), Duration::from_secs(60))
        );
        assert_eq!(table(""), Err(Error::ExternalTable("orders".to_owned())));
    }

    #[test]
    fn a_generation_is_positive() {
        let table = |generation| {
            ExternalTable::new(
                "orders",
                CatalogTable::new("s3tables", "kafka", "orders"),
                Uuid::new_v4(),
                ExternalLocation::parse("s3://b/t").unwrap(),
                TierSchedule::default(),
                generation,
            )
        };

        assert!(table(1).is_ok());
        assert_eq!(table(0), Err(Error::Generation(0)));
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
