//! Plans which time partitions to create ahead and drop behind for an auto-partitioned table.

use chrono::{DateTime, Datelike, Months, NaiveDateTime, TimeDelta};
use chrono_tz::Tz;
use mink_table::{AutoPartition, Descriptor, Id, PartitionName, TimeUnit};

use crate::Error;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Plan {
    pub create: Vec<PartitionName>,
    pub drop: Vec<PartitionName>,
}

pub fn plan<'a>(
    table_id: Id,
    descriptor: &Descriptor,
    existing: impl IntoIterator<Item = &'a PartitionName>,
    now_ms: i64,
    forced: bool,
) -> Result<Plan, Error> {
    let Some(auto) = &descriptor.options().auto_partition else {
        return Ok(Plan::default());
    };
    let keys = descriptor.partition_keys();
    let key_index = auto.key_index(keys);
    let zone: Tz = auto
        .time_zone
        .parse()
        .map_err(|_| mink_table::Error::AutoPartitionTimeZone(auto.time_zone.clone()))?;
    let mut at = DateTime::from_timestamp_millis(now_ms)
        .expect("clock within range")
        .with_timezone(&zone)
        .naive_local();
    if !forced {
        at -= TimeDelta::minutes(day_delay_minutes(table_id, auto));
    }

    let existing: Vec<&PartitionName> = existing.into_iter().collect();
    let mut plan = Plan::default();

    if let Some(retain) = auto.num_retention {
        let cutoff = partition_time(at, -(retain as i32), auto.time_unit);
        for name in &existing {
            let value = name.as_str().split('$').nth(key_index).unwrap_or("");
            if value < cutoff.as_str() {
                plan.drop.push((*name).clone());
            }
        }
    }

    for offset in 0..auto.precreate(keys) as i32 {
        let value = partition_time(at, offset, auto.time_unit);
        let name: PartitionName = value.parse()?;
        if !existing.iter().any(|e| **e == name) {
            plan.create.push(name);
        }
    }

    Ok(plan)
}

fn day_delay_minutes(id: Id, auto: &AutoPartition) -> i64 {
    match auto.time_unit {
        TimeUnit::Day => (id.0 as i64 * 7919) % (23 * 60),
        _ => 0,
    }
}

pub fn partition_time(at: NaiveDateTime, offset: i32, unit: TimeUnit) -> String {
    let shifted = match unit {
        TimeUnit::Hour => at + TimeDelta::hours(i64::from(offset)),
        TimeUnit::Day => at + TimeDelta::days(i64::from(offset)),
        TimeUnit::Month => add_months(at, offset),
        TimeUnit::Quarter => add_months(at, offset * 3),
        TimeUnit::Year => add_months(at, offset * 12),
    };

    match unit {
        TimeUnit::Hour => shifted.format("%Y%m%d%H").to_string(),
        TimeUnit::Day => shifted.format("%Y%m%d").to_string(),
        TimeUnit::Month => shifted.format("%Y%m").to_string(),
        TimeUnit::Quarter => format!("{}{}", shifted.year(), (shifted.month0() / 3) + 1),
        TimeUnit::Year => shifted.format("%Y").to_string(),
    }
}

fn add_months(at: NaiveDateTime, months: i32) -> NaiveDateTime {
    let shifted = if months >= 0 {
        at.checked_add_months(Months::new(months as u32))
    } else {
        at.checked_sub_months(Months::new(months.unsigned_abs()))
    };
    shifted.expect("date within range")
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use mink_table::{Column, Options, PrimaryKey, Schema};
    use mink_types::DataType;

    use super::*;

    fn at(y: i32, m: u32, d: u32, h: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, 0, 0)
            .unwrap()
    }

    #[test]
    fn formats_every_unit_like_java() {
        let t = at(2024, 1, 31, 13);
        assert_eq!(partition_time(t, 0, TimeUnit::Hour), "2024013113");
        assert_eq!(partition_time(t, 0, TimeUnit::Day), "20240131");
        assert_eq!(partition_time(t, 0, TimeUnit::Month), "202401");
        assert_eq!(partition_time(t, 0, TimeUnit::Quarter), "20241");
        assert_eq!(partition_time(t, 0, TimeUnit::Year), "2024");
        assert_eq!(partition_time(t, 1, TimeUnit::Month), "202402");
        assert_eq!(partition_time(t, 1, TimeUnit::Day), "20240201");
        assert_eq!(partition_time(t, -1, TimeUnit::Quarter), "20234");
        assert_eq!(partition_time(t, 11, TimeUnit::Hour), "2024020100");
    }

    fn table(auto: AutoPartition, keys: &[&str]) -> Descriptor {
        let mut schema = Schema::builder()
            .column(Column::new("id", DataType::big_int().with_nullable(false)).unwrap());
        for key in keys {
            schema =
                schema.column(Column::new(*key, DataType::string().with_nullable(false)).unwrap());
        }
        let mut pk: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        pk.insert(0, "id".to_owned());
        let schema = schema
            .primary_key(PrimaryKey::new(pk).unwrap())
            .build()
            .unwrap();
        Descriptor::builder(schema)
            .partitioned_by(keys.iter().copied())
            .options(Options {
                auto_partition: Some(auto),
                ..Options::default()
            })
            .build()
            .unwrap()
    }

    const NOW_MS: i64 = 1_718_452_800_000;

    #[test]
    fn creates_ahead_and_drops_behind() {
        let descriptor = table(
            AutoPartition {
                time_unit: TimeUnit::Day,
                num_precreate: Some(2),
                num_retention: Some(1),
                ..AutoPartition::default()
            },
            &["dt"],
        );
        let existing: Vec<PartitionName> = ["20240612", "20240614", "20240615"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        let plan = plan(Id(1), &descriptor, &existing, NOW_MS, true).unwrap();
        assert_eq!(plan.create, vec!["20240616".parse().unwrap()]);
        assert_eq!(plan.drop, vec!["20240612".parse().unwrap()]);
    }

    #[test]
    fn unforced_day_tables_lag_by_the_table_delay() {
        let descriptor = table(
            AutoPartition {
                time_unit: TimeUnit::Day,
                num_precreate: Some(1),
                num_retention: None,
                ..AutoPartition::default()
            },
            &["dt"],
        );
        let plan = plan(Id(1), &descriptor, &[], NOW_MS, false).unwrap();
        assert_eq!(plan.create, vec!["20240614".parse().unwrap()]);
        let forced = super::plan(Id(1), &descriptor, &[], NOW_MS, true).unwrap();
        assert_eq!(forced.create, vec!["20240615".parse().unwrap()]);
    }

    #[test]
    fn time_zone_shifts_the_day() {
        let descriptor = table(
            AutoPartition {
                time_unit: TimeUnit::Day,
                time_zone: "Pacific/Auckland".to_owned(),
                num_precreate: Some(1),
                num_retention: None,
                ..AutoPartition::default()
            },
            &["dt"],
        );
        let plan = plan(Id(1), &descriptor, &[], NOW_MS, true).unwrap();
        assert_eq!(plan.create, vec!["20240616".parse().unwrap()]);
    }

    #[test]
    fn several_keys_only_drop_by_the_named_key() {
        let descriptor = table(
            AutoPartition {
                key: Some("dt".to_owned()),
                time_unit: TimeUnit::Month,
                num_precreate: None,
                num_retention: Some(2),
                ..AutoPartition::default()
            },
            &["region", "dt"],
        );
        let existing: Vec<PartitionName> = ["eu$202403", "us$202404", "us$202406"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        let plan = plan(Id(1), &descriptor, &existing, NOW_MS, true).unwrap();
        assert!(plan.create.is_empty());
        assert_eq!(plan.drop, vec!["eu$202403".parse().unwrap()]);
    }
}
