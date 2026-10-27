use std::collections::BTreeSet;

use crate::Error;
use crate::record::Record;

const MILLIS_PER_HOUR: i64 = 3_600_000;
const MILLIS_PER_DAY: i64 = 86_400_000;
const DAYS_PER_ERA: i64 = 146_097;
const EPOCH_SHIFT: i64 = 719_468;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Granularity {
    Hour,
    Day,
    Month,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeSpec {
    id: i32,
    granularity: Granularity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSpecs {
    granularities: Vec<Granularity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimePartition {
    spec_id: i32,
    bucket: Option<i32>,
}

impl Granularity {
    pub const ALL: [Granularity; 4] = [
        Granularity::Hour,
        Granularity::Day,
        Granularity::Month,
        Granularity::None,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Granularity::Hour => "hour",
            Granularity::Day => "day",
            Granularity::Month => "month",
            Granularity::None => "none",
        }
    }

    pub fn from_name(name: &str) -> Option<Granularity> {
        Granularity::ALL
            .into_iter()
            .find(|granularity| granularity.name() == name)
    }

    pub fn bucket(self, timestamp_ms: i64) -> Option<i32> {
        let bucket = match self {
            Granularity::Hour => timestamp_ms.div_euclid(MILLIS_PER_HOUR),
            Granularity::Day => utc_day(timestamp_ms),
            Granularity::Month => {
                let (year, month) = year_month(utc_day(timestamp_ms));

                (year - 1970) * 12 + month - 1
            }
            Granularity::None => return None,
        };

        Some(saturated(bucket))
    }

    pub fn buckets(self, records: &[Record]) -> BTreeSet<Option<i32>> {
        records
            .iter()
            .map(|record| self.bucket(record.timestamp_ms()))
            .collect()
    }

    pub fn bucket_end_ms(self, bucket: i32) -> Option<i64> {
        let next = i64::from(bucket) + 1;

        match self {
            Granularity::Hour => Some(next * MILLIS_PER_HOUR),
            Granularity::Day => Some(next * MILLIS_PER_DAY),
            Granularity::Month => {
                let (year, month) = (1970 + next.div_euclid(12), next.rem_euclid(12) + 1);

                Some(days_from_civil(year, month) * MILLIS_PER_DAY)
            }
            Granularity::None => None,
        }
    }
}

impl TimeSpec {
    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn granularity(&self) -> Granularity {
        self.granularity
    }
}

impl TimeSpecs {
    pub fn new(granularities: Vec<Granularity>) -> Result<TimeSpecs, Error> {
        if granularities.is_empty() || i32::try_from(granularities.len()).is_err() {
            return Err(Error::TimeSpecs(granularities.len()));
        }

        Ok(TimeSpecs { granularities })
    }

    pub fn current(&self) -> TimeSpec {
        let index = self.granularities.len() - 1;
        let id = i32::try_from(index).expect("spec ids fit in an i32");

        TimeSpec {
            id,
            granularity: self.granularities[index],
        }
    }

    pub fn get(&self, id: i32) -> Option<TimeSpec> {
        let granularity = *self.granularities.get(usize::try_from(id).ok()?)?;

        Some(TimeSpec { id, granularity })
    }

    pub fn bucket_ended(&self, time: TimePartition, before_ms: i64) -> bool {
        self.get(time.spec_id())
            .zip(time.bucket())
            .and_then(|(spec, bucket)| spec.granularity().bucket_end_ms(bucket))
            .is_some_and(|end| end <= before_ms)
    }

    pub fn granularities(&self) -> &[Granularity] {
        &self.granularities
    }
}

impl TimePartition {
    pub fn new(spec_id: i32, bucket: Option<i32>) -> TimePartition {
        TimePartition { spec_id, bucket }
    }

    pub fn spec_id(&self) -> i32 {
        self.spec_id
    }

    pub fn bucket(&self) -> Option<i32> {
        self.bucket
    }
}

pub fn utc_day(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(MILLIS_PER_DAY)
}

fn saturated(value: i64) -> i32 {
    i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX })
}

fn year_month(days: i64) -> (i64, i64) {
    let shifted = days + EPOCH_SHIFT;
    let era = shifted.div_euclid(DAYS_PER_ERA);
    let day_of_era = shifted.rem_euclid(DAYS_PER_ERA);

    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;

    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);

    (year, month)
}

fn days_from_civil(year: i64, month: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);

    let shifted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted_month + 2) / 5;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    era * DAYS_PER_ERA + day_of_era - EPOCH_SHIFT
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn buckets_follow_the_iceberg_transforms() {
        let moment = 1_793_188_800_123;

        assert_eq!(Granularity::Hour.bucket(moment), Some(498_108));
        assert_eq!(Granularity::Day.bucket(moment), Some(20_754));
        assert_eq!(Granularity::Month.bucket(moment), Some(681));
        assert_eq!(Granularity::None.bucket(moment), None);
        assert_eq!(Granularity::Hour.bucket(-1), Some(-1));
        assert_eq!(Granularity::Month.bucket(-1), Some(-1));
        assert_eq!(
            Granularity::Month.bucket_end_ms(681),
            Some(1_793_491_200_000)
        );

        for granularity in Granularity::ALL {
            assert_eq!(
                Granularity::from_name(granularity.name()),
                Some(granularity)
            );
        }
    }

    #[test]
    fn a_bucket_has_ended_once_its_end_is_before_the_bound() {
        let specs = TimeSpecs::new(vec![Granularity::Hour, Granularity::None]).unwrap();
        let hour = TimePartition::new(0, Some(1));

        assert!(specs.bucket_ended(hour, 7_200_000));
        assert!(!specs.bucket_ended(hour, 7_199_999));
        assert!(!specs.bucket_ended(TimePartition::new(1, None), i64::MAX));
        assert!(!specs.bucket_ended(TimePartition::new(2, Some(1)), i64::MAX));
        assert_eq!(
            specs.get(1).map(|spec| spec.granularity()),
            Some(Granularity::None)
        );
    }

    proptest! {
        #[test]
        fn a_bucket_ends_where_the_next_begins(timestamp in -10_000_000_000_000i64..10_000_000_000_000) {
            for granularity in [Granularity::Hour, Granularity::Day, Granularity::Month] {
                let bucket = granularity.bucket(timestamp).unwrap();
                let end = granularity.bucket_end_ms(bucket).unwrap();

                prop_assert!(end > timestamp);
                prop_assert_eq!(granularity.bucket(end), Some(bucket + 1));
                prop_assert_eq!(granularity.bucket(end - 1), Some(bucket));
            }
        }
    }
}
