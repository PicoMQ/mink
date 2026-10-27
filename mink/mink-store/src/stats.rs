use std::collections::BTreeMap;

use mink_core::Stats;
use serde_json::{Map, Value, json};

use crate::Error;

pub(crate) const FIRST_TIMESTAMP_MS: &str = "first_timestamp_ms";

pub fn stats_json(stats: &Stats) -> Value {
    let bounds = |values: &BTreeMap<i32, Vec<u8>>| -> Map<String, Value> {
        values
            .iter()
            .map(|(field, value)| (field.to_string(), hex::encode(value).into()))
            .collect()
    };
    let counts = |values: &BTreeMap<i32, u64>| -> Map<String, Value> {
        values
            .iter()
            .map(|(field, count)| (field.to_string(), (*count).into()))
            .collect()
    };

    json!({
        "records": stats.records(),
        "bytes": stats.bytes(),
        FIRST_TIMESTAMP_MS: stats.first_timestamp_ms(),
        "lower": bounds(stats.lower()),
        "upper": bounds(stats.upper()),
        "nulls": counts(stats.nulls()),
        "nans": counts(stats.nans()),
    })
}

pub fn parse_stats(value: &Value) -> Result<Stats, Error> {
    let malformed = || Error::Corrupt(format!("stats {value}"));
    let fields = |key: &str| {
        value[key]
            .as_object()
            .ok_or_else(malformed)?
            .iter()
            .map(|(field, value)| Ok((field.parse::<i32>().map_err(|_| malformed())?, value)))
            .collect::<Result<BTreeMap<_, _>, Error>>()
    };
    let bytes = |value: &Value| {
        value
            .as_str()
            .and_then(|text| hex::decode(text).ok())
            .ok_or_else(malformed)
    };
    let count = |value: &Value| value.as_u64().ok_or_else(malformed);
    let first_timestamp_ms = value[FIRST_TIMESTAMP_MS].as_i64().ok_or_else(malformed)?;

    let mut stats = Stats::new(
        count(&value["records"])?,
        count(&value["bytes"])?,
        first_timestamp_ms,
    );
    let upper = fields("upper")?;

    for (field, lower) in fields("lower")? {
        let upper = upper.get(&field).ok_or_else(malformed)?;

        stats = stats.with_bounds(field, bytes(lower)?, bytes(upper)?);
    }

    for (field, nulls) in fields("nulls")? {
        stats = stats.with_nulls(field, count(nulls)?);
    }

    for (field, nans) in fields("nans")? {
        stats = stats.with_nans(field, count(nans)?);
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_round_trip_through_their_json_form() {
        let stats = Stats::new(3, 120, 1_700_000_000_000)
            .with_bounds(2, vec![1, 0], vec![9, 0])
            .with_nulls(4, 2)
            .with_nans(5, 0);

        let json = stats_json(&stats);

        assert_eq!(json["lower"]["2"], "0100");
        assert_eq!(json["nulls"]["4"], 2);
        assert_eq!(json["first_timestamp_ms"], 1_700_000_000_000_i64);
        assert_eq!(parse_stats(&json).unwrap(), stats);
        assert!(matches!(
            parse_stats(&json!({"records": 1})),
            Err(Error::Corrupt(_))
        ));
    }
}
