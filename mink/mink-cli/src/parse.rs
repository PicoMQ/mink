//! Parsers for command-line values: key=value pairs, name lists, column definitions,
//! durations, offset specs and partition specs.

use std::time::{Duration, UNIX_EPOCH};

use anyhow::Context;
use mink_client::proto::OffsetSpec;
use mink_table::{Column, PartitionSpec};
use mink_types::DataType;

pub type Pair = (String, String);

pub fn key_value(text: &str) -> anyhow::Result<Pair> {
    let (key, value) = text
        .split_once('=')
        .with_context(|| format!("expected key=value, got {text:?}"))?;

    Ok((key.trim().to_owned(), value.trim().to_owned()))
}

pub fn names(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

pub fn enum_value<T: serde::de::DeserializeOwned>(what: &str, text: &str) -> anyhow::Result<T> {
    mink_common::json::enum_value(text).with_context(|| format!("invalid {what} {text:?}"))
}

pub fn column(text: &str) -> anyhow::Result<Column> {
    let text = text.trim();
    let (name, data_type) = text
        .split_once(char::is_whitespace)
        .with_context(|| format!("expected `name TYPE`, got {text:?}"))?;
    let data_type: DataType = data_type
        .trim()
        .parse()
        .with_context(|| format!("column {name}: bad type {:?}", data_type.trim()))?;

    Ok(Column::new(name.trim(), data_type)?)
}

pub fn duration(what: &str, text: &str) -> anyhow::Result<Duration> {
    humantime::parse_duration(text).with_context(|| format!("invalid {what} {text:?}"))
}

pub fn offset_spec(text: &str) -> anyhow::Result<OffsetSpec> {
    Ok(match text {
        "earliest" => OffsetSpec::Earliest,
        "latest" => OffsetSpec::Latest,
        text => {
            if let Ok(ms) = text.parse::<i64>() {
                return Ok(OffsetSpec::Timestamp { timestamp: ms });
            }

            let time = humantime::parse_rfc3339_weak(text).with_context(|| {
                format!("expected earliest, latest, millis or RFC 3339, got {text:?}")
            })?;
            let ms = time
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);

            OffsetSpec::Timestamp { timestamp: ms }
        }
    })
}

pub fn partition_spec(spec: Vec<Pair>) -> anyhow::Result<PartitionSpec> {
    let mut entries = Vec::with_capacity(spec.len());
    for (key, value) in spec {
        let value = value
            .parse()
            .with_context(|| format!("partition value {value:?} for {key}"))?;
        entries.push((key, value));
    }

    Ok(PartitionSpec::new(entries)?)
}
