use std::collections::{BTreeMap, HashMap};

use crate::Error;

const PARTITION_PREFIX: &str = "mink.partition.";
const EXTERNAL_PARTITION_PREFIX: &str = "mink.external.partition.";
const START_OFFSET_SUFFIX: &str = ".start-offset";
const END_OFFSET_SUFFIX: &str = ".end-offset";

pub(super) const EXTERNAL_SNAPSHOT_ID: &str = "mink.external.snapshot-id";

pub fn start_offset_key(partition: i32) -> String {
    format!("{PARTITION_PREFIX}{partition}{START_OFFSET_SUFFIX}")
}

pub fn end_offset_key(partition: i32) -> String {
    format!("{PARTITION_PREFIX}{partition}{END_OFFSET_SUFFIX}")
}

pub(super) fn external_end_offset_key(partition: i32) -> String {
    format!("{EXTERNAL_PARTITION_PREFIX}{partition}{END_OFFSET_SUFFIX}")
}

pub fn end_offsets(properties: &HashMap<String, String>) -> Result<BTreeMap<i32, i64>, Error> {
    offsets(properties, PARTITION_PREFIX).map_err(|(key, value)| Error::table_property(key, value))
}

pub(super) fn external_end_offsets(
    summary: &HashMap<String, String>,
) -> Result<BTreeMap<i32, i64>, Error> {
    offsets(summary, EXTERNAL_PARTITION_PREFIX)
        .map_err(|(key, value)| Error::Corrupt(format!("summary {key} is {value:?}")))
}

fn offsets<'a>(
    entries: &'a HashMap<String, String>,
    prefix: &str,
) -> Result<BTreeMap<i32, i64>, (&'a str, &'a str)> {
    let mut offsets = BTreeMap::new();

    for (key, value) in entries {
        let Some(partition) = key
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(END_OFFSET_SUFFIX))
        else {
            continue;
        };

        let partition = partition
            .parse()
            .map_err(|_| (key.as_str(), value.as_str()))?;
        let offset = value.parse().map_err(|_| (key.as_str(), value.as_str()))?;
        offsets.insert(partition, offset);
    }

    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_offsets_are_read_back_from_their_keys_and_refuse_bad_values() {
        let properties = HashMap::from([
            (end_offset_key(0), "6".to_owned()),
            (start_offset_key(0), "2".to_owned()),
            (external_end_offset_key(1), "9".to_owned()),
            ("write.format.default".to_owned(), "parquet".to_owned()),
        ]);
        let bad = HashMap::from([(end_offset_key(0), "six".to_owned())]);

        assert_eq!(end_offsets(&properties).unwrap(), BTreeMap::from([(0, 6)]));
        assert_eq!(
            external_end_offsets(&properties).unwrap(),
            BTreeMap::from([(1, 9)])
        );
        assert!(matches!(
            end_offsets(&bad),
            Err(Error::TableProperty { .. })
        ));
        assert!(matches!(external_end_offsets(&bad), Ok(offsets) if offsets.is_empty()));
    }
}
