use crate::file::{AssemblyHash, File, FileVersion};
use crate::granularity::TimePartition;
use crate::offsets::OffsetRange;
use crate::topic::Partition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableLocation {
    path: String,
}

impl TableLocation {
    pub fn new(warehouse: &str, topic: &str) -> TableLocation {
        TableLocation {
            path: format!("{}/{topic}", root(warehouse)),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn metadata_directory(&self) -> String {
        format!("{}/metadata/", self.path)
    }

    pub fn metadata_file(&self, key: &str) -> String {
        format!("{}/metadata/v{key}.metadata.json", self.path)
    }

    pub fn manifest_list(&self, key: &str) -> String {
        format!("{}/metadata/snap-{key}.avro", self.path)
    }

    pub fn manifest(&self, key: &str, index: usize) -> String {
        format!("{}/metadata/{key}-m{index}.avro", self.path)
    }

    pub fn statistics(&self, key: &str) -> String {
        format!("{}/metadata/{key}-stats.puffin", self.path)
    }

    pub fn parse_statistics(&self, path: &str) -> Option<(i64, i64)> {
        let key = path
            .strip_prefix(&format!("{}/metadata/", self.path))?
            .strip_suffix("-stats.puffin")?;
        let (id, epoch) = key.split_once("-e")?;

        Some((id.parse().ok()?, epoch.parse().ok()?))
    }

    pub fn data_file(warehouse: &str, file: &File) -> String {
        let location = TableLocation::new(warehouse, file.partition().topic());

        format!(
            "{}/data/{}",
            location.path,
            TableLocation::data_file_name(file)
        )
    }

    pub fn data_file_name(file: &File) -> String {
        let time = file.time();
        let bucket = time
            .bucket()
            .map_or_else(String::new, |bucket| format!("_{bucket}"));
        let assembly = file.assembly().map_or_else(String::new, |assembly| {
            format!(".a{:016x}", assembly.value())
        });

        format!(
            "p={}/{}-{}.t{}{bucket}.s{}{assembly}.v{}.parquet",
            file.partition().index(),
            file.start_offset(),
            file.end_offset(),
            time.spec_id(),
            file.schema_id(),
            file.version().number()
        )
    }

    pub fn parse_data_file(warehouse: &str, path: &str) -> Option<File> {
        let rest = path.strip_prefix(root(warehouse))?.strip_prefix('/')?;
        let [topic, "data", partition, name] = rest.split('/').collect::<Vec<_>>()[..] else {
            return None;
        };

        let index: i32 = partition.strip_prefix("p=")?.parse().ok()?;
        let segments: Vec<&str> = name.strip_suffix(".parquet")?.split('.').collect();
        let (range, time, schema_id, assembly, version) = match segments[..] {
            [range, time, schema_id, version] => (range, time, schema_id, None, version),
            [range, time, schema_id, assembly, version] => {
                (range, time, schema_id, Some(assembly), version)
            }
            _ => return None,
        };

        let (start, end) = range.split_once('-')?;
        let range = OffsetRange::new(start.parse().ok()?, end.parse().ok()?).ok()?;
        let time = time_partition(time.strip_prefix('t')?)?;
        let schema_id: i32 = schema_id.strip_prefix('s')?.parse().ok()?;
        let assembly = match assembly {
            Some(assembly) => Some(AssemblyHash::new(
                u64::from_str_radix(assembly.strip_prefix('a')?, 16).ok()?,
            )),
            None => None,
        };
        let version = FileVersion::from_number(version.strip_prefix('v')?.parse().ok()?)?;

        let partition = Partition::new(topic, index).ok()?;
        let file = File::identified(partition, range, time, schema_id, assembly, version).ok()?;

        (TableLocation::data_file(warehouse, &file) == path).then_some(file)
    }

    pub fn parse_data_file_name(warehouse: &str, topic: &str, name: &str) -> Option<File> {
        let location = TableLocation::new(warehouse, topic);

        TableLocation::parse_data_file(warehouse, &format!("{}/data/{name}", location.path))
    }

    pub fn parse_metadata<'a>(warehouse: &str, path: &'a str) -> Option<&'a str> {
        let (topic, rest) = path
            .strip_prefix(root(warehouse))?
            .strip_prefix('/')?
            .split_once('/')?;
        let name = rest.strip_prefix("metadata/")?;

        (!topic.is_empty() && !name.is_empty() && !name.contains('/')).then_some(topic)
    }
}

fn root(warehouse: &str) -> &str {
    warehouse.trim_end_matches('/')
}

fn time_partition(text: &str) -> Option<TimePartition> {
    let (spec_id, bucket) = match text.split_once('_') {
        Some((spec_id, bucket)) => (spec_id, Some(bucket.parse().ok()?)),
        None => (text, None),
    };

    Some(TimePartition::new(spec_id.parse().ok()?, bucket))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use proptest::prelude::*;
    use serde_json::Value;

    use super::*;

    fn cases() -> Value {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../conformance/fixtures/path/cases.json");

        serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
    }

    fn file(case: &Value) -> File {
        let int = |key: &str| i32::try_from(case[key].as_i64().unwrap()).unwrap();
        let partition = Partition::new(case["topic"].as_str().unwrap(), int("partition")).unwrap();
        let bucket = case["bucket"]
            .as_i64()
            .map(|bucket| i32::try_from(bucket).unwrap());
        let time = TimePartition::new(int("spec_id"), bucket);
        let version = FileVersion::from_number(case["version"].as_u64().unwrap() as u16).unwrap();

        match case["parts"].as_array() {
            Some(parts) => {
                let parts: Vec<OffsetRange> = parts
                    .iter()
                    .map(|part| {
                        OffsetRange::new(part[0].as_i64().unwrap(), part[1].as_i64().unwrap())
                            .unwrap()
                    })
                    .collect();

                File::assembled(partition, time, int("schema_id"), version, &parts).unwrap()
            }
            None => File::new(
                partition,
                case["start_offset"].as_i64().unwrap(),
                case["end_offset"].as_i64().unwrap(),
                time,
                int("schema_id"),
                version,
            )
            .unwrap(),
        }
    }

    #[test]
    fn files_render_and_parse_as_the_fixtures_say() {
        let cases = cases();
        let warehouse = cases["warehouse"].as_str().unwrap();
        let files = cases["files"].as_array().unwrap();
        assert!(!files.is_empty());

        for case in files {
            let name = case["name"].as_str().unwrap();
            let path = case["path"].as_str().unwrap();

            assert_eq!(
                TableLocation::data_file(warehouse, &file(case)),
                path,
                "render: {name}"
            );
            assert_eq!(
                TableLocation::parse_data_file(warehouse, path),
                Some(file(case)),
                "parse: {name}"
            );

            let file = file(case);
            let short = TableLocation::data_file_name(&file);
            let topic = file.partition().topic();
            assert!(path.ends_with(&format!("/data/{short}")), "name: {name}");
            assert_eq!(
                TableLocation::parse_data_file_name(warehouse, topic, &short),
                Some(file),
                "parse name: {name}"
            );
        }
    }

    #[test]
    fn rejected_paths_in_the_fixtures_do_not_parse() {
        let cases = cases();
        let warehouse = cases["warehouse"].as_str().unwrap();
        let rejected = cases["rejected"].as_array().unwrap();
        assert!(!rejected.is_empty());

        for case in rejected {
            let name = case["name"].as_str().unwrap();
            let path = case["path"].as_str().unwrap();

            assert_eq!(
                TableLocation::parse_data_file(warehouse, path),
                None,
                "{name}"
            );
        }
    }

    #[test]
    fn statistics_paths_name_their_snapshot() {
        let location = TableLocation::new("s3://mink/warehouse", "orders");

        let path = location.statistics("12-e3");

        assert_eq!(location.parse_statistics(&path), Some((12, 3)));
        assert_eq!(
            location.parse_statistics("s3://mink/warehouse/orders/metadata/x.puffin"),
            None
        );
    }

    #[test]
    fn metadata_paths_name_their_topic() {
        let location = TableLocation::new("s3://mink/kafka/", "orders");

        let path = location.metadata_file("3-e1");

        assert_eq!(path, "s3://mink/kafka/orders/metadata/v3-e1.metadata.json");
        assert_eq!(
            location.metadata_directory(),
            "s3://mink/kafka/orders/metadata/"
        );
        assert_eq!(
            TableLocation::parse_metadata("s3://mink/kafka", &path),
            Some("orders")
        );
        assert_eq!(
            TableLocation::parse_metadata(
                "s3://mink/kafka",
                "s3://mink/kafka/orders/data/p=0/0-1.t0.s0.v1.parquet"
            ),
            None
        );
        assert_eq!(
            TableLocation::parse_metadata("s3://mink/kafka", "s3://mink/kafka/orders/metadata/"),
            None
        );
    }

    #[test]
    fn trailing_slash_on_the_warehouse_renders_the_same_path() {
        let time = TimePartition::new(0, Some(498_108));
        let file = File::v1(Partition::new("orders", 0).unwrap(), 0, 10, time, 0).unwrap();

        assert_eq!(
            TableLocation::data_file("s3://mink/kafka/", &file),
            TableLocation::data_file("s3://mink/kafka", &file)
        );
    }

    proptest! {
        #[test]
        fn any_valid_file_round_trips_through_its_path(
            topic in "[a-zA-Z0-9._-]{1,249}",
            index in 0..i32::MAX,
            start_offset in 0..i64::MAX - 1,
            length in 1..i64::MAX,
            schema_id in 0..i32::MAX,
            spec_id in 0..i32::MAX,
            bucket in proptest::option::of(any::<i32>()),
        ) {
            let Ok(partition) = Partition::new(topic, index) else { return Ok(()) };
            let end_offset = start_offset.saturating_add(length);
            prop_assume!(start_offset < end_offset);
            let time = TimePartition::new(spec_id, bucket);
            let file = File::v1(partition, start_offset, end_offset, time, schema_id).unwrap();

            let path = TableLocation::data_file("s3://mink/kafka", &file);

            prop_assert_eq!(TableLocation::parse_data_file("s3://mink/kafka", &path), Some(file));
        }
    }
}
