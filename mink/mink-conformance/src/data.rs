use std::fs;
use std::io;
use std::path::PathBuf;

use bytes::Bytes;
use mink_core::{File, Granularity, Partition, Record, Stats, TableLocation, TimePartition};
use mink_store::{parse_stats, stats_json};
use serde_json::{Value, json};

use crate::{WAREHOUSE, fixtures, record_fixtures};

const TOPIC: &str = "conformance";

pub struct DataFixture {
    name: String,
    file: File,
    records: Vec<Record>,
}

impl DataFixture {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn records(&self) -> &[Record] {
        &self.records
    }

    pub fn granularity(&self) -> Granularity {
        Granularity::None
    }

    pub fn path(&self) -> PathBuf {
        fixtures("data").join(format!("{}.parquet", self.name))
    }

    pub fn bytes(&self) -> Bytes {
        Bytes::from(fs::read(self.path()).unwrap())
    }

    pub fn stats(&self) -> Stats {
        let cases: Value =
            serde_json::from_str(&fs::read_to_string(fixtures("data").join("cases.json")).unwrap())
                .unwrap();
        let entry = cases["files"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == self.name.as_str())
            .unwrap_or_else(|| panic!("no data fixture {}", self.name));
        assert_eq!(
            entry["file"],
            TableLocation::data_file(WAREHOUSE, &self.file).as_str(),
            "{}",
            self.name
        );

        parse_stats(&entry["stats"]).unwrap()
    }
}

pub fn data_fixtures() -> Vec<DataFixture> {
    let partition = Partition::new(TOPIC, 0).unwrap();

    record_fixtures()
        .into_iter()
        .filter(|fixture| fixture.corrupt_at().is_none())
        .filter_map(|fixture| {
            let records = fixture.expected().to_vec();
            let (first, last) = (records.first()?, records.last()?);
            let unbucketed = TimePartition::new(0, None);
            let file = File::v1(
                partition.clone(),
                first.offset(),
                last.offset() + 1,
                unbucketed,
                0,
            )
            .unwrap();

            Some(DataFixture {
                name: fixture.name().to_owned(),
                file,
                records,
            })
        })
        .collect()
}

pub fn bless_data(written: &[(&DataFixture, &Bytes, &Stats)]) -> io::Result<()> {
    let directory = fixtures("data");
    fs::create_dir_all(&directory)?;

    let mut files = Vec::new();
    for (fixture, bytes, stats) in written {
        fs::write(fixture.path(), bytes)?;
        files.push(json!({
            "name": fixture.name,
            "file": TableLocation::data_file(WAREHOUSE, &fixture.file),
            "stats": stats_json(stats),
        }));
    }

    let cases = serde_json::to_string_pretty(&json!({ "files": files }))? + "\n";

    fs::write(directory.join("cases.json"), cases)
}
