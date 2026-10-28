use mink_core::{
    BuiltFile, DaySketches, File, IndexedFile, Offsets, Record, TimePartition, TimeSpec,
};

use crate::Error;
use crate::file::{build_file, index_file};
use crate::schema::FileSchema;

#[derive(Default)]
pub struct NewFiles {
    files: Vec<BuiltFile>,
    sketches: DaySketches,
}

impl NewFiles {
    pub fn add_range(
        &mut self,
        offsets: &Offsets,
        records: &[Record],
        spec: TimeSpec,
        schema: &FileSchema,
    ) -> Result<(), Error> {
        for file in bucket_files(offsets, records, spec, schema)? {
            let (built, sketches) = index_file(&file, records, spec.granularity(), schema)?;

            self.files.push(built);
            self.sketches.merge(sketches)?;
        }

        Ok(())
    }

    pub fn files(&self) -> &[BuiltFile] {
        &self.files
    }

    pub fn sketches(&self) -> &DaySketches {
        &self.sketches
    }

    pub fn records(&self) -> u64 {
        self.files
            .iter()
            .map(|file| file.indexed().stats().records())
            .sum()
    }

    pub fn bytes(&self) -> u64 {
        self.files
            .iter()
            .map(|file| file.indexed().stats().bytes())
            .sum()
    }
}

pub fn tail_files(
    offsets: &Offsets,
    records: &[Record],
    spec: TimeSpec,
    schema: &FileSchema,
) -> Result<Vec<IndexedFile>, Error> {
    let mut files = Vec::new();

    for file in bucket_files(offsets, records, spec, schema)? {
        let (_, stats) = build_file(&file, records, spec.granularity(), schema)?;

        files.push(IndexedFile::new(file, stats));
    }

    Ok(files)
}

fn bucket_files(
    offsets: &Offsets,
    records: &[Record],
    spec: TimeSpec,
    schema: &FileSchema,
) -> Result<Vec<File>, Error> {
    spec.granularity()
        .buckets(records)
        .into_iter()
        .map(|bucket| {
            let time = TimePartition::new(spec.id(), bucket);

            Ok(File::v1(
                offsets.partition().clone(),
                offsets.start_offset(),
                offsets.end_offset(),
                time,
                schema.id(),
            )?)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use mink_core::{Granularity, Partition, TimeSpecs};

    use super::*;
    use crate::testing::hour_records;

    fn hourly_spec() -> TimeSpec {
        TimeSpecs::new(vec![Granularity::Hour]).unwrap().current()
    }

    fn offsets(start: i64, end: i64) -> Offsets {
        Offsets::new(Partition::new("orders", 0).unwrap(), start, end).unwrap()
    }

    #[test]
    fn a_range_becomes_one_file_per_bucket_with_its_offsets() {
        let records = hour_records(0..10, |offset| if offset == 6 { 4 } else { 5 });
        let mut new_files = NewFiles::default();

        new_files
            .add_range(
                &offsets(0, 12),
                &records,
                hourly_spec(),
                &FileSchema::Binary,
            )
            .unwrap();
        let tail = tail_files(
            &offsets(0, 12),
            &records,
            hourly_spec(),
            &FileSchema::Binary,
        )
        .unwrap();

        let files: Vec<_> = new_files
            .files()
            .iter()
            .map(|built| {
                let file = built.indexed().file();

                (
                    file.start_offset(),
                    file.end_offset(),
                    file.time().bucket(),
                    built.indexed().stats().records(),
                )
            })
            .collect();
        let tail: Vec<_> = tail
            .iter()
            .map(|indexed| (indexed.file().time().bucket(), indexed.stats().records()))
            .collect();
        assert_eq!(files, [(0, 12, Some(4), 1), (0, 12, Some(5), 9)]);
        assert_eq!(tail, [(Some(4), 1), (Some(5), 9)]);
        assert!(new_files.sketches().iter().next().is_some());
    }
}
