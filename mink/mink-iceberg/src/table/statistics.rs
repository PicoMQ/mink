use std::collections::HashMap;

use iceberg::compression::CompressionCodec;
use iceberg::io::FileIO;
use iceberg::puffin::{APACHE_DATASKETCHES_THETA_V1, Blob, CREATED_BY_PROPERTY, PuffinWriter};
use iceberg::spec::{BlobMetadata, StatisticsFile};
use mink_core::{FieldSketches, Object, ObjectKind, estimate};

use super::Table;
use super::commit::{Commit, CommitOutput};
use crate::Error;

const MAGIC_LENGTH: usize = 4;
const CREATED_BY: &str = "mink";

impl Table {
    pub(super) async fn statistics(
        &self,
        io: &FileIO,
        commit: &Commit<'_>,
        sketches: Option<&FieldSketches>,
        output: &mut CommitOutput,
    ) -> Result<Option<StatisticsFile>, Error> {
        let kept = self.statistics.clone().filter(|statistics| {
            self.snapshots
                .iter()
                .any(|snapshot| snapshot.snapshot_id() == statistics.snapshot_id)
        });
        let statistics = match sketches {
            Some(sketches) => Some(self.statistics_file(io, commit, sketches, output).await?),
            None => kept,
        };

        if let Some(previous) = &self.statistics
            && statistics.as_ref() != Some(previous)
        {
            output.released.push(Object::new(
                &previous.statistics_path,
                ObjectKind::Statistics,
            ));
        }

        Ok(statistics)
    }

    async fn statistics_file(
        &self,
        io: &FileIO,
        commit: &Commit<'_>,
        sketches: &FieldSketches,
        output: &mut CommitOutput,
    ) -> Result<StatisticsFile, Error> {
        let location = self.location().statistics(&commit.key);
        let properties = HashMap::from([(CREATED_BY_PROPERTY.to_owned(), CREATED_BY.to_owned())]);
        let mut writer = PuffinWriter::new(&io.new_output(&location)?, properties, false).await?;

        let mut blob_metadata = Vec::new();
        let mut blob_bytes = 0;
        for (field, sketch) in sketches.iter() {
            let properties =
                HashMap::from([("ndv".to_owned(), estimate(sketch)?.round().to_string())]);
            let blob = Blob::builder()
                .r#type(APACHE_DATASKETCHES_THETA_V1.to_owned())
                .fields(vec![field])
                .snapshot_id(commit.snapshot_id)
                .sequence_number(commit.sequence_number)
                .data(sketch.to_vec())
                .properties(properties.clone())
                .build();

            writer.add(blob, CompressionCodec::None).await?;
            blob_bytes += sketch.len();
            blob_metadata.push(BlobMetadata {
                r#type: APACHE_DATASKETCHES_THETA_V1.to_owned(),
                snapshot_id: commit.snapshot_id,
                sequence_number: commit.sequence_number,
                fields: vec![field],
                properties,
            });
        }

        writer.close().await?;

        let bytes = io.new_input(&location)?.read().await?;
        let footer = bytes.len() - MAGIC_LENGTH - blob_bytes;

        output.objects.push((
            Object::new(&location, ObjectKind::Statistics),
            bytes.clone(),
        ));
        Ok(StatisticsFile {
            snapshot_id: commit.snapshot_id,
            statistics_path: location,
            file_size_in_bytes: bytes.len() as i64,
            file_footer_size_in_bytes: footer as i64,
            key_metadata: None,
            blob_metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use iceberg::io::FileIO;
    use iceberg::spec::TableMetadata;
    use mink_conformance::WAREHOUSE;

    use super::super::FormatVersion;
    use super::super::testing::{file, snapshot, specs, topic};
    use super::*;

    #[tokio::test]
    async fn the_statistics_file_is_valid_puffin() {
        let specs = specs();
        let table = Table::new(FormatVersion::V2, WAREHOUSE, &topic());
        let mut sketch = mink_core::Sketch::new();
        for value in 0u64..1000 {
            sketch.update(&value.to_le_bytes());
        }
        let mut days = mink_core::DaySketches::default();
        days.insert(0, 2, sketch.serialize()).unwrap();
        let sketches = days.by_field().unwrap();

        let first = table
            .apply(
                &snapshot(1, 1, [(0, 10), (0, 0)]),
                &[file(0, 0, 10)],
                &[],
                Some(&sketches),
                &specs,
                &[],
            )
            .await
            .unwrap();
        let (object, bytes) = first
            .objects()
            .find(|(object, _)| object.kind() == ObjectKind::Statistics)
            .unwrap()
            .clone();
        let io = FileIO::new_with_memory();
        io.new_output(object.path())
            .unwrap()
            .write(bytes.clone())
            .await
            .unwrap();
        let reader = iceberg::puffin::PuffinReader::new(io.new_input(object.path()).unwrap());
        let metadata = reader.file_metadata().await.unwrap().clone();
        let blob = reader.blob(&metadata.blobs()[0]).await.unwrap();
        let rendered: TableMetadata = serde_json::from_slice(&first.metadata().1).unwrap();
        let listed = rendered.statistics_iter().next().unwrap();

        assert_eq!(blob.blob_type(), APACHE_DATASKETCHES_THETA_V1);
        assert_eq!(blob.fields(), [2]);
        assert_eq!(blob.properties()["ndv"], "1000");
        assert_eq!(blob.data(), sketches.get(2).unwrap());
        assert_eq!(listed.file_size_in_bytes, bytes.len() as i64);
        assert_eq!(listed.statistics_path, object.path());

        let second = first
            .into_table()
            .apply(
                &snapshot(2, 1, [(0, 20), (0, 0)]),
                &[file(0, 10, 20)],
                &[],
                Some(&sketches),
                &specs,
                &[],
            )
            .await
            .unwrap();
        assert!(second.released().contains(&object));
    }
}
