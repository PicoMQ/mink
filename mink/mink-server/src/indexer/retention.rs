use mink_core::IndexedFile;

use super::Indexer;
use crate::Error;

impl Indexer {
    pub(super) async fn trim_to_retention(&mut self) -> Result<(), Error> {
        let earliest = self
            .services
            .kafka
            .cluster()
            .await?
            .earliest(&self.partition)
            .await?;
        if earliest <= self.indexed.start_offset() {
            return Ok(());
        }

        let files = self.services.store.files().list(&self.indexed).await?;
        let boundary = boundary_at_or_after(&files, earliest, self.indexed.end_offset());

        self.indexed = self
            .services
            .store
            .indexed_ranges()
            .trim(&self.lease, boundary)
            .await?;
        Ok(())
    }
}

fn boundary_at_or_after(files: &[IndexedFile], earliest: i64, end_offset: i64) -> i64 {
    let mut boundary = files
        .iter()
        .map(|indexed| indexed.file().start_offset())
        .find(|start| *start >= earliest)
        .unwrap_or(end_offset);

    while let Some(assembled) = files.iter().map(IndexedFile::file).find(|file| {
        file.assembly().is_some() && file.start_offset() < boundary && boundary < file.end_offset()
    }) {
        boundary = assembled.end_offset();
    }

    boundary
}

#[cfg(test)]
mod tests {
    use mink_core::{OffsetRange, Partition};

    use super::*;
    use crate::config::History;
    use crate::indexer::tests::{bounds, indexed_files, indexer, produce};

    #[test]
    fn trims_never_split_an_assembled_file() {
        let partition = Partition::new("orders", 0).unwrap();
        let hour = |bucket| mink_core::TimePartition::new(0, Some(bucket));
        let file = |start, end, bucket| {
            let file = mink_core::File::v1(partition.clone(), start, end, hour(bucket), 0).unwrap();

            IndexedFile::new(file, mink_core::Stats::new(1, 1, 0))
        };
        let assembled = mink_core::File::assembled(
            partition.clone(),
            hour(5),
            0,
            mink_core::FileVersion::V1,
            &[
                OffsetRange::new(0, 10).unwrap(),
                OffsetRange::new(20, 30).unwrap(),
            ],
        )
        .unwrap();
        let files = [
            IndexedFile::new(assembled, mink_core::Stats::new(1, 1, 0)),
            file(10, 20, 6),
            file(30, 40, 5),
        ];

        assert_eq!(boundary_at_or_after(&files, 5, 40), 30);
        assert_eq!(boundary_at_or_after(&files, 31, 40), 40);
        assert_eq!(boundary_at_or_after(&files, 0, 40), 0);
    }

    #[tokio::test]
    async fn retention_trims_to_the_next_file_boundary() {
        let Some((mut indexer, topic, bootstrap)) = indexer(|config| {
            config.history = History::None;
            config.file_bytes = 4;
        })
        .await
        else {
            return;
        };
        produce(&bootstrap, &topic, 6);
        indexer.index().await.unwrap();

        mink_conformance::delete_records(&bootstrap, topic.name(), 0, 3).await;
        indexer.trim_to_retention().await.unwrap();

        assert_eq!(bounds(&indexer), (4, 6));
        assert_eq!(indexed_files(&indexer).await, [(4, 6, 2)]);
    }
}
