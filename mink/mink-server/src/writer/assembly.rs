use std::collections::BTreeMap;

use mink_core::{Assembly, File, IndexedFile, Snapshot, TimePartition, millis, now_ms};

use super::Writer;
use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct AssemblyGroup {
    partition: i32,
    time: TimePartition,
    schema_id: i32,
    version: u16,
}

impl Writer {
    pub(super) async fn assemblies(
        &self,
        latest: &Snapshot,
        next: &Snapshot,
    ) -> Result<Vec<Assembly>, Error> {
        let config = &self.services.config;
        let store = &self.services.store;
        let specs = store.time_specs().list(self.topic.name()).await?;
        let ended_before = now_ms() - millis(config.assembly_delay);

        let tiered = store.tiered_ranges().ends(self.topic.name()).await?;

        let mut groups: BTreeMap<AssemblyGroup, Vec<IndexedFile>> = BTreeMap::new();
        for indexed in store.snapshots().files(latest).await? {
            let file = indexed.file();
            let ended = specs.bucket_ended(file.time(), ended_before);
            let untiered = tiered
                .get(&file.partition().index())
                .is_none_or(|end| file.start_offset() >= *end);

            if ended && untiered && next.covers(file) {
                groups
                    .entry(AssemblyGroup::of(file))
                    .or_default()
                    .push(indexed);
            }
        }

        let mut assemblies = Vec::new();
        for files in groups.values() {
            for run in sized_runs(files, config.assembled_bytes) {
                assemblies.push(self.assembly_of(run).await?);
            }
        }

        Ok(assemblies)
    }

    async fn assembly_of(&self, run: &[IndexedFile]) -> Result<Assembly, Error> {
        let store = &self.services.store;
        let replaced: Vec<File> = run.iter().map(|indexed| indexed.file().clone()).collect();
        let first = &replaced[0];

        let mut built = Vec::with_capacity(replaced.len());
        for file in &replaced {
            let stored = store.files().built(file).await?;
            built.push(stored.ok_or_else(|| mink_store::Error::MissingFile(file.clone()))?);
        }

        let topic = first.partition().topic();
        let schema = self
            .services
            .schemas
            .table_schema(topic, first.schema_id())
            .await?;

        Ok(mink_iceberg::assemble(&built, &schema)?)
    }
}

impl AssemblyGroup {
    fn of(file: &File) -> AssemblyGroup {
        AssemblyGroup {
            partition: file.partition().index(),
            time: file.time(),
            schema_id: file.schema_id(),
            version: file.version().number(),
        }
    }
}

fn sized_runs(files: &[IndexedFile], limit: u64) -> Vec<&[IndexedFile]> {
    let mut runs = Vec::new();
    let mut start = 0;
    let mut bytes = 0;

    for (index, indexed) in files.iter().enumerate() {
        let size = mink_iceberg::unpadded(indexed.stats().bytes());
        if index > start && bytes + size > limit {
            runs.push(&files[start..index]);
            start = index;
            bytes = 0;
        }

        bytes += size;
    }

    runs.push(&files[start..]);
    runs.retain(|run| run.len() >= 2);
    runs
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mink_core::{Granularity, Offsets, Partition, Scope, Span, Stats, TieredRange};
    use uuid::Uuid;

    use super::*;
    use crate::Config;
    use crate::testing::TTL;
    use crate::writer::tests::{WRITER, setup};

    #[tokio::test]
    async fn ended_buckets_are_assembled_within_their_size() {
        let hourly = |config: &mut Config| {
            config.granularity = Granularity::Hour;
            config.assembly_delay = Duration::from_millis(1);
        };
        let Some(setup) = setup(1, hourly).await else {
            return;
        };
        let mut writer = setup.writer(WRITER, TTL).await;
        let store = &setup.services.store;
        let live = |snapshot: Snapshot| async move {
            let files = store.snapshots().files(&snapshot).await.unwrap();

            files
                .into_iter()
                .map(|indexed| indexed.file().clone())
                .collect::<Vec<_>>()
        };

        setup.index_hour(0, 10, 100).await;
        setup.index_hour(10, 20, 100).await;
        writer.commit().await.unwrap().unwrap();
        let assembled = live(writer.commit().await.unwrap().unwrap()).await;

        setup.index_hour(20, 30, 100).await;
        writer.commit().await.unwrap().unwrap();
        let merged = live(writer.commit().await.unwrap().unwrap()).await;

        assert_eq!(assembled.len(), 1);
        let ranges = |file: File| async move {
            let built = store.files().built(&file).await.unwrap().unwrap();

            built.spans().iter().map(Span::range).collect::<Vec<_>>()
        };
        assert_eq!(
            ranges(assembled[0].clone()).await,
            mink_conformance::ranges(&[(0, 10), (10, 20)])
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(
            ranges(merged[0].clone()).await,
            mink_conformance::ranges(&[(0, 10), (10, 20), (20, 30)])
        );
    }

    #[test]
    fn runs_stay_within_the_assembled_size() {
        let files: Vec<IndexedFile> = (0..5)
            .map(|index| {
                let file = mink_conformance::file("orders", 0, index * 10, index * 10 + 10);

                IndexedFile::new(file.file().clone(), Stats::new(10, 69_637, 0))
            })
            .collect();

        let runs: Vec<usize> = sized_runs(&files, 10).iter().map(|run| run.len()).collect();
        let whole: Vec<usize> = sized_runs(&files, 100)
            .iter()
            .map(|run| run.len())
            .collect();

        assert_eq!(runs, [2, 2]);
        assert_eq!(whole, [5]);
    }

    #[tokio::test]
    async fn files_below_the_tiered_end_are_not_assembled() {
        let hourly = |config: &mut Config| {
            config.granularity = Granularity::Hour;
            config.assembly_delay = Duration::from_millis(1);
        };
        let Some(setup) = setup(1, hourly).await else {
            return;
        };
        let mut writer = setup.writer(WRITER, TTL).await;
        let store = &setup.services.store;
        setup.index_hour(0, 10, 100).await;
        setup.index_hour(10, 20, 100).await;
        setup.index_hour(20, 30, 100).await;
        let latest = writer.commit().await.unwrap().unwrap();
        let partition = Partition::new(setup.topic.name(), 0).unwrap();
        let tier = store
            .leases()
            .acquire(&Scope::Tier(partition.clone()), Uuid::from_u128(9), TTL)
            .await
            .unwrap()
            .unwrap();
        let offsets = Offsets::new(partition, 0, 10).unwrap();
        store
            .tiered_ranges()
            .add(&tier, &TieredRange::new(offsets, 0, Vec::new()))
            .await
            .unwrap();

        let assemblies = writer.assemblies(&latest, &latest).await.unwrap();

        let replaced: Vec<(i64, i64)> = assemblies
            .iter()
            .flat_map(Assembly::replaced)
            .map(|file| (file.start_offset(), file.end_offset()))
            .collect();
        assert_eq!(replaced, [(10, 20), (20, 30)]);
    }
}
