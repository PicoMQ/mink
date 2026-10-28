use mink_conformance::{MetadataStep, WAREHOUSE, file};
use mink_core::{Offsets, Partition, Snapshot, Topic};
use mink_iceberg::{FormatVersion, Table};
use uuid::Uuid;

const VERSION: FormatVersion = FormatVersion::V2;
const START_MS: i64 = 1_793_000_000_000;
const STEP_MS: i64 = 60_000;

struct Step {
    epoch: i64,
    expire_through: Option<i64>,
    offsets: [(i64, i64); 2],
    added: &'static [(i32, i64, i64)],
}

const SCRIPT: [Step; 5] = [
    Step {
        epoch: 1,
        expire_through: None,
        offsets: [(0, 100), (0, 50)],
        added: &[(0, 0, 100), (1, 0, 50)],
    },
    Step {
        epoch: 1,
        expire_through: None,
        offsets: [(0, 200), (0, 50)],
        added: &[(0, 100, 200)],
    },
    Step {
        epoch: 2,
        expire_through: None,
        offsets: [(100, 250), (0, 80)],
        added: &[(0, 200, 250), (1, 50, 80)],
    },
    Step {
        epoch: 2,
        expire_through: Some(1),
        offsets: [(200, 250), (0, 80)],
        added: &[],
    },
    Step {
        epoch: 2,
        expire_through: None,
        offsets: [(200, 300), (0, 80)],
        added: &[(0, 250, 300)],
    },
];

pub async fn generate(bless: bool) -> anyhow::Result<()> {
    let topic = Topic::new("orders", Uuid::from_u128(1), 2)?;
    let specs = mink_core::TimeSpecs::new(vec![mink_core::Granularity::None])?;
    let mut table = Table::new(VERSION, WAREHOUSE, &topic);
    let mut steps = Vec::new();
    let mut written = Vec::new();

    for (index, step) in SCRIPT.into_iter().enumerate() {
        let id = index as i64 + 1;
        if let Some(through_id) = step.expire_through {
            table = table.expire(through_id)?;
        }

        let offsets = step
            .offsets
            .iter()
            .enumerate()
            .map(|(partition, (start, end))| {
                Offsets::new(
                    Partition::new(topic.name(), partition as i32)?,
                    *start,
                    *end,
                )
            })
            .collect::<Result<_, _>>()?;
        let snapshot = Snapshot::new(id, START_MS + id * STEP_MS, step.epoch, offsets)?;
        let added: Vec<_> = step
            .added
            .iter()
            .map(|(partition, start, end)| file(topic.name(), *partition, *start, *end))
            .collect();

        let change = table
            .apply(&snapshot, &added, &[], None, &specs, &[])
            .await?;

        let objects = change.objects().map(|(object, _)| object.clone()).collect();
        let released = change.released().to_vec();
        steps.push(MetadataStep::new(
            step.expire_through,
            snapshot,
            added,
            objects,
            released,
        ));
        written.extend(change.objects().cloned());
        table = change.into_table();
    }

    if !bless {
        println!(
            "applied {} snapshots; rerun with --bless to write conformance/fixtures/metadata",
            steps.len()
        );
        return Ok(());
    }

    mink_conformance::bless_metadata(WAREHOUSE, &topic, VERSION.iceberg(), &steps, &written)?;
    println!(
        "wrote {} snapshots to conformance/fixtures/metadata",
        steps.len()
    );

    Ok(())
}
