use std::time::Instant;

use mink_core::{Offsets, Snapshot};
use mink_iceberg::Table;

use super::tables::MetadataFile;
use crate::Error;
use crate::services::Services;

pub(super) async fn offsets(
    services: &Services,
    snapshot: &Snapshot,
) -> Result<Vec<Offsets>, Error> {
    let mut cluster = services.kafka.cluster().await?;
    let mut offsets = Vec::with_capacity(snapshot.offsets().len());

    for committed in snapshot.offsets() {
        let stable = cluster.last_stable_offset(committed.partition()).await?;
        offsets.push(Offsets::new(
            committed.partition().clone(),
            committed.start_offset(),
            stable.max(committed.end_offset()),
        )?);
    }

    Ok(offsets)
}

pub(super) async fn build(
    services: &Services,
    name: &str,
    table: &Table,
    snapshot: &Snapshot,
    offsets: Vec<Offsets>,
) -> Result<MetadataFile, Error> {
    let started = Instant::now();

    let built = metadata(services, name, table, snapshot, offsets).await?;

    services.metrics.tail(started.elapsed());
    Ok(built)
}

async fn metadata(
    services: &Services,
    name: &str,
    table: &Table,
    snapshot: &Snapshot,
    offsets: Vec<Offsets>,
) -> Result<MetadataFile, Error> {
    let specs = services.store.time_specs().list(name).await?;
    let mut added = Vec::new();

    for (committed, tail) in snapshot.offsets().iter().zip(&offsets) {
        if tail.end_offset() == committed.end_offset() {
            continue;
        }

        let unread = Offsets::new(
            tail.partition().clone(),
            committed.end_offset(),
            tail.end_offset(),
        )?;
        let records = services.kafka.cluster().await?.read(&unread).await?;
        if records.is_empty() {
            continue;
        }

        let schema = services.schemas.latest_file_schema(name, &records).await?;
        let files = mink_iceberg::tail_files(&unread, &records, specs.current(), &schema)?;

        added.extend(files);
    }

    let schemas = services.store.schemas().table_schemas(name).await?;
    let change = table
        .tail(snapshot, offsets, &added, &specs, &schemas)
        .await?;
    let objects = services.warehouse.write_all(&change).await?;
    services
        .store
        .objects()
        .add_unreferenced(name, &objects)
        .await?;

    let (object, bytes) = change.metadata();
    Ok(MetadataFile::new(object.path().to_owned(), bytes.clone()))
}
