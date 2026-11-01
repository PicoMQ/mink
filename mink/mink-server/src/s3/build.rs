use std::time::Instant;

use bytes::Bytes;
use mink_core::{File, Offsets, TableLocation};
use mink_metrics::Outcome;

use crate::Error;
use crate::services::Services;

pub(super) async fn build(services: &Services, file: &File) -> Result<Bytes, Error> {
    let started = Instant::now();

    let built = contents(services, file).await;

    if !matches!(built, Err(Error::NoSuchKey(_))) {
        let outcome = Outcome::of(&built);
        services.metrics.build(outcome, started.elapsed());
    }

    if let Err(error @ Error::BuiltLengthMismatch { .. }) = &built {
        tracing::error!(%error, "a built file does not match its stored stats");
    }
    built
}

async fn contents(services: &Services, file: &File) -> Result<Bytes, Error> {
    let path = TableLocation::data_file(services.warehouse.root(), file);
    let not_found = || Error::NoSuchKey(path.clone());
    let store = &services.store;
    let topic = file.partition().topic();

    if file.assembly().is_some() {
        return Err(not_found());
    }
    let stored = store.files().built(file).await?;

    let schema = services
        .schemas
        .file_schema(topic, file.schema_id())
        .await?
        .ok_or_else(not_found)?;

    let offsets = Offsets::new(
        file.partition().clone(),
        file.start_offset(),
        file.end_offset(),
    )?;
    let records = services.kafka.cluster().await?.read(&offsets).await?;

    let specs = store.time_specs().list(topic).await?;

    let contents = mink_iceberg::file_contents(file, &records, &specs, &schema)?;
    let bytes = contents.ok_or_else(not_found)?;
    let length = stored.map(|stored| stored.indexed().stats().bytes());

    match length {
        Some(length) if length != bytes.len() as u64 => Err(Error::BuiltLengthMismatch {
            path,
            built: bytes.len() as u64,
            stored: length,
        }),
        _ => Ok(bytes),
    }
}
