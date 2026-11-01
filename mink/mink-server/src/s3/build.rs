use std::ops::Range;
use std::time::Instant;

use bytes::Bytes;
use mink_core::{BuiltFile, File, Offsets, Record, TableLocation};
use mink_metrics::Outcome;

use crate::Error;
use crate::services::Services;

pub(super) async fn build(services: &Services, file: &File) -> Result<Bytes, Error> {
    measured(services, whole(services, file)).await
}

pub(super) async fn extent(
    services: &Services,
    built: &BuiltFile,
    range: Range<u64>,
) -> Result<Bytes, Error> {
    measured(services, joined(services, built, range)).await
}

async fn measured(
    services: &Services,
    work: impl Future<Output = Result<Bytes, Error>>,
) -> Result<Bytes, Error> {
    let started = Instant::now();

    let built = work.await;

    if !matches!(built, Err(Error::NoSuchKey(_))) {
        let outcome = Outcome::of(&built);
        services.metrics.build(outcome, started.elapsed());
    }

    if let Err(error @ Error::BuiltLengthMismatch { .. }) = &built {
        tracing::error!(%error, "a build does not match what Postgres holds");
    }
    built
}

async fn whole(services: &Services, file: &File) -> Result<Bytes, Error> {
    if file.assembly().is_some() {
        return Err(not_found(services, file));
    }
    let stored = services.store.files().built(file).await?;

    let bytes = from_kafka(services, std::slice::from_ref(file))
        .await?
        .remove(0);
    let length = stored.map(|stored| stored.indexed().stats().bytes());

    match length {
        Some(length) if length != bytes.len() as u64 => Err(Error::BuiltLengthMismatch {
            path: path(services, file),
            built: bytes.len() as u64,
            stored: length,
        }),
        _ => Ok(bytes),
    }
}

async fn joined(services: &Services, built: &BuiltFile, range: Range<u64>) -> Result<Bytes, Error> {
    let file = built.indexed().file();
    let extent = mink_iceberg::extent(built, range).ok_or_else(|| not_found(services, file))?;
    let parts = from_kafka(services, &extent.parts(file)?).await?;

    match extent.bytes(file, &parts) {
        Ok(bytes) => Ok(bytes),
        Err(mink_iceberg::Error::PartLength { file, built, span }) => {
            Err(Error::BuiltLengthMismatch {
                path: path(services, &file),
                built,
                stored: span,
            })
        }
        Err(error) => Err(error.into()),
    }
}

async fn from_kafka(services: &Services, files: &[File]) -> Result<Vec<Bytes>, Error> {
    let (Some(first), Some(last)) = (files.first(), files.last()) else {
        return Ok(Vec::new());
    };
    let topic = first.partition().topic();

    let schema = services
        .schemas
        .file_schema(topic, first.schema_id())
        .await?
        .ok_or_else(|| not_found(services, first))?;

    let offsets = Offsets::new(
        first.partition().clone(),
        first.start_offset(),
        last.end_offset(),
    )?;
    let records = services.kafka.cluster().await?.read(&offsets).await?;

    let specs = services.store.time_specs().list(topic).await?;

    let mut built = Vec::with_capacity(files.len());
    for file in files {
        let inside: Vec<Record> = records
            .iter()
            .filter(|record| file.range().includes(record.offset()))
            .cloned()
            .collect();
        let contents = mink_iceberg::file_contents(file, &inside, &specs, &schema)?;

        built.push(contents.ok_or_else(|| not_found(services, file))?);
    }

    Ok(built)
}

pub(super) fn path(services: &Services, file: &File) -> String {
    TableLocation::data_file(services.warehouse.root(), file)
}

fn not_found(services: &Services, file: &File) -> Error {
    Error::NoSuchKey(path(services, file))
}
