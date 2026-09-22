//! Uploads key-value checkpoints to object storage with shared-file reuse, and downloads and discards them.

use std::collections::{HashMap, HashSet};
use std::path::Path as LocalPath;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use mink_table::Bucket;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Error;
use crate::autoinc::IdRange;
use crate::recover::RecoverPoint;
use crate::tablet::Checkpoint;

pub const METADATA_FILE: &str = "_METADATA";
const SNAPSHOT_DIR_PREFIX: &str = "snap-";
const SHARED_DIR: &str = "shared";
const MULTIPART_THRESHOLD: u64 = 16 << 20;
const CHUNK: usize = 8 << 20;
const PARALLELISM: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotFile {
    pub name: String,
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedSnapshot {
    pub version: u32,
    pub bucket: Bucket,
    pub snapshot_id: u64,
    pub location: String,
    pub shared: Vec<SnapshotFile>,
    pub private: Vec<SnapshotFile>,
    pub incremental_size: u64,
    pub log_offset: i64,
    pub row_count: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_increment: Option<IdRange>,
}

impl CompletedSnapshot {
    pub const VERSION: u32 = 1;

    pub fn recover_point(&self) -> RecoverPoint {
        RecoverPoint {
            log_offset: self.log_offset,
            row_count: self.row_count,
            auto_increment: self.auto_increment,
        }
    }

    pub fn total_size(&self) -> u64 {
        self.shared
            .iter()
            .chain(&self.private)
            .map(|f| f.size)
            .sum()
    }

    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec_pretty(self).expect("snapshot serializes")
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, Error> {
        let snapshot: CompletedSnapshot =
            serde_json::from_slice(bytes).map_err(|e| Error::SnapshotCorrupt(e.to_string()))?;
        if snapshot.version != Self::VERSION {
            return Err(Error::SnapshotCorrupt(format!(
                "version {} is not {}",
                snapshot.version,
                Self::VERSION
            )));
        }
        Ok(snapshot)
    }

    pub fn metadata_path(location: &str) -> Path {
        Path::from(location).join(METADATA_FILE)
    }

    pub async fn load(storage: &dyn ObjectStore, location: &str) -> Result<Self, Error> {
        let bytes = storage
            .get(&Self::metadata_path(location))
            .await?
            .bytes()
            .await?;
        Self::from_json(&bytes)
    }
}

pub struct Uploader {
    storage: Arc<dyn ObjectStore>,
    tablet_dir: Path,
    uploaded: HashMap<u64, HashMap<String, SnapshotFile>>,
    last_completed: Option<u64>,
}

impl Uploader {
    pub fn new(
        storage: Arc<dyn ObjectStore>,
        tablet_dir: Path,
        restored: Option<&CompletedSnapshot>,
    ) -> Self {
        let mut uploader = Uploader {
            storage,
            tablet_dir,
            uploaded: HashMap::new(),
            last_completed: None,
        };
        if let Some(snapshot) = restored {
            uploader.uploaded.insert(
                snapshot.snapshot_id,
                snapshot
                    .shared
                    .iter()
                    .map(|f| (f.name.clone(), f.clone()))
                    .collect(),
            );
            uploader.last_completed = Some(snapshot.snapshot_id);
        }
        uploader
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> Path {
        self.tablet_dir
            .clone()
            .join(format!("{SNAPSHOT_DIR_PREFIX}{snapshot_id}"))
    }

    pub async fn upload(
        &mut self,
        snapshot_id: u64,
        bucket: Bucket,
        checkpoint: &Checkpoint,
    ) -> Result<CompletedSnapshot, Error> {
        let mut uploaded = Vec::new();
        match self
            .try_upload(snapshot_id, bucket, checkpoint, &mut uploaded)
            .await
        {
            Ok(snapshot) => Ok(snapshot),
            Err(error) => {
                for path in uploaded {
                    let _ = self.storage.delete(&path).await;
                }
                Err(error)
            }
        }
    }

    async fn try_upload(
        &mut self,
        snapshot_id: u64,
        bucket: Bucket,
        checkpoint: &Checkpoint,
        uploaded: &mut Vec<Path>,
    ) -> Result<CompletedSnapshot, Error> {
        let previous = self
            .last_completed
            .and_then(|id| self.uploaded.get(&id))
            .cloned()
            .unwrap_or_default();
        let files: &mink_kv::Checkpoint = &checkpoint.files;
        let snapshot_dir = self.snapshot_dir(snapshot_id);
        let shared_dir = self
            .tablet_dir
            .clone()
            .join(SHARED_DIR)
            .join(snapshot_id.to_string());

        let mut shared = Vec::new();
        let mut private = Vec::new();
        let mut pending = Vec::new();
        for file in &files.files {
            let name = file.path.to_string_lossy().into_owned();
            if file.shared
                && let Some(reused) = previous.get(&name)
            {
                shared.push(reused.clone());
                continue;
            }
            let dir = if file.shared {
                &shared_dir
            } else {
                &snapshot_dir
            };
            let path = name
                .split('/')
                .fold(dir.clone(), |path, part| path.join(part));
            pending.push((
                file.shared,
                files.dir.join(&file.path),
                name,
                path,
                file.size,
            ));
        }

        let storage = Arc::clone(&self.storage);
        let mut results = futures::stream::iter(pending.into_iter().map(
            |(is_shared, local, name, path, size)| {
                let storage = Arc::clone(&storage);
                async move {
                    put_file(storage.as_ref(), &local, &path, size).await?;
                    Ok::<_, Error>((
                        is_shared,
                        SnapshotFile {
                            name,
                            path: path.to_string(),
                            size,
                        },
                    ))
                }
            },
        ))
        .buffer_unordered(PARALLELISM);
        let mut incremental_size = 0;
        while let Some(result) = results.next().await {
            let (is_shared, file) = result?;
            uploaded.push(Path::from(file.path.as_str()));
            incremental_size += file.size;
            if is_shared {
                shared.push(file);
            } else {
                private.push(file);
            }
        }
        shared.sort_by(|a, b| a.name.cmp(&b.name));
        private.sort_by(|a, b| a.name.cmp(&b.name));

        let snapshot = CompletedSnapshot {
            version: CompletedSnapshot::VERSION,
            bucket,
            snapshot_id,
            location: snapshot_dir.to_string(),
            shared,
            private,
            incremental_size,
            log_offset: checkpoint.recover_point.log_offset,
            row_count: checkpoint.recover_point.row_count,
            auto_increment: checkpoint.recover_point.auto_increment,
        };
        let metadata = CompletedSnapshot::metadata_path(&snapshot.location);
        self.storage
            .put(&metadata, PutPayload::from(snapshot.to_json()))
            .await?;
        uploaded.push(metadata);

        self.uploaded.insert(
            snapshot_id,
            snapshot
                .shared
                .iter()
                .map(|f| (f.name.clone(), f.clone()))
                .collect(),
        );
        Ok(snapshot)
    }

    pub fn completed(&mut self, snapshot_id: u64) {
        self.uploaded.retain(|id, _| *id >= snapshot_id);
        self.last_completed = Some(snapshot_id);
    }

    pub async fn aborted(&mut self, snapshot: &CompletedSnapshot) -> Result<(), Error> {
        self.uploaded.remove(&snapshot.snapshot_id);
        let still_used: HashSet<&str> = self
            .uploaded
            .values()
            .flat_map(|files| files.values().map(|f| f.path.as_str()))
            .collect();
        remove(self.storage.as_ref(), snapshot, &still_used).await
    }
}

pub async fn download(
    storage: &dyn ObjectStore,
    snapshot: &CompletedSnapshot,
    dir: &LocalPath,
) -> Result<(), Error> {
    tokio::fs::create_dir_all(dir).await?;
    // A `map` over `&SnapshotFile` here trips rustc's higher-ranked `Send` inference.
    let mut downloads = Vec::with_capacity(snapshot.shared.len() + snapshot.private.len());
    for file in snapshot.shared.iter().chain(&snapshot.private) {
        downloads.push(download_file(storage, file, dir));
    }
    futures::stream::iter(downloads)
        .buffer_unordered(PARALLELISM)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn download_file(
    storage: &dyn ObjectStore,
    file: &SnapshotFile,
    dir: &LocalPath,
) -> Result<(), Error> {
    let local = dir.join(&file.name);
    if let Some(parent) = local.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut out = File::create(&local).await?;
    let mut stream = storage
        .get(&Path::from(file.path.as_str()))
        .await?
        .into_stream();
    while let Some(chunk) = stream.try_next().await? {
        out.write_all(&chunk).await?;
    }
    out.flush().await?;
    Ok(())
}

pub async fn discard(
    storage: &dyn ObjectStore,
    snapshot: &CompletedSnapshot,
    retained: &[CompletedSnapshot],
) -> Result<(), Error> {
    let still_used: HashSet<&str> = retained
        .iter()
        .flat_map(|s| s.shared.iter().map(|f| f.path.as_str()))
        .collect();
    remove(storage, snapshot, &still_used).await
}

async fn remove(
    storage: &dyn ObjectStore,
    snapshot: &CompletedSnapshot,
    still_used: &HashSet<&str>,
) -> Result<(), Error> {
    let mut paths: Vec<Path> = snapshot
        .private
        .iter()
        .map(|f| Path::from(f.path.as_str()))
        .collect();
    paths.extend(
        snapshot
            .shared
            .iter()
            .filter(|f| !still_used.contains(f.path.as_str()))
            .map(|f| Path::from(f.path.as_str())),
    );
    paths.push(CompletedSnapshot::metadata_path(&snapshot.location));
    for path in paths {
        match storage.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn put_file(
    storage: &dyn ObjectStore,
    local: &LocalPath,
    path: &Path,
    size: u64,
) -> Result<(), Error> {
    if size < MULTIPART_THRESHOLD {
        let bytes = tokio::fs::read(local).await?;
        storage
            .put(path, PutPayload::from(Bytes::from(bytes)))
            .await?;
        return Ok(());
    }
    let mut file = File::open(local).await?;
    let mut upload = WriteMultipart::new_with_chunk_size(storage.put_multipart(path).await?, CHUNK);
    let mut buffer = vec![0u8; CHUNK];
    loop {
        let read = AsyncReadExt::read(&mut file, &mut buffer).await?;
        if read == 0 {
            break;
        }
        upload.wait_for_capacity(PARALLELISM).await?;
        upload.write(&buffer[..read]);
    }
    upload.finish().await?;
    Ok(())
}
