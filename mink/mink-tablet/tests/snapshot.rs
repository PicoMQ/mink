//! Snapshot upload, download and discard against in-memory object storage.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::{fs, slice};

use futures::TryStreamExt;
use mink_kv::{Engine, MemoryEngine, Options};
use mink_table::{Bucket, BucketId, Id};
use mink_tablet::{
    Checkpoint, CompletedSnapshot, IdRange, METADATA_FILE, RecoverPoint, Uploader, discard,
    download,
};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};

fn bucket() -> Bucket {
    Bucket::new(Id(7), BucketId(2))
}

fn checkpoint(dir: &Path, files: &[(&str, &[u8])], log_offset: i64) -> Checkpoint {
    fs::create_dir_all(dir).unwrap();
    for (name, content) in files {
        let path = dir.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }
    Checkpoint {
        files: mink_kv::Checkpoint::scan(dir).unwrap(),
        recover_point: RecoverPoint {
            log_offset,
            row_count: log_offset,
            auto_increment: Some(IdRange {
                column_id: 1,
                start: log_offset,
                end: 100,
            }),
        },
    }
}

async fn keys(storage: &InMemory) -> Vec<String> {
    let mut keys: Vec<String> = storage
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect()
        .await
        .unwrap();
    keys.sort();
    keys
}

#[tokio::test]
async fn incremental_upload_reuses_confirmed_table_files() {
    let storage = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let mut uploader = Uploader::new(storage.clone(), "kv/7/2".into(), None);

    let first = checkpoint(
        &dir.path().join("cp1"),
        &[
            ("a.sst", b"aaaa"),
            ("tables/b.sst", b"bb"),
            ("MANIFEST", b"m1"),
        ],
        10,
    );
    let one = uploader.upload(1, bucket(), &first).await.unwrap();
    assert_eq!(one.snapshot_id, 1);
    assert_eq!(one.location, "kv/7/2/snap-1");
    assert_eq!(one.incremental_size, 8);
    assert_eq!(one.total_size(), 8);
    assert_eq!(one.recover_point(), first.recover_point);
    let shared: Vec<(&str, &str)> = one
        .shared
        .iter()
        .map(|f| (f.name.as_str(), f.path.as_str()))
        .collect();
    assert_eq!(
        shared,
        vec![
            ("a.sst", "kv/7/2/shared/1/a.sst"),
            ("tables/b.sst", "kv/7/2/shared/1/tables/b.sst"),
        ]
    );
    assert_eq!(one.private.len(), 1);
    assert_eq!(one.private[0].path, "kv/7/2/snap-1/MANIFEST");
    assert_eq!(
        keys(&storage).await,
        vec![
            "kv/7/2/shared/1/a.sst",
            "kv/7/2/shared/1/tables/b.sst",
            "kv/7/2/snap-1/MANIFEST",
            "kv/7/2/snap-1/_METADATA",
        ]
    );
    let loaded = CompletedSnapshot::load(storage.as_ref(), &one.location)
        .await
        .unwrap();
    assert_eq!(loaded, one);

    let second = checkpoint(
        &dir.path().join("cp2"),
        &[("a.sst", b"aaaa"), ("c.sst", b"ccc"), ("MANIFEST", b"m2")],
        20,
    );
    let two = uploader.upload(2, bucket(), &second).await.unwrap();
    assert_eq!(two.incremental_size, 9);
    uploader.aborted(&two).await.unwrap();
    assert_eq!(keys(&storage).await.len(), 4);

    uploader.completed(1);
    let two = uploader.upload(2, bucket(), &second).await.unwrap();
    assert_eq!(two.incremental_size, 5);
    assert_eq!(two.total_size(), 9);
    let by_name: BTreeMap<&str, &str> = two
        .shared
        .iter()
        .map(|f| (f.name.as_str(), f.path.as_str()))
        .collect();
    assert_eq!(by_name["a.sst"], "kv/7/2/shared/1/a.sst");
    assert_eq!(by_name["c.sst"], "kv/7/2/shared/2/c.sst");
    uploader.completed(2);

    let restored = dir.path().join("restored");
    download(storage.as_ref(), &two, &restored).await.unwrap();
    assert_eq!(fs::read(restored.join("a.sst")).unwrap(), b"aaaa");
    assert_eq!(fs::read(restored.join("c.sst")).unwrap(), b"ccc");
    assert_eq!(fs::read(restored.join("MANIFEST")).unwrap(), b"m2");
    assert!(!restored.join("tables").exists());

    discard(storage.as_ref(), &one, slice::from_ref(&two))
        .await
        .unwrap();
    assert_eq!(
        keys(&storage).await,
        vec![
            "kv/7/2/shared/1/a.sst",
            "kv/7/2/shared/2/c.sst",
            "kv/7/2/snap-2/MANIFEST",
            "kv/7/2/snap-2/_METADATA",
        ]
    );
    discard(storage.as_ref(), &one, slice::from_ref(&two))
        .await
        .unwrap();

    let third = checkpoint(
        &dir.path().join("cp3"),
        &[("a.sst", b"aaaa"), ("d.sst", b"d"), ("MANIFEST", b"m3")],
        30,
    );
    let three = uploader.upload(3, bucket(), &third).await.unwrap();
    assert_eq!(three.incremental_size, 3);
    uploader.aborted(&three).await.unwrap();
    assert_eq!(keys(&storage).await.len(), 4);

    discard(storage.as_ref(), &two, &[]).await.unwrap();
    assert!(keys(&storage).await.is_empty());
}

#[tokio::test]
async fn uploader_restored_from_a_snapshot_reuses_its_files() {
    let storage = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let first = checkpoint(
        &dir.path().join("cp1"),
        &[("a.sst", b"aaaa"), ("MANIFEST", b"m")],
        1,
    );
    let one = Uploader::new(storage.clone(), "kv/1/0".into(), None)
        .upload(1, bucket(), &first)
        .await
        .unwrap();

    let mut uploader = Uploader::new(storage.clone(), "kv/1/0".into(), Some(&one));
    let second = checkpoint(
        &dir.path().join("cp2"),
        &[("a.sst", b"aaaa"), ("MANIFEST", b"m")],
        2,
    );
    let two = uploader.upload(2, bucket(), &second).await.unwrap();
    assert_eq!(two.incremental_size, 1);
    assert_eq!(two.shared[0].path, "kv/1/0/shared/1/a.sst");
}

#[tokio::test]
async fn failed_upload_leaves_nothing_behind() {
    let storage = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let mut cp = checkpoint(
        &dir.path().join("cp"),
        &[("a.sst", b"aaaa"), ("MANIFEST", b"m")],
        1,
    );
    cp.files.files[0].path = "missing".into();
    let mut uploader = Uploader::new(storage.clone(), "kv/1/0".into(), None);
    uploader.upload(1, bucket(), &cp).await.unwrap_err();
    assert!(keys(&storage).await.is_empty());
}

#[tokio::test]
async fn metadata_rejects_other_versions() {
    let storage = InMemory::new();
    let mut snapshot = CompletedSnapshot {
        version: CompletedSnapshot::VERSION,
        bucket: bucket(),
        snapshot_id: 1,
        location: "kv/x/snap-1".into(),
        shared: vec![],
        private: vec![],
        incremental_size: 0,
        log_offset: 0,
        row_count: 0,
        auto_increment: None,
    };
    let path = CompletedSnapshot::metadata_path(&snapshot.location);
    assert_eq!(path.to_string(), format!("kv/x/snap-1/{METADATA_FILE}"));
    snapshot.version += 1;
    storage.put(&path, snapshot.to_json().into()).await.unwrap();
    assert!(
        CompletedSnapshot::load(&storage, &snapshot.location)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn memory_store_round_trips_through_object_storage() {
    let storage = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let store = MemoryEngine
        .open(Path::new("/unused"), Options::default())
        .unwrap();
    let mut batch = mink_kv::Batch::new();
    batch.put(b"k".to_vec(), b"v".to_vec());
    store.write(batch).await.unwrap();
    let files = store.checkpoint(&dir.path().join("cp")).unwrap();
    let cp = Checkpoint {
        files,
        recover_point: RecoverPoint {
            log_offset: 1,
            row_count: 1,
            auto_increment: None,
        },
    };
    let snapshot = Uploader::new(storage.clone(), "kv/1/0".into(), None)
        .upload(1, bucket(), &cp)
        .await
        .unwrap();

    let downloaded = dir.path().join("down");
    download(storage.as_ref(), &snapshot, &downloaded)
        .await
        .unwrap();
    let restored = MemoryEngine
        .restore(
            &dir.path().join("restored"),
            &downloaded,
            Options::default(),
        )
        .unwrap();
    assert_eq!(restored.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}
