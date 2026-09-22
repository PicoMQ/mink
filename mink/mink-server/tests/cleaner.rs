//! Snapshot deletion against an in-memory object store, including shared files and missing snapshots.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use futures::TryStreamExt;
use mink_coordinator::SnapshotCleaner;
use mink_metadata::KvSnapshotRow;
use mink_server::Cleaner;
use mink_table::{Bucket, BucketId, Id};
use mink_tablet::{Checkpoint, CompletedSnapshot, RecoverPoint, Uploader};
use object_store::ObjectStore;
use object_store::memory::InMemory;

fn bucket() -> Bucket {
    Bucket::new(Id(7), BucketId(2))
}

fn checkpoint(dir: &Path, files: &[(&str, &[u8])], log_offset: i64) -> Checkpoint {
    fs::create_dir_all(dir).unwrap();
    for (name, content) in files {
        fs::write(dir.join(name), content).unwrap();
    }
    Checkpoint {
        files: mink_kv::Checkpoint::scan(dir).unwrap(),
        recover_point: RecoverPoint {
            log_offset,
            row_count: log_offset,
            auto_increment: None,
        },
    }
}

fn row(snapshot: &CompletedSnapshot) -> KvSnapshotRow {
    KvSnapshotRow {
        snapshot_id: snapshot.snapshot_id,
        log_offset: snapshot.recover_point().log_offset,
        row_count: snapshot.recover_point().row_count,
        path: snapshot.location.clone(),
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
async fn discarding_spares_files_the_retained_snapshot_shares() {
    let storage = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let mut uploader = Uploader::new(storage.clone(), "kv/7/2".into(), None);

    let first = checkpoint(
        &dir.path().join("cp1"),
        &[("a.sst", b"aaaa"), ("MANIFEST", b"m1")],
        10,
    );
    let one = uploader.upload(1, bucket(), &first).await.unwrap();
    uploader.completed(1);
    let second = checkpoint(
        &dir.path().join("cp2"),
        &[("a.sst", b"aaaa"), ("c.sst", b"ccc"), ("MANIFEST", b"m2")],
        20,
    );
    let two = uploader.upload(2, bucket(), &second).await.unwrap();
    uploader.completed(2);

    let cleaner = Cleaner::new(storage.clone());
    cleaner
        .discard(bucket(), &row(&one), &[row(&two)])
        .await
        .unwrap();
    assert_eq!(
        keys(&storage).await,
        vec![
            "kv/7/2/shared/1/a.sst",
            "kv/7/2/shared/2/c.sst",
            "kv/7/2/snap-2/MANIFEST",
            "kv/7/2/snap-2/_METADATA",
        ],
        "a.sst is still shared by snapshot 2; snapshot 1's own files are gone"
    );

    cleaner
        .discard(bucket(), &row(&one), &[row(&two)])
        .await
        .unwrap();

    cleaner.discard(bucket(), &row(&two), &[]).await.unwrap();
    assert!(keys(&storage).await.is_empty());
}
