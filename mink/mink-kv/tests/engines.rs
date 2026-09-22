//! Behavioral contract every key-value engine must satisfy, run against each available engine.

use std::fs;
use std::future::Future;

use bytes::Bytes;
use mink_kv::{BATCH_CAPACITY, Batch, Engine, Error, MemoryEngine, Options, Store, Writer};

fn engines() -> Vec<(&'static str, Box<dyn Engine>)> {
    let engines: Vec<(&'static str, Box<dyn Engine>)> = vec![
        ("memory", Box::new(MemoryEngine)),
        #[cfg(feature = "surrealkv")]
        ("surrealkv", Box::new(mink_kv::SurrealEngine)),
    ];
    engines
}

fn options() -> Options {
    Options {
        memtable_bytes: 1 << 20,
        ..Options::default()
    }
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

async fn put_all(store: &dyn Store, pairs: &[(&str, &str)]) {
    let mut batch = Batch::new();
    for (k, v) in pairs {
        batch.put(b(k), b(v));
    }
    store.write(batch).await.unwrap();
}

async fn for_each_engine<F, Fut>(f: F)
where
    F: Fn(&'static str, Box<dyn Store>, Box<dyn Engine>, tempfile::TempDir) -> Fut,
    Fut: Future<Output = ()>,
{
    for (name, engine) in engines() {
        let dir = tempfile::tempdir().unwrap();
        let store = engine.open(&dir.path().join("kv"), options()).unwrap();
        f(name, store, engine, dir).await;
    }
}

#[tokio::test]
async fn point_reads_and_atomic_writes() {
    for_each_engine(|name, store, _, dir| async move {
        assert_eq!(store.get(b"a").unwrap(), None, "{name}");
        put_all(store.as_ref(), &[("a", "1"), ("b", "2"), ("c", "3")]).await;
        assert_eq!(store.get(b"b").unwrap(), Some(b("2")), "{name}");
        assert_eq!(
            store.multi_get(&[b"c", b"zz", b"a"]).unwrap(),
            vec![Some(b("3")), None, Some(b("1"))],
            "{name}"
        );

        let mut batch = Batch::new();
        batch.put(b("a"), b("10"));
        batch.delete(b("b"));
        batch.put(b("b"), b("20"));
        batch.delete(b("c"));
        store.write(batch).await.unwrap();
        assert_eq!(
            store.multi_get(&[b"a", b"b", b"c"]).unwrap(),
            vec![Some(b("10")), Some(b("20")), None],
            "{name}"
        );
        store.write(Batch::new()).await.unwrap();
        store.close().await.unwrap();
        drop(dir);
    })
    .await;
}

#[tokio::test]
async fn prefix_and_limit_scans_are_in_key_order() {
    for_each_engine(|name, store, _, dir| async move {
        put_all(
            store.as_ref(),
            &[
                ("user/2", "u2"),
                ("user/10", "u10"),
                ("user/1", "u1"),
                ("order/1", "o1"),
                ("user", "u"),
                ("userx", "ux"),
            ],
        )
        .await;
        assert_eq!(
            store.prefix_lookup(b"user/").unwrap(),
            vec![b("u1"), b("u10"), b("u2")],
            "{name}"
        );
        assert_eq!(
            store.prefix_lookup(b"user").unwrap(),
            vec![b("u"), b("u1"), b("u10"), b("u2"), b("ux")],
            "{name}"
        );
        assert!(store.prefix_lookup(b"nobody").unwrap().is_empty(), "{name}");
        assert_eq!(
            store.limit_scan(2).unwrap(),
            vec![b("o1"), b("u")],
            "{name}"
        );
        assert_eq!(store.limit_scan(100).unwrap().len(), 6, "{name}");
        assert!(store.limit_scan(0).unwrap().is_empty(), "{name}");
        store.close().await.unwrap();
        drop(dir);
    })
    .await;
}

#[tokio::test]
async fn snapshot_is_pinned_paged_and_in_key_order() {
    for_each_engine(|name, store, _, dir| async move {
        let mut batch = Batch::new();
        for i in 0..25u32 {
            batch.put(
                Bytes::from(i.to_be_bytes().to_vec()),
                Bytes::from(format!("v{i}")),
            );
        }
        batch.put(b("k"), b("short"));
        batch.put(Bytes::from_static(b"k\x00"), b("nul"));
        batch.put(b("k0"), b("long"));
        store.write(batch).await.unwrap();

        let mut snapshot = store.snapshot().unwrap();
        let mut later = Batch::new();
        later.put(Bytes::from(3u32.to_be_bytes().to_vec()), b("changed"));
        later.delete(Bytes::from(20u32.to_be_bytes().to_vec()));
        later.put(b("zzz"), b("new"));
        store.write(later).await.unwrap();

        let mut seen = Vec::new();
        loop {
            let page = snapshot.next_page(7).unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 7, "{name}");
            seen.extend(page);
        }
        assert!(snapshot.next_page(7).unwrap().is_empty(), "{name}");

        let mut expected: Vec<(Bytes, Bytes)> = (0..25u32)
            .map(|i| {
                (
                    Bytes::from(i.to_be_bytes().to_vec()),
                    Bytes::from(format!("v{i}")),
                )
            })
            .collect();
        expected.push((b("k"), b("short")));
        expected.push((Bytes::from_static(b"k\x00"), b("nul")));
        expected.push((b("k0"), b("long")));
        expected.sort();
        assert_eq!(seen, expected, "{name}");

        assert_eq!(store.get(b"zzz").unwrap(), Some(b("new")), "{name}");
        assert_eq!(
            store.snapshot().unwrap().next_page(1000).unwrap().len(),
            28,
            "{name}"
        );
        store.close().await.unwrap();
        drop(dir);
    })
    .await;
}

#[tokio::test]
async fn binary_keys_including_0xff_and_empty_prefix() {
    for_each_engine(|name, store, _, dir| async move {
        let mut batch = Batch::new();
        batch.put(Bytes::from_static(&[0xff, 0xff]), b("ff"));
        batch.put(Bytes::from_static(&[0xff]), b("f"));
        batch.put(Bytes::from_static(&[0x00]), b("zero"));
        batch.put(Bytes::from_static(&[0xff, 0x00]), b("f0"));
        store.write(batch).await.unwrap();
        assert_eq!(
            store.prefix_lookup(&[0xff]).unwrap(),
            vec![b("f"), b("f0"), b("ff")],
            "{name}"
        );
        assert_eq!(
            store.prefix_lookup(&[]).unwrap(),
            vec![b("zero"), b("f"), b("f0"), b("ff")],
            "{name}"
        );
        assert_eq!(store.limit_scan(10).unwrap().len(), 4, "{name}");
        store.close().await.unwrap();
        drop(dir);
    })
    .await;
}

#[tokio::test]
async fn batch_writer_flushes_at_capacity_and_on_close() {
    for_each_engine(|name, store, _, dir| async move {
        let mut writer = Writer::new(store.as_ref(), 0);
        for i in 0..BATCH_CAPACITY - 1 {
            writer.put(format!("k{i:04}"), b("v")).await.unwrap();
        }
        assert_eq!(writer.pending(), BATCH_CAPACITY - 1, "{name}");
        assert_eq!(store.get(b"k0000").unwrap(), None, "{name}");
        writer.put("last", b("v")).await.unwrap();
        assert_eq!(writer.pending(), 0, "{name}");
        assert_eq!(store.get(b"k0000").unwrap(), Some(b("v")), "{name}");

        writer.delete("k0000").await.unwrap();
        assert_eq!(store.get(b"k0000").unwrap(), Some(b("v")), "{name}");
        writer.close().await.unwrap();
        assert_eq!(store.get(b"k0000").unwrap(), None, "{name}");

        let mut sized = Writer::new(store.as_ref(), 8);
        sized.put("abcd", "efgh").await.unwrap();
        assert_eq!(sized.pending(), 0, "{name}");
        assert_eq!(store.get(b"abcd").unwrap(), Some(b("efgh")), "{name}");
        store.close().await.unwrap();
        drop(dir);
    })
    .await;
}

#[tokio::test]
async fn checkpoint_restores_into_a_fresh_store() {
    for_each_engine(|name, store, engine, dir| async move {
        put_all(store.as_ref(), &[("a", "1"), ("b", "2")]).await;
        let checkpoint_dir = dir.path().join("checkpoint");
        let checkpoint = store.checkpoint(&checkpoint_dir).unwrap();
        assert!(!checkpoint.files.is_empty(), "{name}");
        assert!(checkpoint.total_size() > 0, "{name}");
        assert!(
            checkpoint
                .files
                .iter()
                .all(|f| checkpoint_dir.join(&f.path).is_file()),
            "{name}"
        );

        put_all(store.as_ref(), &[("c", "3")]).await;
        store.close().await.unwrap();
        assert!(matches!(store.get(b"a"), Err(Error::Closed)), "{name}");

        let restored = engine
            .restore(&dir.path().join("restored"), &checkpoint_dir, options())
            .unwrap();
        assert_eq!(
            restored.multi_get(&[b"a", b"b", b"c"]).unwrap(),
            vec![Some(b("1")), Some(b("2")), None],
            "{name}"
        );
        put_all(restored.as_ref(), &[("d", "4")]).await;
        assert_eq!(restored.limit_scan(10).unwrap().len(), 3, "{name}");
        restored.close().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn checkpoint_restores_without_its_empty_directories() {
    for_each_engine(|name, store, engine, dir| async move {
        put_all(store.as_ref(), &[("a", "1"), ("b", "2")]).await;
        let checkpoint_dir = dir.path().join("checkpoint");
        let checkpoint = store.checkpoint(&checkpoint_dir).unwrap();
        store.close().await.unwrap();

        let stripped = dir.path().join("downloaded");
        for file in &checkpoint.files {
            let to = stripped.join(&file.path);
            fs::create_dir_all(to.parent().unwrap()).unwrap();
            fs::copy(checkpoint_dir.join(&file.path), to).unwrap();
        }

        let restored = engine
            .restore(&dir.path().join("restored"), &stripped, options())
            .unwrap();
        assert_eq!(
            restored.multi_get(&[b"a", b"b"]).unwrap(),
            vec![Some(b("1")), Some(b("2"))],
            "{name}"
        );
        put_all(restored.as_ref(), &[("c", "3")]).await;
        assert_eq!(restored.limit_scan(10).unwrap().len(), 3, "{name}");
        restored.close().await.unwrap();
    })
    .await;
}

#[cfg(feature = "surrealkv")]
#[tokio::test]
async fn surrealkv_checkpoint_has_shared_table_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = mink_kv::SurrealEngine
        .open(&dir.path().join("kv"), options())
        .unwrap();
    put_all(store.as_ref(), &[("a", "1")]).await;
    let checkpoint = store.checkpoint(&dir.path().join("cp")).unwrap();
    assert!(checkpoint.shared_files().count() >= 1);
    assert!(
        checkpoint
            .shared_files()
            .all(|f| f.path.extension().is_some_and(|e| e == "sst"))
    );
    assert!(checkpoint.private_files().count() >= 1);
    store.close().await.unwrap();
    assert_eq!(checkpoint.dir, dir.path().join("cp"));
}
