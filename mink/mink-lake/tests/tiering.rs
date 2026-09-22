//! Tiering rounds end to end against an in-memory Iceberg catalog: log tables, primary-key tables, compaction.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::TryStreamExt;
use iceberg::spec::{DataContentType, FormatVersion, Literal, Operation, PrimitiveLiteral};
use iceberg::table::Table;
use mink_coordinator::LakeCatalog;
use mink_lake::iceberg::{
    COMMIT_USER_PROPERTY, Catalog, Committable, Config, FORMAT_VERSION_OPTION, RowDelta, Source,
    WriteResult, commit, partition_key, produce,
};
use mink_lake::{
    COMMIT_USER, CommitterContext, Error, Factory, ScanOptions, TieredBatch, WriterContext,
};
use mink_record::{ChangeType, Changes};
use mink_table::{
    Bucket, BucketId, Column, Descriptor, Id, LakeFormat, Options, PartitionName, Path, PrimaryKey,
    Schema,
};
use mink_types::DataType;
use tempfile::TempDir;

fn path(s: &str) -> Path {
    s.parse().unwrap()
}

fn lake_options() -> Options {
    Options {
        lake: Some(LakeFormat::Iceberg),
        ..Options::default()
    }
}

fn schema(primary_key: bool) -> Schema {
    let builder = Schema::builder()
        .column(Column::new("k", DataType::big_int()).unwrap())
        .column(Column::new("v", DataType::string()).unwrap());
    if primary_key {
        builder
            .primary_key(PrimaryKey::new(vec!["k".into()]).unwrap())
            .build()
            .unwrap()
    } else {
        builder.build().unwrap()
    }
}

fn rows(kv: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("k", ArrowType::Int64, false),
            ArrowField::new("v", ArrowType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(kv.iter().map(|(k, _)| *k))),
            Arc::new(StringArray::from_iter_values(kv.iter().map(|(_, v)| *v))),
        ],
    )
    .unwrap()
}

fn appends(kv: &[(i64, &str)], base_offset: i64) -> TieredBatch {
    TieredBatch {
        changes: Changes::AppendOnly(kv.len()),
        rows: rows(kv),
        base_offset,
        timestamp_ms: 1_700_000_000_000 + base_offset,
    }
}

fn changes(kv: &[(ChangeType, i64, &str)], base_offset: i64) -> TieredBatch {
    let bytes: Vec<u8> = kv.iter().map(|(c, _, _)| c.byte()).collect();
    let rows: Vec<(i64, &str)> = kv.iter().map(|(_, k, v)| (*k, *v)).collect();
    TieredBatch {
        changes: Changes::vector(Bytes::from(bytes)).unwrap(),
        rows: self::rows(&rows),
        base_offset,
        timestamp_ms: 1_700_000_000_000 + base_offset,
    }
}

struct Lake {
    _dir: TempDir,
    catalog: Catalog,
}

impl Lake {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = format!("file://{}", dir.path().display());
        let catalog = Catalog::connect(&Config::memory(warehouse)).await.unwrap();
        Lake { _dir: dir, catalog }
    }

    async fn create(&self, path: &Path, descriptor: &Descriptor) {
        LakeCatalog::create_table(&self.catalog, path, descriptor)
            .await
            .unwrap();
    }

    async fn table(&self, path: &Path) -> Table {
        self.catalog
            .target()
            .load(&Catalog::identifier(path))
            .await
            .unwrap()
    }

    async fn write_bucket(
        &self,
        path: &Path,
        descriptor: &Arc<Descriptor>,
        bucket: u32,
        batches: &[TieredBatch],
    ) -> WriteResult {
        let mut writer = self
            .catalog
            .create_writer(WriterContext {
                path: path.clone(),
                bucket: Bucket::new(Id(1), BucketId(bucket)),
                partition: None,
                descriptor: descriptor.clone(),
            })
            .await
            .unwrap();
        for batch in batches {
            writer.write(batch).await.unwrap();
        }
        writer.complete().await.unwrap()
    }

    async fn commit(
        &self,
        path: &Path,
        descriptor: &Arc<Descriptor>,
        results: Vec<WriteResult>,
        offsets: &str,
    ) -> i64 {
        let mut committer = self
            .catalog
            .create_committer(CommitterContext {
                path: path.clone(),
                descriptor: descriptor.clone(),
            })
            .await
            .unwrap();
        let committable = committer.to_committable(results).await.unwrap();
        let mut properties = BTreeMap::new();
        properties.insert("mink-offsets".to_string(), offsets.to_string());
        let result = committer.commit(committable, properties).await.unwrap();
        assert!(result.committed_is_readable());
        result.committed_snapshot_id
    }
}

async fn scan(table: &Table) -> Vec<(i64, String)> {
    let stream = table.scan().build().unwrap().to_arrow().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut out = Vec::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), 2, "user columns only");
        let k = batch
            .column_by_name("k")
            .unwrap()
            .as_primitive::<Int64Type>();
        let v = batch.column_by_name("v").unwrap().as_string::<i32>();
        for i in 0..batch.num_rows() {
            out.push((k.value(i), v.value(i).to_string()));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn log_table_rounds_append_user_rows_only() {
    let lake = Lake::new().await;
    let orders = path("sales.orders");
    let descriptor = Arc::new(
        Descriptor::builder(schema(false))
            .bucket_count(2)
            .options(lake_options())
            .build()
            .unwrap(),
    );
    lake.create(&orders, &descriptor).await;

    let b0 = lake
        .write_bucket(
            &orders,
            &descriptor,
            0,
            &[appends(&[(1, "a"), (2, "b")], 0), appends(&[(3, "c")], 2)],
        )
        .await;
    let b1 = lake
        .write_bucket(&orders, &descriptor, 1, &[appends(&[(10, "x")], 0)])
        .await;
    assert_eq!(b0.data_files.len(), 1, "one rolling file per bucket round");
    assert!(b0.delete_files.is_empty(), "log tables never delete");
    assert_eq!(b0.data_files[0].record_count(), 3);
    assert!(
        b0.data_files[0].partition().fields().is_empty(),
        "keyless tables are unpartitioned in the lake"
    );
    assert!(b1.data_files[0].partition().fields().is_empty());

    let first = lake
        .commit(
            &orders,
            &descriptor,
            vec![b0, b1],
            "[{\"bucket\":0,\"log_offset\":3}]",
        )
        .await;

    let table = lake.table(&orders).await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.snapshot_id(), first);
    assert_eq!(snapshot.sequence_number(), 1);
    assert_eq!(snapshot.summary().operation, Operation::Append);
    let props = &snapshot.summary().additional_properties;
    assert_eq!(props[COMMIT_USER_PROPERTY], COMMIT_USER);
    assert_eq!(props["mink-offsets"], "[{\"bucket\":0,\"log_offset\":3}]");
    assert_eq!(props["added-data-files"], "2");
    assert_eq!(props["added-records"], "4");
    assert_eq!(props["total-records"], "4");
    assert_eq!(props["total-data-files"], "2");

    assert_eq!(
        scan(&table).await,
        vec![
            (1, "a".into()),
            (2, "b".into()),
            (3, "c".into()),
            (10, "x".into()),
        ]
    );

    let b0 = lake
        .write_bucket(&orders, &descriptor, 0, &[appends(&[(4, "d")], 3)])
        .await;
    let second = lake.commit(&orders, &descriptor, vec![b0], "r2").await;
    let table = lake.table(&orders).await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.snapshot_id(), second);
    assert_eq!(snapshot.parent_snapshot_id(), Some(first));
    assert_eq!(snapshot.sequence_number(), 2);
    let props = &snapshot.summary().additional_properties;
    assert_eq!(props["added-records"], "1");
    assert_eq!(props["total-records"], "5");
    assert_eq!(props["total-data-files"], "3");
    assert_eq!(scan(&table).await.len(), 5);
    assert_eq!(table.metadata().snapshots().count(), 2);
}

#[tokio::test]
async fn primary_key_rounds_write_the_last_row_per_key_and_equality_deletes() {
    let lake = Lake::new().await;
    let users = path("crm.users");
    let descriptor = Arc::new(
        Descriptor::builder(schema(true))
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    );
    lake.create(&users, &descriptor).await;

    let round1 = lake
        .write_bucket(
            &users,
            &descriptor,
            0,
            &[
                changes(
                    &[
                        (ChangeType::Insert, 1, "one"),
                        (ChangeType::Insert, 2, "two"),
                        (ChangeType::Insert, 3, "three"),
                    ],
                    0,
                ),
                changes(
                    &[
                        (ChangeType::UpdateBefore, 2, "two"),
                        (ChangeType::UpdateAfter, 2, "TWO"),
                        (ChangeType::Delete, 3, "three"),
                    ],
                    3,
                ),
            ],
        )
        .await;
    assert_eq!(round1.data_files.len(), 1);
    let data = &round1.data_files[0];
    assert_eq!(data.content_type(), DataContentType::Data);
    assert_eq!(data.record_count(), 2, "only the surviving rows 1 and 2");
    assert!(
        round1.delete_files.is_empty(),
        "every key was first inserted this round: nothing older to retract, \
         so no delete file and nothing for readers to apply"
    );
    assert_eq!(
        data.partition().fields(),
        &[Some(Literal::Primitive(PrimitiveLiteral::Int(0)))],
        "keyed tables partition by bucket(k)"
    );

    let first = lake.commit(&users, &descriptor, vec![round1], "r1").await;
    let table = lake.table(&users).await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.summary().operation, Operation::Append);
    assert_eq!(
        scan(&table).await,
        vec![(1, "one".into()), (2, "TWO".into()),],
        "the updated row carries the offset of its last version"
    );

    let round2 = lake
        .write_bucket(
            &users,
            &descriptor,
            0,
            &[changes(
                &[
                    (ChangeType::UpdateBefore, 1, "one"),
                    (ChangeType::UpdateAfter, 1, "ONE"),
                    (ChangeType::Delete, 2, "TWO"),
                    (ChangeType::Insert, 4, "four"),
                ],
                6,
            )],
        )
        .await;
    assert_eq!(round2.data_files[0].record_count(), 2, "1 and 4");
    assert_eq!(round2.delete_files.len(), 1);
    let deletes = &round2.delete_files[0];
    assert_eq!(deletes.content_type(), DataContentType::EqualityDeletes);
    assert_eq!(deletes.record_count(), 2, "keys 1 and 2, not 4");
    let k_id = table
        .metadata()
        .current_schema()
        .field_id_by_name("k")
        .unwrap();
    assert_eq!(k_id, 1);
    assert_eq!(deletes.equality_ids(), Some(vec![k_id]));

    let second = lake.commit(&users, &descriptor, vec![round2], "r2").await;
    let table = lake.table(&users).await;
    let snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.summary().operation, Operation::Overwrite);
    let props = &snapshot.summary().additional_properties;
    assert_eq!(props["added-delete-files"], "1");
    assert_eq!(props["added-equality-deletes"], "2");
    assert_eq!(props["total-equality-deletes"], "2");
    assert_eq!(
        scan(&table).await,
        vec![(1, "ONE".into()), (4, "four".into()),]
    );
    assert_ne!(first, second);
    assert_eq!(
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id(),
        Some(first)
    );

    let round3 = lake
        .write_bucket(
            &users,
            &descriptor,
            0,
            &[changes(&[(ChangeType::Delete, 4, "four")], 10)],
        )
        .await;
    assert!(round3.data_files.is_empty());
    assert_eq!(round3.delete_files.len(), 1);
    assert_eq!(round3.delete_files[0].record_count(), 1);
    lake.commit(&users, &descriptor, vec![round3], "r3").await;
    assert_eq!(
        scan(&lake.table(&users).await).await,
        vec![(1, "ONE".into())]
    );
}

#[tokio::test]
async fn v3_tables_write_read_and_delete_like_v2_and_v2_upgrades_to_v3() {
    let lake = Lake::new().await;
    let users = path("crm.users_v3");
    let descriptor = Arc::new(
        Descriptor::builder(schema(true))
            .bucket_count(1)
            .options(lake_options())
            .custom(FORMAT_VERSION_OPTION, "3")
            .build()
            .unwrap(),
    );
    lake.create(&users, &descriptor).await;
    let table = lake.table(&users).await;
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);

    let round1 = lake
        .write_bucket(
            &users,
            &descriptor,
            0,
            &[changes(
                &[
                    (ChangeType::Insert, 1, "one"),
                    (ChangeType::Insert, 2, "two"),
                ],
                0,
            )],
        )
        .await;
    assert_eq!(round1.context.format_version, FormatVersion::V3);
    lake.commit(&users, &descriptor, vec![round1], "r1").await;
    let round2 = lake
        .write_bucket(
            &users,
            &descriptor,
            0,
            &[changes(
                &[
                    (ChangeType::Delete, 1, "one"),
                    (ChangeType::Insert, 3, "three"),
                ],
                2,
            )],
        )
        .await;
    assert_eq!(round2.delete_files.len(), 1);
    lake.commit(&users, &descriptor, vec![round2], "r2").await;
    let table = lake.table(&users).await;
    assert_eq!(
        scan(&table).await,
        vec![(2, "two".into()), (3, "three".into())]
    );
    let source = Source::open(&lake.catalog, &users).await.unwrap();
    let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
    let splits = mink_lake::Source::plan(&source, snapshot_id, None)
        .await
        .unwrap();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].bucket, Some(BucketId(0)));

    let orders = path("sales.orders_v2");
    let v2 = Arc::new(
        Descriptor::builder(schema(false))
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    );
    lake.create(&orders, &v2).await;
    let b0 = lake
        .write_bucket(&orders, &v2, 0, &[appends(&[(1, "a")], 0)])
        .await;
    lake.commit(&orders, &v2, vec![b0], "r1").await;
    let v3 = Arc::new(
        v2.to_builder()
            .custom(FORMAT_VERSION_OPTION, "3")
            .build()
            .unwrap(),
    );
    lake.catalog.alter(&orders, &v2, &v3).await.unwrap();
    let table = lake.table(&orders).await;
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    let b0 = lake
        .write_bucket(&orders, &v3, 0, &[appends(&[(2, "b")], 1)])
        .await;
    lake.commit(&orders, &v3, vec![b0], "r2").await;
    assert_eq!(
        scan(&lake.table(&orders).await).await,
        vec![(1, "a".into()), (2, "b".into())]
    );

    let err = lake.catalog.alter(&orders, &v3, &v2).await.unwrap_err();
    assert!(err.to_string().contains("cannot go from"), "{err}");
}

#[tokio::test]
async fn source_reads_an_older_snapshot_under_the_evolved_schema() {
    let lake = Lake::new().await;
    let orders = path("sales.orders");
    let v1 = Arc::new(
        Descriptor::builder(schema(false))
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    );
    lake.create(&orders, &v1).await;
    let b0 = lake
        .write_bucket(&orders, &v1, 0, &[appends(&[(1, "a"), (2, "b")], 0)])
        .await;
    let before = lake.commit(&orders, &v1, vec![b0], "r1").await;

    let v2 = Arc::new(
        Descriptor::builder(
            Schema::builder()
                .column(Column::new("k", DataType::big_int()).unwrap())
                .column(Column::new("v", DataType::string()).unwrap())
                .column(Column::new("w", DataType::int()).unwrap())
                .build()
                .unwrap(),
        )
        .bucket_count(1)
        .options(lake_options())
        .build()
        .unwrap(),
    );
    lake.catalog.alter(&orders, &v1, &v2).await.unwrap();

    let source = Source::open(&lake.catalog, &orders).await.unwrap();
    assert_eq!(source.user_schema().unwrap().fields().len(), 3);
    let splits = mink_lake::Source::plan(&source, before, None)
        .await
        .unwrap();
    assert_eq!(splits.len(), 1);
    let batches: Vec<RecordBatch> = mink_lake::Source::read(
        &source,
        splits.into_iter().next().unwrap(),
        ScanOptions::default(),
    )
    .await
    .unwrap()
    .try_collect()
    .await
    .unwrap();
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(batch.num_columns(), 3, "user columns only");
    assert_eq!(batch.schema().field(2).name(), "w");
    assert_eq!(
        batch.column(0).as_primitive::<Int64Type>().values(),
        &[1, 2]
    );
    assert_eq!(batch.column(2).null_count(), 2, "the new column reads null");

    let batches: Vec<RecordBatch> = mink_lake::Source::read(
        &source,
        mink_lake::Source::plan(&source, before, None)
            .await
            .unwrap()
            .remove(0),
        ScanOptions {
            projection: Some(vec![2, 0]),
            limit: None,
        },
    )
    .await
    .unwrap()
    .try_collect()
    .await
    .unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "w");
    assert_eq!(batches[0].schema().field(1).name(), "k");
}

#[tokio::test]
async fn missing_snapshot_finds_tiering_commits_newer_than_recorded() {
    let lake = Lake::new().await;
    let orders = path("sales.orders");
    let descriptor = Arc::new(
        Descriptor::builder(schema(false))
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    );
    lake.create(&orders, &descriptor).await;
    let mut committer = lake
        .catalog
        .create_committer(CommitterContext {
            path: orders.clone(),
            descriptor: descriptor.clone(),
        })
        .await
        .unwrap();

    assert_eq!(committer.missing_snapshot(None).await.unwrap(), None);

    let b0 = lake
        .write_bucket(&orders, &descriptor, 0, &[appends(&[(1, "a")], 0)])
        .await;
    let first = lake.commit(&orders, &descriptor, vec![b0], "r1").await;

    let missing = committer.missing_snapshot(None).await.unwrap().unwrap();
    assert_eq!(missing.snapshot_id, first);
    assert_eq!(missing.properties["mink-offsets"], "r1");
    assert_eq!(missing.properties[COMMIT_USER_PROPERTY], COMMIT_USER);

    assert_eq!(committer.missing_snapshot(Some(first)).await.unwrap(), None);

    let table = lake.table(&orders).await;
    let b0 = lake
        .write_bucket(&orders, &descriptor, 0, &[appends(&[(2, "b")], 1)])
        .await;
    let foreign = commit(
        lake.catalog.target().as_ref(),
        table.identifier(),
        &RowDelta {
            data_files: b0.data_files,
            delete_files: Vec::new(),
            properties: Default::default(),
        },
    )
    .await
    .unwrap()
    .1;
    assert_ne!(foreign, first);
    assert_eq!(committer.missing_snapshot(Some(first)).await.unwrap(), None);
    let missing = committer.missing_snapshot(None).await.unwrap().unwrap();
    assert_eq!(missing.snapshot_id, first, "latest *tiering* snapshot");

    let b0 = lake
        .write_bucket(&orders, &descriptor, 0, &[appends(&[(3, "c")], 2)])
        .await;
    let second = lake.commit(&orders, &descriptor, vec![b0], "r2").await;
    let missing = committer
        .missing_snapshot(Some(first))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(missing.snapshot_id, second);
    assert_eq!(missing.properties["mink-offsets"], "r2");

    let err = committer.missing_snapshot(Some(424242)).await.unwrap_err();
    assert!(
        err.to_string().contains("not found in Iceberg table"),
        "{err}"
    );
}

#[tokio::test]
async fn conflicting_commits_are_retried_on_a_fresh_table() {
    let lake = Lake::new().await;
    let orders = path("sales.orders");
    let descriptor = Arc::new(
        Descriptor::builder(schema(false))
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    );
    lake.create(&orders, &descriptor).await;
    let target = lake.catalog.target();
    let ident = Catalog::identifier(&orders);
    let stale = target.load(&ident).await.unwrap();

    let other = lake
        .write_bucket(&orders, &descriptor, 0, &[appends(&[(1, "a")], 0)])
        .await;
    let (_, first) = commit(
        target.as_ref(),
        &ident,
        &RowDelta {
            data_files: other.data_files,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let ours = lake
        .write_bucket(&orders, &descriptor, 0, &[appends(&[(2, "b")], 1)])
        .await;
    let delta = RowDelta {
        data_files: ours.data_files,
        ..Default::default()
    };
    let produced = produce(&stale, &delta).await.unwrap();
    let err = target
        .commit(&ident, produced.requirements, produced.updates)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::CommitConflict(_)), "{err}");

    let (table, second) = commit(target.as_ref(), &ident, &delta).await.unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.snapshot_id(), second);
    assert_eq!(snapshot.parent_snapshot_id(), Some(first));
    assert_eq!(scan(&table).await.len(), 2);
}

#[tokio::test]
async fn abort_removes_the_round_files() {
    let lake = Lake::new().await;
    let users = path("crm.users");
    let descriptor = Arc::new(
        Descriptor::builder(schema(true))
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    );
    lake.create(&users, &descriptor).await;
    let result = lake
        .write_bucket(
            &users,
            &descriptor,
            0,
            &[changes(
                &[
                    (ChangeType::Insert, 1, "one"),
                    (ChangeType::Delete, 2, "two"),
                ],
                0,
            )],
        )
        .await;
    let table = lake.table(&users).await;
    let paths: Vec<String> = result
        .data_files
        .iter()
        .chain(&result.delete_files)
        .map(|f| f.file_path().to_string())
        .collect();
    assert_eq!(paths.len(), 2);
    for p in &paths {
        assert!(table.file_io().exists(p).await.unwrap(), "{p}");
    }

    let mut committer = lake
        .catalog
        .create_committer(CommitterContext {
            path: users.clone(),
            descriptor: descriptor.clone(),
        })
        .await
        .unwrap();
    let committable = committer.to_committable(vec![result]).await.unwrap();
    committer.abort(committable).await.unwrap();
    for p in &paths {
        assert!(!table.file_io().exists(p).await.unwrap(), "{p}");
    }
    assert!(table.metadata().current_snapshot().is_none());
}

#[tokio::test]
async fn write_results_round_trip_through_json() {
    let lake = Lake::new().await;
    let users = path("crm.users");
    let descriptor = Arc::new(
        Descriptor::builder(schema(true))
            .bucket_count(1)
            .options(lake_options())
            .build()
            .unwrap(),
    );
    lake.create(&users, &descriptor).await;
    let result = lake
        .write_bucket(
            &users,
            &descriptor,
            0,
            &[changes(&[(ChangeType::Insert, 1, "one")], 0)],
        )
        .await;

    let json = serde_json::to_string(&result).unwrap();
    let back: WriteResult = serde_json::from_str(&json).unwrap();
    assert_eq!(back, result);

    let committable = Committable::from_results(vec![result.clone(), result]);
    assert_eq!(committable.data_files.len(), 2);
    let json = serde_json::to_string(&committable).unwrap();
    let back: Committable = serde_json::from_str(&json).unwrap();
    assert_eq!(back, committable);

    let empty = Committable::from_results(vec![]);
    assert!(empty.is_empty());
    let json = serde_json::to_string(&empty).unwrap();
    let back: Committable = serde_json::from_str(&json).unwrap();
    assert_eq!(back, empty);
}

#[tokio::test]
async fn partition_keys_follow_the_spec_field_order() {
    let lake = Lake::new().await;
    let orders = path("sales.orders");
    let descriptor = Descriptor::builder(
        Schema::builder()
            .column(Column::new("k", DataType::big_int()).unwrap())
            .column(Column::new("region", DataType::string()).unwrap())
            .primary_key(PrimaryKey::new(vec!["k".into(), "region".into()]).unwrap())
            .build()
            .unwrap(),
    )
    .partitioned_by(["region"])
    .bucket_count(4)
    .options(lake_options())
    .build()
    .unwrap();
    lake.create(&orders, &descriptor).await;
    let table = lake.table(&orders).await;
    let metadata = table.metadata();
    let spec = metadata.default_partition_spec();
    let schema = metadata.current_schema().clone();

    let eu: PartitionName = "eu".parse().unwrap();
    let key = partition_key(spec, schema.clone(), Some(&eu), 3).unwrap();
    assert_eq!(
        key.data().fields(),
        &[
            Some(Literal::Primitive(PrimitiveLiteral::String("eu".into()))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(3))),
        ]
    );

    let err = partition_key(spec, schema.clone(), None, 3).unwrap_err();
    assert!(err.to_string().contains("has no partition"), "{err}");
    let two: PartitionName = "eu$us".parse().unwrap();
    let err = partition_key(spec, schema, Some(&two), 3).unwrap_err();
    assert!(err.to_string().contains("more values"), "{err}");
}
