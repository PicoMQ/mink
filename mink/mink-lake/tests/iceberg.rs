//! Catalog, writer and reader behavior against an in-memory Iceberg catalog.

use std::collections::HashMap;

use iceberg::spec::{FormatVersion, PrimitiveType, Transform, Type};
use mink_coordinator::{LakeCatalog, lake};
use mink_lake::Error;
use mink_lake::iceberg::{Catalog, Config, FORMAT_VERSION_OPTION, Spec};
use mink_table::{Column, Descriptor, LakeFormat, MergeEngine, Options, Path, PrimaryKey, Schema};
use mink_types::{DataType, Decimal, Field, Fields, Precision};

fn path(s: &str) -> Path {
    s.parse().unwrap()
}

fn lake_options() -> Options {
    Options {
        lake: Some(LakeFormat::Iceberg),
        ..Options::default()
    }
}

fn pk_schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int()).unwrap())
        .column(
            Column::new("region", DataType::string())
                .unwrap()
                .with_description("where"),
        )
        .column(Column::new("v", DataType::string()).unwrap())
        .primary_key(PrimaryKey::new(vec!["k".into(), "region".into()]).unwrap())
        .build()
        .unwrap()
}

fn log_schema() -> Schema {
    Schema::builder()
        .column(Column::new("k", DataType::big_int()).unwrap())
        .column(Column::new("region", DataType::string()).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .build()
        .unwrap()
}

fn primitive(t: &Type) -> &PrimitiveType {
    match t {
        Type::Primitive(p) => p,
        other => panic!("expected primitive, got {other:?}"),
    }
}

#[test]
fn schema_keeps_column_order_ids_and_types() {
    let nested = Fields::new(vec![
        Field::new("a", DataType::int()).unwrap(),
        Field::new("b", DataType::string().with_nullable(false)).unwrap(),
    ])
    .unwrap();
    let schema = Schema::builder()
        .column(Column::new("id", DataType::big_int()).unwrap())
        .column(Column::new("tags", DataType::array(DataType::string())).unwrap())
        .column(
            Column::new(
                "attrs",
                DataType::map(DataType::string(), DataType::int().with_nullable(false)),
            )
            .unwrap(),
        )
        .column(Column::new("nested", DataType::row(nested)).unwrap())
        .column(Column::new("tiny", DataType::tiny_int()).unwrap())
        .column(Column::new("small", DataType::small_int()).unwrap())
        .column(Column::new("amount", DataType::decimal(Decimal::new(10, 2).unwrap())).unwrap())
        .column(Column::new("ts", DataType::timestamp(Precision::MILLIS)).unwrap())
        .column(Column::new("ts_ltz", DataType::timestamp_ltz(Precision::MICROS)).unwrap())
        .column(Column::new("t", DataType::time(Precision::SECONDS)).unwrap())
        .column(Column::new("blob", DataType::bytes()).unwrap())
        .column(Column::new("flag", DataType::boolean()).unwrap())
        .primary_key(PrimaryKey::new(vec!["id".into()]).unwrap())
        .build()
        .unwrap();
    let descriptor = Descriptor::builder(schema)
        .bucket_count(2)
        .options(lake_options())
        .build()
        .unwrap();
    let spec = Spec::new(&path("db.t"), &descriptor).unwrap();
    let fields = spec.schema.as_struct().fields();

    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "id", "tags", "attrs", "nested", "tiny", "small", "amount", "ts", "ts_ltz", "t",
            "blob", "flag",
        ]
    );
    let ids: Vec<i32> = fields.iter().map(|f| f.id).collect();
    assert_eq!(ids, (0..12).collect::<Vec<_>>());

    assert!(fields[0].required);
    assert_eq!(spec.schema.identifier_field_ids().collect::<Vec<_>>(), [0]);
    assert!(!fields[1].required, "nullable column stays optional");

    match fields[1].field_type.as_ref() {
        Type::List(list) => {
            assert_eq!(list.element_field.id, 12);
            assert!(!list.element_field.required);
            assert_eq!(
                primitive(&list.element_field.field_type),
                &PrimitiveType::String
            );
        }
        other => panic!("tags: {other:?}"),
    }
    match fields[2].field_type.as_ref() {
        Type::Map(map) => {
            assert_eq!((map.key_field.id, map.value_field.id), (13, 14));
            assert!(map.value_field.required);
            assert_eq!(primitive(&map.value_field.field_type), &PrimitiveType::Int);
        }
        other => panic!("attrs: {other:?}"),
    }
    match fields[3].field_type.as_ref() {
        Type::Struct(row) => {
            let ids: Vec<(i32, &str, bool)> = row
                .fields()
                .iter()
                .map(|f| (f.id, f.name.as_str(), f.required))
                .collect();
            assert_eq!(ids, [(15, "a", false), (16, "b", true)]);
        }
        other => panic!("nested: {other:?}"),
    }

    let types: Vec<&PrimitiveType> = fields[4..12]
        .iter()
        .map(|f| primitive(&f.field_type))
        .collect();
    assert_eq!(
        types,
        [
            &PrimitiveType::Int,
            &PrimitiveType::Int,
            &PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            },
            &PrimitiveType::Timestamp,
            &PrimitiveType::Timestamptz,
            &PrimitiveType::Time,
            &PrimitiveType::Binary,
            &PrimitiveType::Boolean,
        ]
    );
    assert_eq!(spec.format_version, FormatVersion::V2);
    assert!(spec.sort_order.fields.is_empty(), "no system sort order");
}

#[test]
fn format_version_option_selects_v3_and_nanosecond_timestamps() {
    let schema = Schema::builder()
        .column(Column::new("id", DataType::big_int()).unwrap())
        .column(Column::new("ts", DataType::timestamp(Precision::NANOS)).unwrap())
        .column(Column::new("ts_ltz", DataType::timestamp_ltz(Precision::NANOS)).unwrap())
        .build()
        .unwrap();
    let v3 = Descriptor::builder(schema.clone())
        .bucket_count(1)
        .options(lake_options())
        .custom(FORMAT_VERSION_OPTION, "3")
        .build()
        .unwrap();
    let spec = Spec::new(&path("db.t"), &v3).unwrap();
    assert_eq!(spec.format_version, FormatVersion::V3);
    let fields = spec.schema.as_struct().fields();
    assert_eq!(
        primitive(&fields[1].field_type),
        &PrimitiveType::TimestampNs
    );
    assert_eq!(
        primitive(&fields[2].field_type),
        &PrimitiveType::TimestamptzNs
    );
    assert!(
        !spec.properties.contains_key("format-version"),
        "the version is table metadata, not a property"
    );

    let v2 = Descriptor::builder(schema)
        .bucket_count(1)
        .options(lake_options())
        .build()
        .unwrap();
    let spec = Spec::new(&path("db.t"), &v2).unwrap();
    assert_eq!(spec.format_version, FormatVersion::V2);
    let fields = spec.schema.as_struct().fields();
    assert_eq!(
        primitive(&fields[1].field_type),
        &PrimitiveType::Timestamp,
        "v2 has no nanosecond type: micros"
    );

    let bad = Descriptor::builder(pk_schema())
        .bucket_keys(["k"])
        .bucket_count(1)
        .options(lake_options())
        .custom(FORMAT_VERSION_OPTION, "1")
        .build()
        .unwrap();
    let err = Spec::new(&path("db.t"), &bad).unwrap_err();
    assert!(
        matches!(&err, Error::Invalid(m) if m.contains("must be 2 or 3")),
        "{err}"
    );
}

#[test]
fn column_descriptions_become_docs() {
    let descriptor = Descriptor::builder(pk_schema())
        .bucket_keys(["k"])
        .bucket_count(1)
        .options(lake_options())
        .build()
        .unwrap();
    let spec = Spec::new(&path("db.t"), &descriptor).unwrap();
    assert_eq!(
        spec.schema.field_by_name("region").unwrap().doc.as_deref(),
        Some("where")
    );
}

#[test]
fn partition_spec_buckets_keyed_tables_and_leaves_keyless_ones_unbucketed() {
    let pk = Descriptor::builder(pk_schema())
        .partitioned_by(["region"])
        .bucket_count(8)
        .options(lake_options())
        .build()
        .unwrap();
    let spec = Spec::new(&path("db.pk"), &pk).unwrap();
    let fields: Vec<(i32, &str, &Transform)> = spec
        .partition_spec
        .fields()
        .iter()
        .map(|f| (f.source_id, f.name.as_str(), &f.transform))
        .collect();
    assert_eq!(
        fields,
        [
            (1, "region", &Transform::Identity),
            (0, "k_bucket", &Transform::Bucket(8)),
        ]
    );

    let keyed = Descriptor::builder(log_schema())
        .bucket_keys(["region"])
        .bucket_count(3)
        .options(lake_options())
        .build()
        .unwrap();
    let spec = Spec::new(&path("db.keyed"), &keyed).unwrap();
    let f = &spec.partition_spec.fields()[0];
    assert_eq!((f.source_id, f.name.as_str()), (1, "region_bucket"));
    assert_eq!(f.transform, Transform::Bucket(3));

    let keyless = Descriptor::builder(log_schema())
        .bucket_count(3)
        .options(lake_options())
        .build()
        .unwrap();
    let spec = Spec::new(&path("db.keyless"), &keyless).unwrap();
    assert!(
        spec.partition_spec.fields().is_empty(),
        "no bucket key: nothing to bucket by in the lake"
    );
    assert!(spec.sort_order.fields.is_empty());

    let partitioned_keyless = Descriptor::builder(log_schema())
        .partitioned_by(["region"])
        .bucket_count(3)
        .options(lake_options())
        .build()
        .unwrap();
    let spec = Spec::new(&path("db.keyless"), &partitioned_keyless).unwrap();
    let fields: Vec<(i32, &str, &Transform)> = spec
        .partition_spec
        .fields()
        .iter()
        .map(|f| (f.source_id, f.name.as_str(), &f.transform))
        .collect();
    assert_eq!(fields, [(1, "region", &Transform::Identity)]);
}

#[test]
fn partition_spec_rejects_unsupported_keys() {
    let multi = Descriptor::builder(log_schema())
        .bucket_keys(["k", "region"])
        .bucket_count(1)
        .options(lake_options())
        .build()
        .unwrap();
    let err = Spec::new(&path("db.t"), &multi).unwrap_err();
    assert!(
        matches!(&err, Error::Invalid(m) if m.contains("Only one bucket key")),
        "{err}"
    );

    let numeric_partition = Descriptor::builder(log_schema())
        .partitioned_by(["k"])
        .bucket_count(1)
        .options(lake_options())
        .build()
        .unwrap();
    let err = Spec::new(&path("db.t"), &numeric_partition).unwrap_err();
    assert!(
        matches!(&err, Error::Invalid(m) if m.contains("column `k` is not")),
        "{err}"
    );

    let no_buckets = Descriptor::builder(log_schema())
        .options(lake_options())
        .build()
        .unwrap();
    let err = Spec::new(&path("db.t"), &no_buckets).unwrap_err();
    assert!(
        matches!(&err, Error::Invalid(m) if m.contains("bucket.num")),
        "{err}"
    );
}

#[test]
fn properties_pin_merge_on_read_and_prefix_table_options() {
    let pk = Descriptor::builder(pk_schema())
        .bucket_keys(["k"])
        .bucket_count(2)
        .options(Options {
            merge_engine: Some(MergeEngine::FirstRow),
            ..lake_options()
        })
        .custom("iceberg.write.format.default", "parquet")
        .custom("owner", "mink")
        .build()
        .unwrap();
    let props = Spec::new(&path("db.t"), &pk).unwrap().properties;
    for key in ["write.delete.mode", "write.update.mode", "write.merge.mode"] {
        assert_eq!(props.get(key).map(String::as_str), Some("merge-on-read"));
    }
    assert_eq!(props.get("mink.table.datalake.enabled").unwrap(), "true");
    assert_eq!(props.get("mink.table.datalake.format").unwrap(), "iceberg");
    assert_eq!(props.get("mink.table.merge-engine").unwrap(), "first_row");
    assert!(props.contains_key("mink.table.kv.format"));
    assert_eq!(props.get("write.format.default").unwrap(), "parquet");
    assert_eq!(props.get("mink.owner").unwrap(), "mink");

    let log = Descriptor::builder(log_schema())
        .bucket_count(2)
        .options(lake_options())
        .build()
        .unwrap();
    let props = Spec::new(&path("db.t"), &log).unwrap().properties;
    assert!(!props.contains_key("write.merge.mode"));
    assert!(!props.contains_key("mink.table.kv.format"));
}

#[tokio::test]
async fn attach_adopts_an_existing_table_and_validates_what_is_given() {
    let catalog = Catalog::connect(&Config::memory("memory:///attach"))
        .await
        .unwrap();
    let orders = path("sales.orders");
    let existing = Descriptor::builder(pk_schema())
        .partitioned_by(["region"])
        .bucket_keys(["k"])
        .bucket_count(4)
        .options(lake_options())
        .build()
        .unwrap();
    LakeCatalog::create_table(&catalog, &orders, &existing)
        .await
        .unwrap();

    let attach = |schema: Schema| {
        Descriptor::builder(schema)
            .partitioned_by(["region"])
            .bucket_keys(["k"])
            .bucket_count(4)
            .options(Options {
                lake_attach: true,
                ..lake_options()
            })
            .build()
            .unwrap()
    };
    let bare = Descriptor::builder(Schema::builder().build().unwrap())
        .options(Options {
            lake_attach: true,
            ..lake_options()
        })
        .build()
        .unwrap();
    let created = LakeCatalog::create_table(&catalog, &orders, &bare)
        .await
        .unwrap();
    assert_eq!(created.baseline_snapshot_id, None, "no data yet");
    let adopted = created.descriptor.unwrap();
    assert_eq!(adopted.partition_keys(), ["region"]);
    assert_eq!(adopted.bucket_keys(), ["k"]);
    assert_eq!(adopted.bucket_count(), Some(4));
    assert!(adopted.options().lake_attach);
    let adopted = adopted.schema();
    assert_eq!(
        adopted
            .columns()
            .iter()
            .map(Column::name)
            .collect::<Vec<_>>(),
        ["k", "region", "v"]
    );
    assert!(!adopted.columns()[0].data_type().is_nullable());
    assert_eq!(adopted.columns()[1].description(), Some("where"));
    assert_eq!(
        adopted.primary_key().unwrap().columns(),
        &["k".to_string(), "region".to_string()]
    );

    let created = LakeCatalog::create_table(&catalog, &orders, &attach(pk_schema()))
        .await
        .unwrap();
    assert_eq!(created.descriptor.unwrap(), attach(pk_schema()));

    let wrong_type = Schema::builder()
        .column(Column::new("k", DataType::int()).unwrap())
        .column(Column::new("region", DataType::string()).unwrap())
        .column(Column::new("v", DataType::string()).unwrap())
        .primary_key(PrimaryKey::new(vec!["k".into(), "region".into()]).unwrap())
        .build()
        .unwrap();
    let err = LakeCatalog::create_table(&catalog, &orders, &attach(wrong_type))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, lake::Error::Invalid { reason, .. } if reason.contains("column `k` is long")),
        "{err}"
    );

    let unbucketed = Descriptor::builder(pk_schema())
        .partitioned_by(["region"])
        .bucket_count(2)
        .options(Options {
            lake_attach: true,
            ..lake_options()
        })
        .build()
        .unwrap();
    let err = LakeCatalog::create_table(&catalog, &orders, &unbucketed)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, lake::Error::Invalid { reason, .. } if reason.contains("buckets by `k` into 4")),
        "{err}"
    );

    let wrong_partition = Descriptor::builder(pk_schema())
        .bucket_keys(["k"])
        .bucket_count(4)
        .options(Options {
            lake_attach: true,
            ..lake_options()
        })
        .build()
        .unwrap();
    let err = LakeCatalog::create_table(&catalog, &orders, &wrong_partition)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, lake::Error::Invalid { reason, .. } if reason.contains("partitioned by")),
        "{err}"
    );

    let err = LakeCatalog::create_table(&catalog, &path("sales.missing"), &attach(pk_schema()))
        .await
        .unwrap_err();
    assert!(matches!(err, lake::Error::TableNotFound(_)), "{err}");
}

#[tokio::test]
async fn catalog_creates_namespace_and_table_then_refuses_a_second() {
    let catalog = Catalog::connect(&Config::memory("memory:///warehouse"))
        .await
        .unwrap();
    let descriptor = Descriptor::builder(pk_schema())
        .partitioned_by(["region"])
        .bucket_count(4)
        .options(lake_options())
        .custom("owner", "mink")
        .build()
        .unwrap();
    let orders = path("sales.orders");

    LakeCatalog::create_table(&catalog, &orders, &descriptor)
        .await
        .unwrap();

    let table = catalog
        .catalog()
        .load_table(&Catalog::identifier(&orders))
        .await
        .unwrap();
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    let names: Vec<&str> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(names, ["k", "region", "v"]);
    let mut identifiers: Vec<&str> = schema
        .identifier_field_ids()
        .map(|id| schema.field_by_id(id).unwrap().name.as_str())
        .collect();
    identifiers.sort_unstable();
    assert_eq!(identifiers, ["k", "region"]);
    let partition: Vec<(&str, &Transform)> = metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|f| (f.name.as_str(), &f.transform))
        .collect();
    assert_eq!(
        partition,
        [
            ("region", &Transform::Identity),
            ("k_bucket", &Transform::Bucket(4))
        ]
    );
    assert!(metadata.default_sort_order().fields.is_empty());
    assert_eq!(metadata.format_version(), FormatVersion::V2);
    let props: HashMap<&str, &str> = metadata
        .properties()
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert_eq!(props.get("write.merge.mode"), Some(&"merge-on-read"));
    assert_eq!(props.get("mink.owner"), Some(&"mink"));

    let err = LakeCatalog::create_table(&catalog, &orders, &descriptor)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, lake::Error::TableExists(p) if *p == orders),
        "{err}"
    );

    let bad = Descriptor::builder(log_schema())
        .partitioned_by(["k"])
        .bucket_count(1)
        .options(lake_options())
        .build()
        .unwrap();
    let err = LakeCatalog::create_table(&catalog, &path("sales.bad"), &bad)
        .await
        .unwrap_err();
    assert!(matches!(err, lake::Error::Invalid { .. }), "{err}");
}
