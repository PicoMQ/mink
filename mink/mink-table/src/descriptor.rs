//! The complete definition of a table and the builder that validates partition keys, bucket keys,
//! options and aggregates together.

use std::collections::{BTreeMap, HashSet};

use mink_types::{Family, Root};
use serde::{Deserialize, Serialize};

use crate::{
    BucketId, Bucketing, ChangelogImage, DeleteBehavior, Error, MergeEngine, Options, PrimaryKey,
    Schema,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Repr", into = "Repr")]
pub struct Descriptor {
    schema: Schema,
    comment: Option<String>,
    partition_keys: Vec<String>,
    bucket_keys: Vec<String>,
    bucket_count: Option<u32>,
    options: Options,
    custom: BTreeMap<String, String>,
}

impl Descriptor {
    pub fn builder(schema: Schema) -> DescriptorBuilder {
        DescriptorBuilder {
            schema,
            comment: None,
            partition_keys: Vec::new(),
            bucket_keys: Vec::new(),
            bucket_count: None,
            options: Options::default(),
            custom: BTreeMap::new(),
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn has_primary_key(&self) -> bool {
        self.schema.primary_key().is_some()
    }

    pub fn partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_keys.is_empty()
    }

    pub fn bucket_keys(&self) -> &[String] {
        &self.bucket_keys
    }

    pub fn bucket_count(&self) -> Option<u32> {
        self.bucket_count
    }

    pub fn physical_primary_key(&self) -> Vec<String> {
        self.schema
            .primary_key()
            .map(|key| physical_key(key, &self.partition_keys))
            .unwrap_or_default()
    }

    pub fn is_default_bucket_key(&self) -> bool {
        if self.has_primary_key() {
            self.bucket_keys == self.physical_primary_key()
        } else {
            self.bucket_keys.is_empty()
        }
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn delete_behavior(&self) -> DeleteBehavior {
        self.options.delete_behavior.unwrap_or({
            if self.options.merge_engine.is_some() {
                DeleteBehavior::Ignore
            } else {
                DeleteBehavior::Allow
            }
        })
    }

    pub fn bucketing(&self) -> Bucketing {
        Bucketing::for_lake(self.options.lake)
    }

    pub fn custom(&self) -> &BTreeMap<String, String> {
        &self.custom
    }

    pub fn with_bucket_count(&self, bucket_count: u32) -> Self {
        Descriptor {
            bucket_count: Some(bucket_count),
            ..self.clone()
        }
    }

    pub fn with_custom(&self, custom: BTreeMap<String, String>) -> Self {
        Descriptor {
            custom,
            ..self.clone()
        }
    }

    pub fn to_builder(&self) -> DescriptorBuilder {
        DescriptorBuilder {
            schema: self.schema.clone(),
            comment: self.comment.clone(),
            partition_keys: self.partition_keys.clone(),
            bucket_keys: self.bucket_keys.clone(),
            bucket_count: self.bucket_count,
            options: self.options.clone(),
            custom: self.custom.clone(),
        }
    }
}

#[derive(Debug)]
pub struct DescriptorBuilder {
    schema: Schema,
    comment: Option<String>,
    partition_keys: Vec<String>,
    bucket_keys: Vec<String>,
    bucket_count: Option<u32>,
    options: Options,
    custom: BTreeMap<String, String>,
}

impl DescriptorBuilder {
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn partitioned_by<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.partition_keys = keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn bucket_keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.bucket_keys = keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn bucket_count(mut self, count: u32) -> Self {
        self.bucket_count = Some(count);
        self
    }

    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = schema;
        self
    }

    pub fn customs(mut self, custom: BTreeMap<String, String>) -> Self {
        self.custom = custom;
        self
    }

    pub fn custom(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> Result<Descriptor, Error> {
        let schema = self.schema;
        check_partition_keys(&schema, &self.partition_keys)?;
        let bucket_keys = resolve_bucket_keys(&schema, &self.partition_keys, self.bucket_keys)?;
        if let Some(count) = self.bucket_count {
            BucketId::check_count(count)?;
        }
        check_options(&schema, &self.options)?;
        check_auto_partition(&self.partition_keys, &self.options)?;
        check_aggregates(&schema)?;

        Ok(Descriptor {
            schema,
            comment: self.comment,
            partition_keys: self.partition_keys,
            bucket_keys,
            bucket_count: self.bucket_count,
            options: self.options,
            custom: self.custom,
        })
    }
}

fn physical_key(primary_key: &PrimaryKey, partition_keys: &[String]) -> Vec<String> {
    primary_key
        .columns()
        .iter()
        .filter(|column| !partition_keys.contains(column))
        .cloned()
        .collect()
}

fn check_partition_keys(schema: &Schema, partition_keys: &[String]) -> Result<(), Error> {
    let mut seen = HashSet::with_capacity(partition_keys.len());
    for key in partition_keys {
        if !seen.insert(key.as_str()) {
            return Err(Error::DuplicatePartitionKey(key.clone()));
        }
        let column = schema
            .column(key)
            .ok_or_else(|| Error::UnknownColumn(key.clone()))?;
        if let Some(primary_key) = schema.primary_key()
            && !primary_key.contains(key)
        {
            return Err(Error::PartitionKeyNotInPrimaryKey(key.clone()));
        }
        let root = column.data_type().root();
        if !root.is(Family::Predefined) || root == Root::Decimal {
            return Err(Error::PartitionKeyType {
                column: key.clone(),
                data_type: column.data_type().clone(),
            });
        }
    }
    Ok(())
}

fn resolve_bucket_keys(
    schema: &Schema,
    partition_keys: &[String],
    bucket_keys: Vec<String>,
) -> Result<Vec<String>, Error> {
    let mut seen = HashSet::with_capacity(bucket_keys.len());
    for key in &bucket_keys {
        if !seen.insert(key.as_str()) {
            return Err(Error::DuplicateBucketKey(key.clone()));
        }
        if schema.column(key).is_none() {
            return Err(Error::UnknownColumn(key.clone()));
        }
        if partition_keys.contains(key) {
            return Err(Error::BucketKeyIsPartitionKey(key.clone()));
        }
    }

    let Some(primary_key) = schema.primary_key() else {
        return Ok(bucket_keys);
    };
    if bucket_keys.is_empty() {
        let physical = physical_key(primary_key, partition_keys);
        if physical.is_empty() {
            return Err(Error::PrimaryKeyIsPartitionKey {
                primary_key: primary_key.columns().to_vec(),
                partition_keys: partition_keys.to_vec(),
            });
        }
        return Ok(physical);
    }

    for key in &bucket_keys {
        if !primary_key.contains(key) {
            return Err(Error::BucketKeyNotInPrimaryKey(key.clone()));
        }
    }

    Ok(bucket_keys)
}

fn check_auto_partition(partition_keys: &[String], options: &Options) -> Result<(), Error> {
    let Some(auto) = &options.auto_partition else {
        return Ok(());
    };
    if partition_keys.is_empty() {
        return Err(Error::AutoPartitionWithoutPartitionKeys);
    }

    if let Some(key) = &auto.key
        && !partition_keys.contains(key)
    {
        return Err(Error::AutoPartitionKeyUnknown(key.clone()));
    }
    if partition_keys.len() > 1 {
        if auto.key.is_none() {
            return Err(Error::AutoPartitionKeyMissing);
        }
        if auto.num_precreate.is_some_and(|n| n > 0) {
            return Err(Error::AutoPartitionPrecreateWithSeveralKeys);
        }
    }

    if auto.time_zone.parse::<chrono_tz::Tz>().is_err() {
        return Err(Error::AutoPartitionTimeZone(auto.time_zone.clone()));
    }

    Ok(())
}

fn check_options(schema: &Schema, options: &Options) -> Result<(), Error> {
    let has_primary_key = schema.primary_key().is_some();
    if options.delete_behavior.is_some() && !has_primary_key {
        return Err(Error::DeleteBehaviorWithoutPrimaryKey);
    }
    let Some(merge_engine) = &options.merge_engine else {
        return Ok(());
    };
    if !has_primary_key {
        return Err(Error::MergeEngineWithoutPrimaryKey);
    }

    match merge_engine {
        MergeEngine::FirstRow => {}
        MergeEngine::Versioned { column } => {
            let version = schema
                .column(column)
                .ok_or_else(|| Error::UnknownColumn(column.clone()))?;
            let root = version.data_type().root();
            if !matches!(
                root,
                Root::Int | Root::BigInt | Root::Timestamp | Root::TimestampLtz
            ) {
                return Err(Error::VersionColumnType {
                    column: column.clone(),
                    data_type: version.data_type().clone(),
                });
            }
        }
        MergeEngine::Aggregation => {
            if options.changelog_image == ChangelogImage::Wal {
                return Err(Error::AggregationWithWalImage);
            }
        }
    }

    if options.delete_behavior == Some(DeleteBehavior::Allow)
        && matches!(
            merge_engine,
            MergeEngine::FirstRow | MergeEngine::Versioned { .. }
        )
    {
        return Err(Error::DeleteNotAllowed(merge_engine.clone()));
    }

    Ok(())
}

fn check_aggregates(schema: &Schema) -> Result<(), Error> {
    for column in schema.columns() {
        if let Some(aggregate) = column.aggregate()
            && !aggregate.supports(column.data_type())
        {
            return Err(Error::AggregateType {
                aggregate: aggregate.clone(),
                column: column.name().to_owned(),
                data_type: column.data_type().clone(),
            });
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Repr {
    schema: Schema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    partition_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    bucket_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bucket_count: Option<u32>,
    #[serde(default)]
    options: Options,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    custom: BTreeMap<String, String>,
}

impl TryFrom<Repr> for Descriptor {
    type Error = Error;

    fn try_from(repr: Repr) -> Result<Self, Self::Error> {
        let mut builder = Descriptor::builder(repr.schema)
            .partitioned_by(repr.partition_keys)
            .bucket_keys(repr.bucket_keys)
            .options(repr.options)
            .customs(repr.custom);
        if let Some(comment) = repr.comment {
            builder = builder.comment(comment);
        }
        if let Some(count) = repr.bucket_count {
            builder = builder.bucket_count(count);
        }

        builder.build()
    }
}

impl From<Descriptor> for Repr {
    fn from(descriptor: Descriptor) -> Self {
        Repr {
            schema: descriptor.schema,
            comment: descriptor.comment,
            partition_keys: descriptor.partition_keys,
            bucket_keys: descriptor.bucket_keys,
            bucket_count: descriptor.bucket_count,
            options: descriptor.options,
            custom: descriptor.custom,
        }
    }
}

#[cfg(test)]
mod tests {
    use mink_types::{DataType, Decimal, Precision};
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::{Aggregate, Column, LakeFormat, PrimaryKey};

    fn column(name: &str, data_type: DataType) -> Column {
        Column::new(name, data_type).unwrap()
    }

    fn strings(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    fn log_schema() -> Schema {
        Schema::builder()
            .column(column("dt", DataType::string()))
            .column(column("user", DataType::string()))
            .column(column(
                "amount",
                DataType::decimal(Decimal::new(10, 2).unwrap()),
            ))
            .build()
            .unwrap()
    }

    fn pk_schema() -> Schema {
        Schema::builder()
            .column(column("dt", DataType::string()))
            .column(column("user", DataType::string()))
            .column(column("version", DataType::big_int()))
            .column(column("note", DataType::string()))
            .column(column("total", DataType::big_int()).with_aggregate(Aggregate::Sum))
            .primary_key(PrimaryKey::new(strings(&["dt", "user"])).unwrap())
            .build()
            .unwrap()
    }

    #[test]
    fn log_table_defaults() {
        let descriptor = Descriptor::builder(log_schema()).build().unwrap();
        assert!(!descriptor.has_primary_key());
        assert!(!descriptor.is_partitioned());
        assert!(descriptor.bucket_keys().is_empty());
        assert!(descriptor.is_default_bucket_key());
        assert_eq!(descriptor.bucket_count(), None);
        assert_eq!(descriptor.delete_behavior(), DeleteBehavior::Allow);
        assert_eq!(descriptor.bucketing(), Bucketing::Native);
    }

    #[test]
    fn primary_key_table_buckets_by_physical_key() {
        let descriptor = Descriptor::builder(pk_schema())
            .partitioned_by(["dt"])
            .bucket_count(16)
            .build()
            .unwrap();
        assert_eq!(descriptor.physical_primary_key(), strings(&["user"]));
        assert_eq!(descriptor.bucket_keys(), strings(&["user"]));
        assert!(descriptor.is_default_bucket_key());

        let explicit = Descriptor::builder(pk_schema())
            .bucket_keys(["user"])
            .build()
            .unwrap();
        assert_eq!(explicit.bucket_keys(), strings(&["user"]));
        assert!(!explicit.is_default_bucket_key());
    }

    #[test]
    fn partition_key_rules() {
        let err = |keys: &[&str], schema: Schema| {
            Descriptor::builder(schema)
                .partitioned_by(keys.iter().copied())
                .build()
                .unwrap_err()
        };
        assert_eq!(
            err(&["dt", "dt"], log_schema()),
            Error::DuplicatePartitionKey("dt".into())
        );
        assert_eq!(
            err(&["nope"], log_schema()),
            Error::UnknownColumn("nope".into())
        );
        assert_eq!(
            err(&["note"], pk_schema()),
            Error::PartitionKeyNotInPrimaryKey("note".into())
        );
        assert!(matches!(
            err(&["amount"], log_schema()),
            Error::PartitionKeyType { column, .. } if column == "amount"
        ));
        assert_eq!(
            err(&["dt", "user"], pk_schema()),
            Error::PrimaryKeyIsPartitionKey {
                primary_key: strings(&["dt", "user"]),
                partition_keys: strings(&["dt", "user"]),
            }
        );
    }

    #[test]
    fn bucket_key_rules() {
        let err = |keys: &[&str], partition: &[&str], schema: Schema| {
            Descriptor::builder(schema)
                .partitioned_by(partition.iter().copied())
                .bucket_keys(keys.iter().copied())
                .build()
                .unwrap_err()
        };
        assert_eq!(
            err(&["user", "user"], &[], log_schema()),
            Error::DuplicateBucketKey("user".into())
        );
        assert_eq!(
            err(&["nope"], &[], log_schema()),
            Error::UnknownColumn("nope".into())
        );
        assert_eq!(
            err(&["dt"], &["dt"], log_schema()),
            Error::BucketKeyIsPartitionKey("dt".into())
        );
        assert_eq!(
            err(&["note"], &[], pk_schema()),
            Error::BucketKeyNotInPrimaryKey("note".into())
        );
        assert_eq!(
            Descriptor::builder(log_schema())
                .bucket_count(0)
                .build()
                .unwrap_err(),
            Error::BucketCount
        );
        assert!(
            Descriptor::builder(log_schema())
                .bucket_keys(["user", "dt"])
                .build()
                .is_ok()
        );
    }

    #[test]
    fn merge_engine_rules() {
        let build =
            |options: Options, schema: Schema| Descriptor::builder(schema).options(options).build();
        let versioned = |column: &str| Options {
            merge_engine: Some(MergeEngine::Versioned {
                column: column.into(),
            }),
            ..Options::default()
        };

        assert_eq!(
            build(versioned("version"), log_schema()).unwrap_err(),
            Error::MergeEngineWithoutPrimaryKey
        );
        assert!(build(versioned("version"), pk_schema()).is_ok());
        assert_eq!(
            build(versioned("nope"), pk_schema()).unwrap_err(),
            Error::UnknownColumn("nope".into())
        );
        assert!(matches!(
            build(versioned("note"), pk_schema()).unwrap_err(),
            Error::VersionColumnType { column, .. } if column == "note"
        ));

        let aggregation = Options {
            merge_engine: Some(MergeEngine::Aggregation),
            changelog_image: ChangelogImage::Wal,
            ..Options::default()
        };
        assert_eq!(
            build(aggregation, pk_schema()).unwrap_err(),
            Error::AggregationWithWalImage
        );
    }

    #[test]
    fn delete_behavior_rules() {
        let with = |merge_engine: Option<MergeEngine>, delete: Option<DeleteBehavior>| {
            Descriptor::builder(pk_schema())
                .options(Options {
                    merge_engine,
                    delete_behavior: delete,
                    ..Options::default()
                })
                .build()
        };
        assert_eq!(
            Descriptor::builder(log_schema())
                .options(Options {
                    delete_behavior: Some(DeleteBehavior::Allow),
                    ..Options::default()
                })
                .build()
                .unwrap_err(),
            Error::DeleteBehaviorWithoutPrimaryKey
        );
        assert_eq!(
            with(None, None).unwrap().delete_behavior(),
            DeleteBehavior::Allow
        );
        assert_eq!(
            with(Some(MergeEngine::FirstRow), None)
                .unwrap()
                .delete_behavior(),
            DeleteBehavior::Ignore
        );
        assert_eq!(
            with(Some(MergeEngine::FirstRow), Some(DeleteBehavior::Disable))
                .unwrap()
                .delete_behavior(),
            DeleteBehavior::Disable
        );
        assert_eq!(
            with(Some(MergeEngine::FirstRow), Some(DeleteBehavior::Allow)).unwrap_err(),
            Error::DeleteNotAllowed(MergeEngine::FirstRow)
        );
        assert_eq!(
            with(Some(MergeEngine::Aggregation), Some(DeleteBehavior::Allow))
                .unwrap()
                .delete_behavior(),
            DeleteBehavior::Allow
        );
    }

    #[test]
    fn aggregates_must_fit_their_column_type() {
        let schema = Schema::builder()
            .column(column("id", DataType::int()))
            .column(column("flag", DataType::boolean()).with_aggregate(Aggregate::Sum))
            .primary_key(PrimaryKey::new(strings(&["id"])).unwrap())
            .build()
            .unwrap();
        let err = Descriptor::builder(schema).build().unwrap_err();
        assert_eq!(
            err,
            Error::AggregateType {
                aggregate: Aggregate::Sum,
                column: "flag".into(),
                data_type: DataType::boolean(),
            }
        );
    }

    #[test]
    fn lake_selects_bucketing() {
        let with_lake = |lake| {
            Descriptor::builder(log_schema())
                .options(Options {
                    lake,
                    ..Options::default()
                })
                .build()
                .unwrap()
                .bucketing()
        };
        assert_eq!(with_lake(Some(LakeFormat::Paimon)), Bucketing::Paimon);
        assert_eq!(with_lake(Some(LakeFormat::Iceberg)), Bucketing::Iceberg);
        assert_eq!(with_lake(Some(LakeFormat::Lance)), Bucketing::Native);
    }

    #[test]
    fn json_round_trip_revalidates() {
        let descriptor = Descriptor::builder(pk_schema())
            .comment("orders per user per day")
            .partitioned_by(["dt"])
            .bucket_count(8)
            .options(Options {
                merge_engine: Some(MergeEngine::Versioned {
                    column: "version".into(),
                }),
                lake: Some(LakeFormat::Paimon),
                ..Options::default()
            })
            .custom("owner", "billing")
            .build()
            .unwrap();
        let json = serde_json::to_value(&descriptor).unwrap();
        assert_eq!(json["partition_keys"], serde_json::json!(["dt"]));
        assert_eq!(json["bucket_keys"], serde_json::json!(["user"]));
        assert_eq!(json["bucket_count"], serde_json::json!(8));
        assert_eq!(
            json["options"],
            serde_json::json!({
                "log_format": "arrow",
                "kv_format": "compacted",
                "merge_engine": {"versioned": {"column": "version"}},
                "changelog_image": "full",
                "lake": "paimon",
            })
        );
        assert_eq!(json["custom"], serde_json::json!({"owner": "billing"}));
        assert_eq!(
            serde_json::from_value::<Descriptor>(json.clone()).unwrap(),
            descriptor
        );

        let mut broken = json;
        broken["bucket_keys"] = serde_json::json!(["dt"]);
        assert!(serde_json::from_value::<Descriptor>(broken).is_err());
    }

    #[test]
    fn minimal_json_omits_defaults() {
        let descriptor = Descriptor::builder(
            Schema::builder()
                .column(column("v", DataType::timestamp(Precision::new(3).unwrap())))
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
        let json = serde_json::to_value(&descriptor).unwrap();
        let mut keys: Vec<_> = json.as_object().unwrap().keys().collect();
        keys.sort();
        assert_eq!(keys, ["options", "schema"]);
        assert_eq!(
            json["options"],
            serde_json::json!({"log_format": "arrow", "kv_format": "compacted", "changelog_image": "full"})
        );
    }
}
