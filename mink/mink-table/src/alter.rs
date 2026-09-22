//! Schema and option changes applied to an existing table, with the rules for what may be altered.

use std::collections::BTreeMap;
use std::time::Duration;

use mink_common::time;
use mink_types::{DataType, Field, FieldId, Fields, Kind};
use serde::{Deserialize, Serialize};

use crate::{
    Column, DEFAULT_LAKE_FRESHNESS, Descriptor, Error, LakeFormat, MergeEngine, Options, Schema,
};

pub const DATALAKE_ENABLED: &str = "table.datalake.enabled";
pub const DATALAKE_FRESHNESS: &str = "table.datalake.freshness";
const TABLE_PREFIX: &str = "table.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum Change {
    AddColumn {
        name: String,
        data_type: DataType,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        comment: Option<String>,
    },
    DropColumn {
        name: String,
    },
    RenameColumn {
        name: String,
        new_name: String,
    },
    ModifyColumn {
        name: String,
        data_type: DataType,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        comment: Option<String>,
    },
    SetOption {
        key: String,
        value: String,
    },
    ResetOption {
        key: String,
    },
}

impl Change {
    pub fn add_column(name: impl Into<String>, data_type: DataType) -> Self {
        Change::AddColumn {
            name: name.into(),
            data_type,
            comment: None,
        }
    }

    pub fn set(key: impl Into<String>, value: impl Into<String>) -> Self {
        Change::SetOption {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn reset(key: impl Into<String>) -> Self {
        Change::ResetOption { key: key.into() }
    }
}

pub fn apply_schema_changes(descriptor: &Descriptor, changes: &[Change]) -> Result<Schema, Error> {
    let schema = descriptor.schema();
    let mut columns = schema.columns().to_vec();
    let mut next_field_id = schema.next_field_id();
    for change in changes {
        match change {
            Change::AddColumn {
                name,
                data_type,
                comment,
            } => {
                if columns.iter().any(|c| c.name() == name) {
                    return Err(Error::ColumnExists(name.clone()));
                }
                if !data_type.is_nullable() {
                    return Err(Error::ColumnNotNullable(name.clone()));
                }
                let id = FieldId(next_field_id);
                next_field_id += 1;
                let field =
                    Field::new(name, data_type.assign_field_ids(&mut next_field_id))?.with_id(id);
                let mut column = Column::from_field(field);
                if let Some(comment) = comment {
                    column = column.with_description(comment.clone());
                }
                columns.push(column);
            }

            Change::DropColumn { name } => {
                let index = position(&columns, name)?;
                referenced(descriptor, name, "dropped")?;
                if columns.len() == 1 {
                    return Err(Error::NoColumns);
                }
                columns.remove(index);
            }
            Change::RenameColumn { name, new_name } => {
                let index = position(&columns, name)?;
                referenced(descriptor, name, "renamed")?;
                if columns.iter().any(|c| c.name() == new_name) {
                    return Err(Error::ColumnExists(new_name.clone()));
                }
                columns[index] = columns[index].clone().with_name(new_name.clone())?;
            }
            Change::ModifyColumn {
                name,
                data_type,
                comment,
            } => {
                let index = position(&columns, name)?;
                let column = &columns[index];
                if column.data_type() != data_type {
                    referenced(descriptor, name, "retyped")?;
                    if !promotable(column.data_type(), data_type) {
                        return Err(Error::TypeNotPromotable {
                            column: name.clone(),
                            from: column.data_type().clone(),
                            to: data_type.clone(),
                        });
                    }
                }
                let mut modified = column
                    .clone()
                    .with_data_type(keep_field_ids(column.data_type(), data_type));
                if let Some(comment) = comment {
                    modified = modified.with_description(comment.clone());
                }
                columns[index] = modified;
            }
            Change::SetOption { .. } | Change::ResetOption { .. } => {}
        }
    }

    Schema::new(
        columns,
        schema.primary_key().cloned(),
        schema.auto_increment().map(str::to_owned),
        next_field_id,
    )
}

fn position(columns: &[Column], name: &str) -> Result<usize, Error> {
    columns
        .iter()
        .position(|c| c.name() == name)
        .ok_or_else(|| Error::UnknownColumn(name.to_owned()))
}

fn referenced(descriptor: &Descriptor, name: &str, change: &'static str) -> Result<(), Error> {
    let schema = descriptor.schema();
    let by = if schema
        .primary_key()
        .is_some_and(|k| k.columns().iter().any(|c| c == name))
    {
        Some("the primary key")
    } else if descriptor.partition_keys().iter().any(|k| k == name) {
        Some("the partition keys")
    } else if descriptor.bucket_keys().iter().any(|k| k == name) {
        Some("the bucket keys")
    } else if schema.auto_increment() == Some(name) {
        Some("auto-increment")
    } else if matches!(
        descriptor.options().merge_engine,
        Some(MergeEngine::Versioned { ref column }) if column == name
    ) {
        Some("the versioned merge engine")
    } else {
        None
    };
    match by {
        Some(referenced_by) => Err(Error::ColumnReferenced {
            column: name.to_owned(),
            referenced_by,
            change,
        }),
        None => Ok(()),
    }
}

fn keep_field_ids(from: &DataType, to: &DataType) -> DataType {
    let kind = match (from.kind(), to.kind()) {
        (Kind::Array(a), Kind::Array(b)) => Kind::Array(Box::new(keep_field_ids(a, b))),
        (Kind::Map { key: ka, value: va }, Kind::Map { key: kb, value: vb }) => Kind::Map {
            key: Box::new(keep_field_ids(ka, kb)),
            value: Box::new(keep_field_ids(va, vb)),
        },
        (Kind::Row(a), Kind::Row(b)) if a.len() == b.len() => {
            let fields = a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| {
                    let mut field = y
                        .clone()
                        .with_data_type(keep_field_ids(x.data_type(), y.data_type()));
                    if let Some(id) = x.id() {
                        field = field.with_id(id);
                    }
                    field
                })
                .collect();
            Kind::Row(Fields::new(fields).expect("names come from a valid row"))
        }
        (_, kind) => kind.clone(),
    };
    DataType::new(kind, to.is_nullable())
}

pub fn promotable(from: &DataType, to: &DataType) -> bool {
    if from.is_nullable() && !to.is_nullable() {
        return false;
    }
    use Kind::*;
    match (from.kind(), to.kind()) {
        (a, b) if a == b => true,
        (TinyInt, SmallInt | Int | BigInt) => true,
        (SmallInt, Int | BigInt) => true,
        (Int, BigInt) => true,
        (Float, Double) => true,
        (Decimal(a), Decimal(b)) => a.scale() == b.scale() && b.precision() >= a.precision(),
        (Char(a), Char(b)) => b >= a,
        (Char(_), String) => true,
        (Binary(a), Binary(b)) => b >= a,
        (Binary(_), Bytes) => true,
        (Time(a), Time(b)) | (Timestamp(a), Timestamp(b)) | (TimestampLtz(a), TimestampLtz(b)) => {
            b >= a
        }
        (Array(a), Array(b)) => promotable(a, b),
        (Map { key: ka, value: va }, Map { key: kb, value: vb }) => ka == kb && promotable(va, vb),
        (Row(a), Row(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(x, y)| x.name() == y.name() && promotable(x.data_type(), y.data_type()))
        }
        _ => false,
    }
}

pub fn alter_table(
    descriptor: &Descriptor,
    changes: &[Change],
    cluster_lake: Option<LakeFormat>,
) -> Result<Descriptor, Error> {
    let schema = apply_schema_changes(descriptor, changes)?;
    let mut options = descriptor.options().clone();
    let mut custom = descriptor.custom().clone();
    for change in changes {
        match change {
            Change::SetOption { key, value } => {
                set_option(&mut options, &mut custom, key, Some(value), cluster_lake)?;
            }
            Change::ResetOption { key } => {
                set_option(&mut options, &mut custom, key, None, cluster_lake)?;
            }
            _ => {}
        }
    }

    if let Some(lake) = options.lake {
        let prefix = format!("{lake}.");
        for change in changes {
            let key = match change {
                Change::SetOption { key, .. } | Change::ResetOption { key } => key,
                _ => continue,
            };
            if key.starts_with(&prefix) {
                return Err(Error::LakeProperty(key.clone()));
            }
        }
    }

    descriptor
        .to_builder()
        .schema(schema)
        .options(options)
        .customs(custom)
        .build()
}

fn set_option(
    options: &mut Options,
    custom: &mut BTreeMap<String, String>,
    key: &str,
    value: Option<&str>,
    cluster_lake: Option<LakeFormat>,
) -> Result<(), Error> {
    match key {
        DATALAKE_ENABLED => {
            let enabled = match value {
                Some(value) => parse_bool(key, value)?,
                None => false,
            };
            options.lake = if enabled {
                Some(cluster_lake.ok_or(Error::LakeNotConfigured)?)
            } else {
                None
            };
        }
        DATALAKE_FRESHNESS => {
            options.lake_freshness = match value {
                Some(value) => parse_duration(key, value)?,
                None => DEFAULT_LAKE_FRESHNESS,
            };
        }
        _ if key.starts_with(TABLE_PREFIX) => {
            return Err(Error::NotAlterable(key.to_owned()));
        }
        _ => match value {
            Some(value) => {
                custom.insert(key.to_owned(), value.to_owned());
            }
            None => {
                custom.remove(key);
            }
        },
    }
    Ok(())
}

fn parse_bool(key: &str, value: &str) -> Result<bool, Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(Error::OptionValue {
            key: key.to_owned(),
            value: value.to_owned(),
        }),
    }
}

fn parse_duration(key: &str, value: &str) -> Result<Duration, Error> {
    time::parse_duration(value).ok_or_else(|| Error::OptionValue {
        key: key.to_owned(),
        value: value.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PrimaryKey;
    use mink_types::{Decimal, Length, Precision};

    fn table() -> Descriptor {
        let schema = Schema::builder()
            .column(Column::new("k", DataType::big_int().with_nullable(false)).unwrap())
            .column(Column::new("v", DataType::string()).unwrap())
            .primary_key(PrimaryKey::new(vec!["k".into()]).unwrap())
            .build()
            .unwrap();
        Descriptor::builder(schema)
            .bucket_count(2)
            .custom("owner", "ann")
            .build()
            .unwrap()
    }

    #[test]
    fn add_column_appends_nullable_with_fresh_id() {
        let altered =
            alter_table(&table(), &[Change::add_column("w", DataType::int())], None).unwrap();
        let columns = altered.schema().columns();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[2].name(), "w");
        assert_eq!(columns[2].id().map(|id| id.0), Some(2));
        assert_eq!(altered.schema().next_field_id(), 3);
        assert_eq!(altered.bucket_count(), Some(2), "the rest is kept");
        assert!(altered.has_primary_key());
    }

    #[test]
    fn schema_changes_are_validated() {
        let t = table();
        assert!(matches!(
            alter_table(&t, &[Change::add_column("v", DataType::int())], None),
            Err(Error::ColumnExists(_))
        ));
        assert!(matches!(
            alter_table(
                &t,
                &[Change::add_column(
                    "w",
                    DataType::int().with_nullable(false)
                )],
                None
            ),
            Err(Error::ColumnNotNullable(_))
        ));
    }

    #[test]
    fn drop_rename_and_modify_keep_ids_and_refuse_referenced_columns() {
        let t = alter_table(
            &table(),
            &[
                Change::add_column("w", DataType::int()),
                Change::add_column(
                    "r",
                    DataType::row(
                        Fields::new(vec![Field::new("a", DataType::float()).unwrap()]).unwrap(),
                    ),
                ),
            ],
            None,
        )
        .unwrap();
        assert_eq!(t.schema().next_field_id(), 5);

        let altered = alter_table(
            &t,
            &[
                Change::RenameColumn {
                    name: "v".into(),
                    new_name: "value".into(),
                },
                Change::ModifyColumn {
                    name: "w".into(),
                    data_type: DataType::big_int(),
                    comment: Some("wide".into()),
                },
                Change::ModifyColumn {
                    name: "r".into(),
                    data_type: DataType::row(
                        Fields::new(vec![Field::new("a", DataType::double()).unwrap()]).unwrap(),
                    ),
                    comment: None,
                },
            ],
            None,
        )
        .unwrap();
        let columns = altered.schema().columns();
        assert_eq!(
            columns
                .iter()
                .map(|c| (c.name(), c.id().unwrap().0))
                .collect::<Vec<_>>(),
            [("k", 0), ("value", 1), ("w", 2), ("r", 3)]
        );
        assert_eq!(columns[2].data_type(), &DataType::big_int());
        assert_eq!(columns[2].description(), Some("wide"));
        let Kind::Row(fields) = columns[3].data_type().kind() else {
            panic!("row");
        };
        assert_eq!(fields[0].data_type(), &DataType::double());
        assert_eq!(fields[0].id().map(|id| id.0), Some(4), "nested id kept");
        assert_eq!(altered.schema().next_field_id(), 5);

        let dropped =
            alter_table(&altered, &[Change::DropColumn { name: "w".into() }], None).unwrap();
        assert_eq!(
            dropped
                .schema()
                .columns()
                .iter()
                .map(Column::name)
                .collect::<Vec<_>>(),
            ["k", "value", "r"]
        );
        assert_eq!(dropped.schema().next_field_id(), 5, "ids are never reused");

        for change in [
            Change::DropColumn { name: "k".into() },
            Change::RenameColumn {
                name: "k".into(),
                new_name: "key".into(),
            },
            Change::ModifyColumn {
                name: "k".into(),
                data_type: DataType::string(),
                comment: None,
            },
        ] {
            assert!(matches!(
                alter_table(&t, &[change], None),
                Err(Error::ColumnReferenced {
                    referenced_by: "the primary key",
                    ..
                })
            ));
        }
        alter_table(
            &t,
            &[Change::ModifyColumn {
                name: "k".into(),
                data_type: DataType::big_int().with_nullable(false),
                comment: Some("the key".into()),
            }],
            None,
        )
        .unwrap();
        assert!(matches!(
            alter_table(
                &t,
                &[Change::ModifyColumn {
                    name: "w".into(),
                    data_type: DataType::string(),
                    comment: None,
                }],
                None
            ),
            Err(Error::TypeNotPromotable { .. })
        ));
        assert!(matches!(
            alter_table(
                &t,
                &[Change::ModifyColumn {
                    name: "w".into(),
                    data_type: DataType::int().with_nullable(false),
                    comment: None,
                }],
                None
            ),
            Err(Error::TypeNotPromotable { .. })
        ));
        assert!(matches!(
            alter_table(
                &t,
                &[Change::RenameColumn {
                    name: "w".into(),
                    new_name: "v".into(),
                }],
                None
            ),
            Err(Error::ColumnExists(_))
        ));
        assert!(matches!(
            alter_table(
                &t,
                &[Change::DropColumn {
                    name: "nope".into()
                }],
                None
            ),
            Err(Error::UnknownColumn(_))
        ));
    }

    #[test]
    fn promotion_table() {
        use DataType as D;
        let ok = [
            (D::tiny_int(), D::big_int()),
            (D::small_int(), D::int()),
            (D::float(), D::double()),
            (
                D::decimal(Decimal::new(10, 2).unwrap()),
                D::decimal(Decimal::new(20, 2).unwrap()),
            ),
            (D::char(Length::new(3).unwrap()), D::string()),
            (D::binary(Length::new(3).unwrap()), D::bytes()),
            (
                D::timestamp(Precision::MILLIS),
                D::timestamp(Precision::MICROS),
            ),
            (D::array(D::int()), D::array(D::big_int())),
            (D::int().with_nullable(false), D::int()),
        ];
        for (from, to) in ok {
            assert!(promotable(&from, &to), "{from} -> {to}");
        }
        let bad = [
            (D::big_int(), D::int()),
            (D::double(), D::float()),
            (
                D::decimal(Decimal::new(10, 2).unwrap()),
                D::decimal(Decimal::new(10, 3).unwrap()),
            ),
            (D::string(), D::char(Length::new(3).unwrap())),
            (D::int(), D::string()),
            (D::int(), D::int().with_nullable(false)),
            (D::array(D::int()), D::int()),
            (D::map(D::int(), D::int()), D::map(D::big_int(), D::int())),
        ];
        for (from, to) in bad {
            assert!(!promotable(&from, &to), "{from} -> {to}");
        }
    }

    #[test]
    fn option_changes_are_validated() {
        let t = table();
        assert!(matches!(
            alter_table(&t, &[Change::set(DATALAKE_ENABLED, "true")], None),
            Err(Error::LakeNotConfigured)
        ));
        let lake = alter_table(
            &t,
            &[
                Change::set(DATALAKE_ENABLED, "true"),
                Change::set(DATALAKE_FRESHNESS, "30s"),
            ],
            Some(LakeFormat::Iceberg),
        )
        .unwrap();
        assert_eq!(lake.options().lake, Some(LakeFormat::Iceberg));
        assert_eq!(lake.options().lake_freshness, Duration::from_secs(30));
        assert!(matches!(
            alter_table(
                &lake,
                &[Change::set("iceberg.write.format.default", "orc")],
                Some(LakeFormat::Iceberg)
            ),
            Err(Error::LakeProperty(_))
        ));
        let reset = alter_table(
            &lake,
            &[
                Change::reset(DATALAKE_ENABLED),
                Change::reset(DATALAKE_FRESHNESS),
            ],
            Some(LakeFormat::Iceberg),
        )
        .unwrap();
        assert_eq!(reset.options(), t.options());
        assert!(matches!(
            alter_table(&t, &[Change::set("table.log.ttl", "1h")], None),
            Err(Error::NotAlterable(_))
        ));
        let custom = alter_table(
            &t,
            &[Change::set("team", "data"), Change::reset("owner")],
            None,
        )
        .unwrap();
        assert_eq!(
            custom.custom().get("team").map(String::as_str),
            Some("data")
        );
        assert!(!custom.custom().contains_key("owner"));
    }

    #[test]
    fn durations_parse_with_units() {
        assert_eq!(
            parse_duration("k", "1500").unwrap(),
            Duration::from_millis(1500)
        );
        assert_eq!(
            parse_duration("k", "3 min").unwrap(),
            Duration::from_secs(180)
        );
        assert_eq!(
            parse_duration("k", "2h").unwrap(),
            Duration::from_secs(7200)
        );
        assert!(parse_duration("k", "soon").is_err());
    }
}
