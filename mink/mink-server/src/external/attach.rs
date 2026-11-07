use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use iceberg::TableRequirement;
use iceberg::spec::{
    NullOrder, Schema, SortDirection, SortField, SortOrder, Transform, UnboundPartitionField,
    UnboundPartitionSpec,
};
use iceberg::table::Table as IcebergTable;
use mink_core::{CatalogTable, ExternalLocation, ExternalTable, TierSchedule, Topic};
use mink_iceberg::ExternalSnapshot;
use mink_kafka::Pool;
use mink_store::Store;

use super::{Catalog, identifier};
use crate::Error;

#[derive(Debug, Clone)]
pub struct Attachment {
    pub topic: String,
    pub table: CatalogTable,
    pub schedule: TierSchedule,
    pub source: Source,
}

#[derive(Debug, Clone)]
pub enum Source {
    New {
        partitioning: Partitioning,
        sort: Vec<SortTerm>,
        properties: HashMap<String, String>,
        location: Option<String>,
    },
    Existing {
        end_offsets: BTreeMap<i32, i64>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Partitioning {
    Unpartitioned,
    Fields(Vec<Term>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Term {
    transform: Transform,
    column: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortTerm {
    term: Term,
    direction: SortDirection,
    null_order: NullOrder,
}

impl Source {
    pub fn new(
        existing: bool,
        partition: Option<Vec<Term>>,
        sort: Vec<SortTerm>,
        properties: HashMap<String, String>,
        location: Option<String>,
        end_offsets: BTreeMap<i32, i64>,
    ) -> Result<Source, Error> {
        if existing {
            let kept = [
                ("partitioning", partition.is_some()),
                ("sort order", !sort.is_empty()),
                ("properties", !properties.is_empty()),
                ("location", location.is_some()),
            ];
            return match kept.into_iter().find(|(_, given)| *given) {
                Some((kept, _)) => Err(Error::ExistingTableKeeps(kept)),
                None => Ok(Source::Existing { end_offsets }),
            };
        }

        if !end_offsets.is_empty() {
            return Err(Error::EndOffsetsOfNewTable);
        }
        let partitioning = match partition {
            None => return Err(Error::PartitioningRequired),
            Some(terms) if terms.is_empty() => Partitioning::Unpartitioned,
            Some(terms) => Partitioning::Fields(terms),
        };

        Ok(Source::New {
            partitioning,
            sort,
            properties,
            location,
        })
    }
}

pub(crate) async fn attach(
    store: &Store,
    kafka: &Pool,
    catalog: &Catalog,
    warehouse: &str,
    attachment: Attachment,
) -> Result<ExternalTable, Error> {
    let topic = store
        .topics()
        .get(&attachment.topic)
        .await?
        .ok_or_else(|| Error::NoSuchTable(attachment.topic.clone()))?;
    check_retention(kafka, topic.name(), attachment.schedule).await?;

    let identifier = identifier(&attachment.table);
    match &attachment.source {
        Source::New {
            partitioning,
            sort,
            properties,
            location,
        } => {
            let schema = store.schemas().latest(topic.name()).await?;
            let spec = partition_spec(&schema, partitioning)?;
            let order = sort_order(&schema, sort)?;
            if let Some(location) = location {
                external_location(warehouse, location)?;
            }

            let table = catalog
                .create_table_with_ids(
                    &identifier,
                    schema,
                    spec,
                    order,
                    properties.clone(),
                    location.clone(),
                )
                .await?;
            let recorded = record(
                store,
                catalog,
                warehouse,
                &attachment,
                &topic,
                &table,
                &BTreeMap::new(),
            )
            .await;
            if recorded.is_err()
                && let Err(error) = iceberg::Catalog::drop_table(catalog, &identifier).await
            {
                tracing::warn!(topic = topic.name(), %error, "a table created for an attach that failed was not dropped");
            }
            recorded
        }
        Source::Existing { end_offsets } => {
            let table = iceberg::Catalog::load_table(catalog, &identifier).await?;

            record(
                store,
                catalog,
                warehouse,
                &attachment,
                &topic,
                &table,
                end_offsets,
            )
            .await
        }
    }
}

pub(crate) async fn detach(store: &Store, topic: &str) -> Result<(), Error> {
    Ok(store.external_tables().detach(topic).await?)
}

async fn record(
    store: &Store,
    catalog: &Catalog,
    warehouse: &str,
    attachment: &Attachment,
    topic: &Topic,
    table: &IcebergTable,
    given: &BTreeMap<i32, i64>,
) -> Result<ExternalTable, Error> {
    let schema = store.schemas().latest(topic.name()).await?;
    let specs = store.time_specs().list(topic.name()).await?;
    ExternalSnapshot::check(table, &schema, &specs).await?;

    let metadata = table.metadata();
    let location = external_location(warehouse, metadata.location())?;
    let stored = mink_iceberg::end_offsets(table)?;
    let holds_data = metadata.current_snapshot().is_some();
    let offsets = chosen_end_offsets(topic, &stored, given, holds_data)?;

    let external = store
        .external_tables()
        .attach(
            topic.name(),
            attachment.table.clone(),
            metadata.uuid(),
            location,
            attachment.schedule,
            &offsets,
        )
        .await?;

    if stored.is_empty() && !offsets.is_empty() {
        let written = catalog
            .commit(
                table.identifier(),
                vec![TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                }],
                vec![mink_iceberg::end_offsets_update(&offsets)],
            )
            .await;

        if let Err(error) = written {
            store.external_tables().detach(topic.name()).await?;
            return Err(error.into());
        }
    }

    Ok(external)
}

fn chosen_end_offsets(
    topic: &Topic,
    stored: &BTreeMap<i32, i64>,
    given: &BTreeMap<i32, i64>,
    holds_data: bool,
) -> Result<BTreeMap<i32, i64>, Error> {
    if !stored.is_empty() && !given.is_empty() && stored != given {
        return Err(Error::ConflictingEndOffsets(topic.name().to_owned()));
    }
    let offsets = match given.is_empty() {
        true => stored.clone(),
        false => given.clone(),
    };

    for (partition, offset) in &offsets {
        if !(0..topic.partition_count()).contains(partition) || *offset < 0 {
            return Err(Error::InvalidEndOffset {
                partition: *partition,
                offset: *offset,
            });
        }
    }
    let every_partition = offsets.len() == topic.partition_count() as usize;
    if holds_data && !every_partition {
        return Err(Error::MissingEndOffsets(topic.name().to_owned()));
    }

    Ok(offsets)
}

async fn check_retention(kafka: &Pool, topic: &str, schedule: TierSchedule) -> Result<(), Error> {
    let needed = schedule.after() + 2 * schedule.interval();
    let retention = kafka.cluster().await?.retention(topic).await?;

    match retention {
        Some(retention) if retention <= needed => Err(Error::ShortRetention {
            topic: topic.to_owned(),
            retention,
            needed,
        }),
        _ => Ok(()),
    }
}

fn external_location(warehouse: &str, location: &str) -> Result<ExternalLocation, Error> {
    let location = ExternalLocation::parse(location)?;
    if location.overlaps(warehouse) {
        return Err(Error::LocationInWarehouse(location.path().to_owned()));
    }

    Ok(location)
}

fn partition_spec(
    schema: &Schema,
    partitioning: &Partitioning,
) -> Result<UnboundPartitionSpec, Error> {
    let terms = match partitioning {
        Partitioning::Unpartitioned => &[][..],
        Partitioning::Fields(terms) => terms.as_slice(),
    };

    let fields = terms
        .iter()
        .map(|term| {
            let source = schema
                .field_by_name(&term.column)
                .ok_or_else(|| Error::UnknownColumn(term.column.clone()))?;

            Ok(UnboundPartitionField {
                source_id: source.id,
                field_id: None,
                name: term.partition_name(),
                transform: term.transform,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(mink_iceberg::external_partition_spec(schema, fields)?)
}

fn sort_order(schema: &Schema, terms: &[SortTerm]) -> Result<SortOrder, Error> {
    if terms.is_empty() {
        return Ok(SortOrder::unsorted_order());
    }

    let mut builder = SortOrder::builder();
    for sorted in terms {
        let source = schema
            .field_by_name(&sorted.term.column)
            .ok_or_else(|| Error::UnknownColumn(sorted.term.column.clone()))?;

        builder.with_sort_field(
            SortField::builder()
                .source_id(source.id)
                .transform(sorted.term.transform)
                .direction(sorted.direction)
                .null_order(sorted.null_order)
                .build(),
        );
    }

    Ok(builder.with_order_id(1).build(schema)?)
}

impl Term {
    fn partition_name(&self) -> String {
        let column = &self.column;

        match self.transform {
            Transform::Identity => column.clone(),
            Transform::Bucket(_) => format!("{column}_bucket"),
            Transform::Truncate(_) => format!("{column}_trunc"),
            Transform::Year => format!("{column}_year"),
            Transform::Month => format!("{column}_month"),
            Transform::Day => format!("{column}_day"),
            Transform::Hour => format!("{column}_hour"),
            Transform::Void | Transform::Unknown => format!("{column}_null"),
        }
    }
}

impl FromStr for Term {
    type Err = Error;

    fn from_str(text: &str) -> Result<Term, Error> {
        let unreadable = || Error::UnreadableTerm(text.to_owned());
        let text = text.trim();

        let Some((name, rest)) = text.split_once('(') else {
            return column(text).map(|column| Term {
                transform: Transform::Identity,
                column,
            });
        };
        let arguments: Vec<&str> = rest
            .strip_suffix(')')
            .ok_or_else(unreadable)?
            .split(',')
            .map(str::trim)
            .collect();
        let width = |argument: &str| argument.parse::<u32>().map_err(|_| unreadable());

        let (transform, column_name) =
            match (name.trim().to_lowercase().as_str(), arguments.as_slice()) {
                ("identity", [column]) => (Transform::Identity, *column),
                ("year", [column]) => (Transform::Year, *column),
                ("month", [column]) => (Transform::Month, *column),
                ("day", [column]) => (Transform::Day, *column),
                ("hour", [column]) => (Transform::Hour, *column),
                ("void", [column]) => (Transform::Void, *column),
                ("bucket", [count, column]) => (Transform::Bucket(width(count)?), *column),
                ("truncate", [width_text, column]) => {
                    (Transform::Truncate(width(width_text)?), *column)
                }
                _ => return Err(unreadable()),
            };

        Ok(Term {
            transform,
            column: column(column_name)?,
        })
    }
}

impl FromStr for SortTerm {
    type Err = Error;

    fn from_str(text: &str) -> Result<SortTerm, Error> {
        let mut rest = text.trim();
        let lowered = rest.to_lowercase();

        let mut null_order = None;
        for (suffix, order) in [
            (" nulls first", NullOrder::First),
            (" nulls last", NullOrder::Last),
        ] {
            if lowered.ends_with(suffix) {
                rest = &rest[..rest.len() - suffix.len()];
                null_order = Some(order);
            }
        }

        let lowered = rest.to_lowercase();
        let mut direction = SortDirection::Ascending;
        for (suffix, given) in [
            (" asc", SortDirection::Ascending),
            (" desc", SortDirection::Descending),
        ] {
            if lowered.ends_with(suffix) {
                rest = &rest[..rest.len() - suffix.len()];
                direction = given;
            }
        }

        let null_order = null_order.unwrap_or(match direction {
            SortDirection::Ascending => NullOrder::First,
            SortDirection::Descending => NullOrder::Last,
        });

        Ok(SortTerm {
            term: rest.parse()?,
            direction,
            null_order,
        })
    }
}

fn column(text: &str) -> Result<String, Error> {
    let valid = !text.is_empty() && !text.contains(char::is_whitespace);

    valid
        .then(|| text.to_owned())
        .ok_or_else(|| Error::UnreadableTerm(text.to_owned()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use mink_core::Record;
    use mink_iceberg::FileSchema;

    use super::*;
    use crate::testing::{Tiering, tiering, unattached};

    fn unpartitioned(location: Option<String>) -> Source {
        Source::new(
            false,
            Some(Vec::new()),
            Vec::new(),
            HashMap::new(),
            location,
            BTreeMap::new(),
        )
        .unwrap()
    }

    fn existing(end_offsets: &[(i32, i64)]) -> Source {
        Source::new(
            true,
            None,
            Vec::new(),
            HashMap::new(),
            None,
            end_offsets.iter().copied().collect(),
        )
        .unwrap()
    }

    fn named(attachment: Attachment, name: &str) -> Attachment {
        Attachment {
            table: CatalogTable::new("fixture", "mink", name),
            ..attachment
        }
    }

    async fn created_by_a_user(tiering: &Tiering, schema: Schema) -> IcebergTable {
        tiering
            .catalog
            .create_table_with_ids(
                &tiering.identifier,
                schema,
                UnboundPartitionSpec::builder().build(),
                SortOrder::unsorted_order(),
                HashMap::new(),
                None,
            )
            .await
            .unwrap()
    }

    async fn appended_by_a_user(tiering: &Tiering, table: &IcebergTable) {
        let partition = mink_core::Partition::new(tiering.topic.name(), 0).unwrap();
        let record = Record::new(0, 0, None, Some(Bytes::from_static(b"x")), Vec::new()).unwrap();
        let rows = mink_iceberg::record_batch(&partition, &[record], &FileSchema::Binary).unwrap();
        let files = mink_iceberg::write_data_files(table, rows, "user")
            .await
            .unwrap();
        let transaction = Transaction::new(table);
        let transaction = transaction
            .fast_append()
            .add_data_files(files)
            .apply(transaction)
            .unwrap();
        transaction.commit(tiering.catalog.as_ref()).await.unwrap();
    }

    async fn start(tiering: &Tiering) -> i64 {
        let latest = tiering
            .services
            .store
            .snapshots()
            .latest(tiering.topic.name())
            .await
            .unwrap()
            .unwrap();

        latest.offsets()[0].start_offset()
    }

    async fn loaded(tiering: &Tiering, name: &str) -> iceberg::Result<IcebergTable> {
        let table = CatalogTable::new("fixture", "mink", name);

        iceberg::Catalog::load_table(tiering.catalog.as_ref(), &identifier(&table)).await
    }

    #[test]
    fn a_source_takes_only_the_options_of_its_kind() {
        let new = |partition, end_offsets: &[(i32, i64)]| {
            Source::new(
                false,
                partition,
                Vec::new(),
                HashMap::new(),
                None,
                end_offsets.iter().copied().collect(),
            )
        };
        let existing_with_location = Source::new(
            true,
            None,
            Vec::new(),
            HashMap::new(),
            Some("s3://b/t".to_owned()),
            BTreeMap::new(),
        );

        assert!(matches!(new(None, &[]), Err(Error::PartitioningRequired)));
        assert!(matches!(
            new(Some(Vec::new()), &[]),
            Ok(Source::New {
                partitioning: Partitioning::Unpartitioned,
                ..
            })
        ));
        assert!(matches!(
            new(Some(Vec::new()), &[(0, 1)]),
            Err(Error::EndOffsetsOfNewTable)
        ));
        assert!(matches!(
            existing_with_location,
            Err(Error::ExistingTableKeeps("location"))
        ));
    }

    #[tokio::test]
    async fn a_new_table_takes_the_topics_schema_and_its_terms() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };

        let table = loaded(&tiering, tiering.topic.name()).await.unwrap();
        let registered = tiering
            .services
            .store
            .external_tables()
            .require(tiering.topic.name())
            .await
            .unwrap();
        let second = format!("{}_second", tiering.topic.name());
        let again = tiering
            .attach(named(
                tiering.attachment(Duration::from_secs(1), unpartitioned(None)),
                &second,
            ))
            .await;

        let metadata = table.metadata();
        let field = &metadata.default_partition_spec().fields()[0];
        assert_eq!(
            metadata.current_schema().as_struct(),
            mink_schema::binary_schema().as_struct()
        );
        let chosen = partition_spec(
            mink_schema::binary_schema(),
            &Partitioning::Fields(vec!["day(_kafka_timestamp)".parse().unwrap()]),
        )
        .unwrap();
        assert_eq!(Some(field.field_id), chosen.fields()[0].field_id);
        assert_eq!(field.transform, Transform::Day);
        assert_eq!(
            metadata.default_sort_order().fields[0].direction,
            SortDirection::Descending
        );
        assert_eq!(registered.uuid(), metadata.uuid());
        assert_eq!(registered.location().path(), metadata.location());
        assert_eq!(registered.generation(), 1);
        assert!(
            matches!(
                again,
                Err(Error::Store(mink_store::Error::AlreadyExternalTable { .. }))
            ),
            "{again:?}"
        );
        assert!(loaded(&tiering, &second).await.is_err());
    }

    #[tokio::test]
    async fn a_short_retention_or_a_location_in_the_warehouse_is_refused() {
        let Some(tiering) = unattached().await else {
            return;
        };
        let interval = Duration::from_secs(3600);
        mink_conformance::set_retention(&tiering.bootstrap, tiering.topic.name(), 60_000).await;
        let short = tiering
            .attach(tiering.attachment(interval, unpartitioned(None)))
            .await;
        mink_conformance::set_retention(&tiering.bootstrap, tiering.topic.name(), -1).await;
        let inside = unpartitioned(Some(format!(
            "{}/elsewhere",
            tiering.services.warehouse.root()
        )));

        let warehouse = tiering.attach(tiering.attachment(interval, inside)).await;

        assert!(
            matches!(short, Err(Error::ShortRetention { .. })),
            "{short:?}"
        );
        assert!(
            matches!(warehouse, Err(Error::LocationInWarehouse(_))),
            "{warehouse:?}"
        );
        assert!(loaded(&tiering, tiering.topic.name()).await.is_err());
    }

    #[tokio::test]
    async fn an_existing_table_is_onboarded_at_a_boundary_its_offsets_reach() {
        let Some(tiering) = unattached().await else {
            return;
        };
        let interval = Duration::from_secs(3600);
        tiering.index_and_commit(&[3, 2]).await;
        let table = created_by_a_user(&tiering, mink_schema::binary_schema().clone()).await;
        appended_by_a_user(&tiering, &table).await;
        let attach = async |offsets: &[(i32, i64)]| {
            tiering
                .attach(tiering.attachment(interval, existing(offsets)))
                .await
        };

        let unknown = attach(&[]).await;
        let negative = attach(&[(0, -5)]).await;
        let inside = attach(&[(0, 4)]).await;
        let beyond = attach(&[(0, 9)]).await;
        let untouched = loaded(&tiering, tiering.topic.name()).await.unwrap();
        let onboarded = attach(&[(0, 3)]).await;
        let mut writer = tiering.writer().await;
        writer.tick().await.unwrap();
        writer.tick().await.unwrap();

        let table = loaded(&tiering, tiering.topic.name()).await.unwrap();
        let committed = tiering
            .services
            .store
            .tiered_ranges()
            .settled(tiering.topic.name())
            .await
            .unwrap();
        assert!(
            matches!(unknown, Err(Error::MissingEndOffsets(_))),
            "{unknown:?}"
        );
        assert!(
            matches!(negative, Err(Error::InvalidEndOffset { offset: -5, .. })),
            "{negative:?}"
        );
        assert!(
            matches!(
                inside,
                Err(Error::Store(mink_store::Error::NotABoundary {
                    offset: 4,
                    ..
                }))
            ),
            "{inside:?}"
        );
        assert!(
            matches!(
                beyond,
                Err(Error::Store(mink_store::Error::EndOffsetBeyondIndex {
                    offset: 9,
                    ..
                }))
            ),
            "{beyond:?}"
        );
        assert!(mink_iceberg::end_offsets(&untouched).unwrap().is_empty());
        assert!(onboarded.is_ok(), "{onboarded:?}");
        assert_eq!(mink_iceberg::end_offsets(&table).unwrap()[&0], 3);
        assert_eq!(committed, BTreeMap::from([(0, 3)]));
        assert_eq!(start(&tiering).await, 3);
    }

    #[tokio::test]
    async fn an_existing_table_of_another_schema_is_refused() {
        let Some(tiering) = unattached().await else {
            return;
        };
        let binary = mink_schema::binary_schema();
        let extra = NestedField::optional(100, "extra", Type::Primitive(PrimitiveType::String));
        let schema = Schema::builder()
            .with_fields(
                binary
                    .as_struct()
                    .fields()
                    .iter()
                    .cloned()
                    .chain([extra.into()]),
            )
            .build()
            .unwrap();
        created_by_a_user(&tiering, schema).await;

        let refused = tiering
            .attach(tiering.attachment(Duration::from_secs(3600), existing(&[])))
            .await;

        assert!(
            matches!(
                refused,
                Err(Error::Iceberg(mink_iceberg::Error::ExternalSchema))
            ),
            "{refused:?}"
        );
    }

    #[tokio::test]
    async fn a_detached_table_is_neither_tiered_nor_listed() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        let ticked_twice = async || {
            let mut writer = tiering.writer().await;
            writer.tick().await.unwrap();
            writer.tick().await.unwrap();
        };
        tiering.index_and_commit(&[3, 2]).await;
        tiering.tier().await.tick().await.unwrap();
        ticked_twice().await;
        let tiered_start = start(&tiering).await;
        tiering.index_and_commit(&[1]).await;
        tiering.tier().await.tick().await.unwrap();

        detach(&tiering.services.store, tiering.topic.name())
            .await
            .unwrap();
        ticked_twice().await;

        let ranges = tiering.services.store.tiered_ranges();
        assert_eq!(tiered_start, 6);
        assert_eq!(start(&tiering).await, 0);
        assert!(ranges.list(tiering.topic.name()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_stale_tier_worker_cannot_record_into_the_next_attachment() {
        let Some(tiering) = tiering(Duration::from_secs(1)).await else {
            return;
        };
        tiering.index_and_commit(&[3]).await;
        let mut stale = tiering.tier().await;
        detach(&tiering.services.store, tiering.topic.name())
            .await
            .unwrap();
        let second = format!("{}_second", tiering.topic.name());
        tiering
            .attach(named(
                tiering.attachment(Duration::from_secs(1), unpartitioned(None)),
                &second,
            ))
            .await
            .unwrap();
        let mut writer = tiering.writer().await;
        writer.tick().await.unwrap();
        writer.tick().await.unwrap();

        let ticked = stale.tick().await;

        let ranges = tiering.services.store.tiered_ranges();
        assert!(
            matches!(
                ticked,
                Err(Error::Store(mink_store::Error::DetachedGeneration {
                    generation: 1,
                    ..
                }))
            ),
            "{ticked:?}"
        );
        assert!(ranges.list(tiering.topic.name()).await.unwrap().is_empty());
        assert_eq!(start(&tiering).await, 0);
    }

    #[test]
    fn a_location_is_a_path_in_a_bucket_outside_the_warehouse() {
        let warehouse = "s3://mink/warehouse";

        assert!(external_location(warehouse, "s3://lake/orders").is_ok());
        assert!(matches!(
            external_location(warehouse, "s3://lake"),
            Err(Error::Core(mink_core::Error::ExternalLocation(_)))
        ));
        assert!(matches!(
            external_location(warehouse, "s3://mink/warehouse/orders"),
            Err(Error::LocationInWarehouse(_))
        ));
    }

    #[test]
    fn terms_read_iceberg_transforms() {
        let term = |text: &str| text.parse::<Term>().unwrap();

        assert_eq!(
            term("day(_kafka_timestamp)"),
            Term {
                transform: Transform::Day,
                column: "_kafka_timestamp".to_owned()
            }
        );
        assert_eq!(
            term("bucket(16, _kafka_key)").transform,
            Transform::Bucket(16)
        );
        assert_eq!(term("truncate(4, payload.region)").column, "payload.region");
        assert_eq!(term("region").transform, Transform::Identity);
        assert!("bucket(x, a)".parse::<Term>().is_err());
        assert!("day(a, b)".parse::<Term>().is_err());
        assert!("decade(a)".parse::<Term>().is_err());
        assert!("".parse::<Term>().is_err());
    }

    #[test]
    fn sort_terms_read_direction_and_null_order() {
        let sort = |text: &str| text.parse::<SortTerm>().unwrap();

        assert_eq!(
            sort("_kafka_key"),
            SortTerm {
                term: "_kafka_key".parse().unwrap(),
                direction: SortDirection::Ascending,
                null_order: NullOrder::First
            }
        );
        assert_eq!(sort("_kafka_key desc").null_order, NullOrder::Last);
        assert_eq!(
            sort("day(_kafka_timestamp) DESC NULLS FIRST"),
            SortTerm {
                term: "day(_kafka_timestamp)".parse().unwrap(),
                direction: SortDirection::Descending,
                null_order: NullOrder::First
            }
        );
    }

    #[test]
    fn partitioning_takes_ids_after_the_time_fields_and_checks_types() {
        let schema = mink_schema::binary_schema();
        let terms = vec![
            "day(_kafka_timestamp)".parse().unwrap(),
            "bucket(8, _kafka_key)".parse().unwrap(),
        ];

        let spec = partition_spec(schema, &Partitioning::Fields(terms)).unwrap();
        let unpartitioned = partition_spec(schema, &Partitioning::Unpartitioned).unwrap();
        let mistyped = partition_spec(
            schema,
            &Partitioning::Fields(vec!["day(_kafka_key)".parse().unwrap()]),
        );
        let unknown = partition_spec(
            schema,
            &Partitioning::Fields(vec!["nothing".parse().unwrap()]),
        );

        let ids: Vec<i32> = spec
            .fields()
            .iter()
            .filter_map(|field| field.field_id)
            .collect();
        assert_eq!(ids, [ids[0], ids[0] + 1]);
        assert!(unpartitioned.fields().is_empty());
        assert!(mistyped.is_err());
        assert!(
            matches!(unknown, Err(Error::UnknownColumn(_))),
            "{unknown:?}"
        );
    }
}
