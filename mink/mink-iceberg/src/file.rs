use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type,
    Time64MicrosecondType, TimestampMicrosecondType,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use bytes::Bytes;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use std::collections::BTreeMap;

use mink_core::{
    BuiltFile, DaySketches, File, FileVersion, Granularity, IndexedFile, Record, Sketch, Span,
    Stats, TimeSpecs, utc_day,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

use crate::Error;
use crate::framing::{footer, padded};
use crate::rows::record_batch;
use crate::schema::FileSchema;

pub(crate) const STRING_BOUND: usize = 16;

#[derive(Clone, Copy)]
enum Bounds {
    Collected,
    Skipped,
}

type Extremes = (Vec<u8>, Vec<u8>);

struct StatsBuilder<'a> {
    stats: Stats,
    sketch_builder: Option<SketchBuilder<'a>>,
}

struct SketchBuilder<'a> {
    days: &'a [i64],
    sketches: DaySketches,
}

pub fn build_file(
    file: &File,
    records: &[Record],
    granularity: Granularity,
    schema: &FileSchema,
) -> Result<(Bytes, BuiltFile), Error> {
    let (records, rows, bytes, body_length) = parquet_file(file, records, granularity, schema)?;

    let stats_builder = collected(schema.schema(), &rows, bytes.len(), &records, None)?;
    let built = built_file(file, &bytes, body_length, stats_builder.stats)?;
    Ok((bytes, built))
}

pub(crate) fn index_file(
    file: &File,
    records: &[Record],
    granularity: Granularity,
    schema: &FileSchema,
) -> Result<(BuiltFile, DaySketches), Error> {
    let (records, rows, bytes, body_length) = parquet_file(file, records, granularity, schema)?;
    let days: Vec<i64> = records
        .iter()
        .map(|record| utc_day(record.timestamp_ms()))
        .collect();
    let sketch_builder = SketchBuilder {
        days: &days,
        sketches: DaySketches::default(),
    };

    let stats_builder = collected(
        schema.schema(),
        &rows,
        bytes.len(),
        &records,
        Some(sketch_builder),
    )?;
    let sketches = stats_builder
        .sketch_builder
        .map(|sketch_builder| sketch_builder.sketches)
        .unwrap_or_default();
    Ok((
        built_file(file, &bytes, body_length, stats_builder.stats)?,
        sketches,
    ))
}

fn built_file(
    file: &File,
    bytes: &[u8],
    body_length: u64,
    stats: Stats,
) -> Result<BuiltFile, Error> {
    let footer = footer(bytes)?;
    let span = Span::new(file.range(), body_length);
    let indexed = IndexedFile::new(file.clone(), stats);

    Ok(BuiltFile::new(
        indexed,
        Bytes::copy_from_slice(footer),
        vec![span],
    )?)
}

pub fn file_contents(
    file: &File,
    records: &[Record],
    specs: &TimeSpecs,
    schema: &FileSchema,
) -> Result<Option<Bytes>, Error> {
    let Some(spec) = specs.get(file.time().spec_id()) else {
        return Ok(None);
    };
    let granularity = spec.granularity();

    match build_file(file, records, granularity, schema) {
        Ok((bytes, _)) => Ok(Some(bytes)),
        Err(Error::NoRecords(_) | Error::Schema(mink_schema::Error::UnmergedSchema { .. })) => {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn parquet_file(
    file: &File,
    records: &[Record],
    granularity: Granularity,
    schema: &FileSchema,
) -> Result<(Vec<Record>, RecordBatch, Bytes, u64), Error> {
    let records: Vec<Record> = records
        .iter()
        .filter(|record| granularity.bucket(record.timestamp_ms()) == file.time().bucket())
        .cloned()
        .collect();

    check(file, &records)?;
    if schema.id() != file.schema_id() {
        return Err(Error::FileSchema {
            file: file.clone(),
            schema_id: schema.id(),
        });
    }

    let rows = record_batch(file.partition(), &records, schema)?;

    let options = ArrowWriterOptions::new()
        .with_properties(properties(file.version())?)
        .with_skip_arrow_metadata(true);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(&mut bytes, rows.schema(), options)?;
    writer.write(&rows)?;
    writer.close()?;

    let (bytes, body_length) = padded(bytes)?;

    Ok((records, rows, Bytes::from(bytes), body_length))
}

fn check(file: &File, records: &[Record]) -> Result<(), Error> {
    if records.is_empty() {
        return Err(Error::NoRecords(file.clone()));
    }

    let mut previous = None;
    for record in records {
        let offset = record.offset();
        if offset < file.start_offset() || offset >= file.end_offset() {
            return Err(Error::RecordOutsideFile { offset });
        }

        if previous.is_some_and(|previous| offset <= previous) {
            return Err(Error::RecordOutOfOrder { offset });
        }

        previous = Some(offset);
    }

    Ok(())
}

fn properties(version: FileVersion) -> Result<WriterProperties, Error> {
    match version {
        FileVersion::V1 => Ok(WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_row_count(Some(131_072))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_offset_index_disabled(true)
            .set_dictionary_enabled(true)
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_created_by(format!("mink file version {}", version.number()))
            .build()),
    }
}

fn collected<'a>(
    schema: &Schema,
    rows: &RecordBatch,
    bytes: usize,
    records: &[Record],
    sketch_builder: Option<SketchBuilder<'a>>,
) -> Result<StatsBuilder<'a>, Error> {
    let first_timestamp_ms = records
        .iter()
        .map(Record::timestamp_ms)
        .min()
        .unwrap_or_default();
    let mut built = StatsBuilder {
        stats: Stats::new(rows.num_rows() as u64, bytes as u64, first_timestamp_ms),
        sketch_builder,
    };

    for (field, column) in schema.as_struct().fields().iter().zip(rows.columns()) {
        built.field(field, column, Bounds::Collected)?;
    }

    Ok(built)
}

impl StatsBuilder<'_> {
    fn field(
        &mut self,
        field: &NestedField,
        column: &ArrayRef,
        bounds: Bounds,
    ) -> Result<(), Error> {
        match field.field_type.as_ref() {
            Type::Primitive(primitive) => {
                self.stats.set_nulls(field.id, column.null_count() as u64);

                if let Some(nans) = nans(primitive, column) {
                    self.stats.set_nans(field.id, nans);
                }

                if matches!(bounds, Bounds::Collected)
                    && let Some((lower, upper)) = self.column(field.id, primitive, column)?
                {
                    self.stats.set_bounds(field.id, lower, upper);
                }

                Ok(())
            }
            Type::Struct(structure) => {
                for (child, column) in structure.fields().iter().zip(column.as_struct().columns()) {
                    self.field(child, column, bounds)?;
                }

                Ok(())
            }
            Type::List(list) => {
                let elements = column.as_list::<i32>().values();

                self.field(&list.element_field, elements, Bounds::Skipped)
            }
            Type::Map(map) => {
                let entries = column.as_map();

                self.field(&map.key_field, entries.keys(), Bounds::Skipped)?;
                self.field(&map.value_field, entries.values(), Bounds::Skipped)
            }
        }
    }

    fn column(
        &mut self,
        field_id: i32,
        primitive: &PrimitiveType,
        column: &ArrayRef,
    ) -> Result<Option<Extremes>, Error> {
        let bounds = match primitive {
            PrimitiveType::Boolean => {
                let encode = |value: &bool| vec![u8::from(*value)];
                let values = self.values(field_id, column.as_boolean().iter(), encode)?;

                extremes(&values, encode)
            }
            PrimitiveType::Int => {
                let encode = |value: &i32| value.to_le_bytes().to_vec();
                let values =
                    self.values(field_id, column.as_primitive::<Int32Type>().iter(), encode)?;

                extremes(&values, encode)
            }
            PrimitiveType::Date => {
                let encode = |value: &i32| value.to_le_bytes().to_vec();
                let values =
                    self.values(field_id, column.as_primitive::<Date32Type>().iter(), encode)?;

                extremes(&values, encode)
            }
            PrimitiveType::Long => {
                let encode = |value: &i64| value.to_le_bytes().to_vec();
                let values =
                    self.values(field_id, column.as_primitive::<Int64Type>().iter(), encode)?;

                extremes(&values, encode)
            }
            PrimitiveType::Time => {
                let encode = |value: &i64| value.to_le_bytes().to_vec();
                let times = column.as_primitive::<Time64MicrosecondType>();
                let values = self.values(field_id, times.iter(), encode)?;

                extremes(&values, encode)
            }
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                let encode = |value: &i64| value.to_le_bytes().to_vec();
                let timestamps = column.as_primitive::<TimestampMicrosecondType>();
                let values = self.values(field_id, timestamps.iter(), encode)?;

                extremes(&values, encode)
            }
            PrimitiveType::Float => {
                let encode = |value: &f32| value.to_le_bytes().to_vec();
                let floats = column.as_primitive::<Float32Type>();
                let values = self.values(field_id, floats.iter(), encode)?;

                let numbers = values.iter().copied().filter(|value| !value.is_nan());
                let lower = numbers.clone().min_by(f32::total_cmp);
                let upper = numbers.max_by(f32::total_cmp);
                lower
                    .zip(upper)
                    .map(|(lower, upper)| (encode(&lower), encode(&upper)))
            }
            PrimitiveType::Double => {
                let encode = |value: &f64| value.to_le_bytes().to_vec();
                let doubles = column.as_primitive::<Float64Type>();
                let values = self.values(field_id, doubles.iter(), encode)?;

                let numbers = values.iter().copied().filter(|value| !value.is_nan());
                let lower = numbers.clone().min_by(f64::total_cmp);
                let upper = numbers.max_by(f64::total_cmp);
                lower
                    .zip(upper)
                    .map(|(lower, upper)| (encode(&lower), encode(&upper)))
            }
            PrimitiveType::Decimal { .. } => {
                let encode = |value: &i128| minimal_bytes(*value);
                let decimals = column.as_primitive::<Decimal128Type>();
                let values = self.values(field_id, decimals.iter(), encode)?;

                extremes(&values, encode)
            }
            PrimitiveType::Uuid => {
                let encode = |value: &&[u8]| value.to_vec();
                let uuids = column.as_fixed_size_binary();
                let values = self.values(field_id, uuids.iter(), encode)?;

                extremes(&values, encode)
            }
            PrimitiveType::String => {
                let encode = |value: &&str| value.as_bytes().to_vec();
                let strings = column.as_string::<i32>();
                let values = self.values(field_id, strings.iter(), encode)?;

                let lower = values.iter().min();
                let upper = values
                    .iter()
                    .max()
                    .and_then(|upper| raised(upper, STRING_BOUND));
                lower.zip(upper).map(|(lower, upper)| {
                    (
                        truncated(lower, STRING_BOUND).into_bytes(),
                        upper.into_bytes(),
                    )
                })
            }
            PrimitiveType::Binary => {
                let encode = |value: &&[u8]| value.to_vec();
                self.values(field_id, column.as_binary::<i64>().iter(), encode)?;

                None
            }
            PrimitiveType::Fixed(_) => {
                let encode = |value: &&[u8]| value.to_vec();
                self.values(field_id, column.as_fixed_size_binary().iter(), encode)?;

                None
            }
            _ => None,
        };

        Ok(bounds)
    }

    fn values<T>(
        &mut self,
        field_id: i32,
        values: impl Iterator<Item = Option<T>>,
        encode: impl Fn(&T) -> Vec<u8>,
    ) -> Result<Vec<T>, Error> {
        let mut by_day: BTreeMap<i64, Sketch> = BTreeMap::new();
        let mut present = Vec::new();

        for (row, value) in values.enumerate() {
            let Some(value) = value else {
                continue;
            };

            if let Some(sketch_builder) = &self.sketch_builder {
                let day = sketch_builder.days[row];

                by_day.entry(day).or_default().update(&encode(&value));
            }
            present.push(value);
        }

        if let Some(sketch_builder) = &mut self.sketch_builder {
            for (day, sketch) in by_day {
                sketch_builder
                    .sketches
                    .insert(day, field_id, sketch.serialize())?;
            }
        }

        Ok(present)
    }
}

fn nans(primitive: &PrimitiveType, column: &ArrayRef) -> Option<u64> {
    let count = match primitive {
        PrimitiveType::Float => column
            .as_primitive::<Float32Type>()
            .iter()
            .flatten()
            .filter(|value| value.is_nan())
            .count(),
        PrimitiveType::Double => column
            .as_primitive::<Float64Type>()
            .iter()
            .flatten()
            .filter(|value| value.is_nan())
            .count(),
        _ => return None,
    };

    Some(count as u64)
}

fn extremes<T: Ord>(values: &[T], encode: impl Fn(&T) -> Vec<u8>) -> Option<Extremes> {
    let lower = values.iter().min()?;
    let upper = values.iter().max()?;

    Some((encode(lower), encode(upper)))
}

fn minimal_bytes(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let sign = if value < 0 { 0xff } else { 0x00 };

    let mut start = 0;
    while start < bytes.len() - 1
        && bytes[start] == sign
        && (bytes[start + 1] & 0x80) == (sign & 0x80)
    {
        start += 1;
    }

    bytes[start..].to_vec()
}

pub(crate) fn truncated(value: &str, length: usize) -> String {
    value.chars().take(length).collect()
}

pub(crate) fn raised(value: &str, length: usize) -> Option<String> {
    if value.chars().count() <= length {
        return Some(value.to_owned());
    }

    let mut chars: Vec<char> = value.chars().take(length).collect();
    while let Some(last) = chars.pop() {
        let next = (u32::from(last) + 1..=u32::from(char::MAX)).find_map(char::from_u32);

        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use mink_core::{Header, Partition, TimePartition};
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};

    use std::collections::BTreeSet;
    use std::sync::Arc;

    use iceberg::spec::Datum;
    use mink_core::Part;
    use mink_schema::{
        Decoders, FieldIds, PartSchemaId, PayloadSchema, RegistrySchema, SchemaFormat, SchemaId,
        SchemaKey, SchemaSet, binary_schema, merge,
    };

    use super::*;
    use crate::schema::{BINARY_ARROW, TypedSchema};
    use crate::testing::{hour_records, hourly};

    fn file(partition: i32, start: i64, end: i64) -> File {
        let partition = Partition::new("orders", partition).unwrap();

        File::v1(partition, start, end, TimePartition::new(0, None), 0).unwrap()
    }

    fn record(
        offset: i64,
        timestamp_ms: i64,
        key: Option<&'static [u8]>,
        value: Option<&'static [u8]>,
        headers: Vec<Header>,
    ) -> Record {
        Record::new(
            offset,
            timestamp_ms,
            key.map(Bytes::from_static),
            value.map(Bytes::from_static),
            headers,
        )
        .unwrap()
    }

    #[test]
    fn files_match_the_data_fixtures() {
        let fixtures = mink_conformance::data_fixtures();
        assert!(!fixtures.is_empty(), "no data fixtures");

        for fixture in fixtures {
            let written = build_file(
                fixture.file(),
                fixture.records(),
                Granularity::None,
                &FileSchema::Binary,
            )
            .unwrap();
            let again = build_file(
                fixture.file(),
                fixture.records(),
                Granularity::None,
                &FileSchema::Binary,
            )
            .unwrap();

            assert!(
                written.0 == fixture.bytes(),
                "{}: the bytes differ from the fixture",
                fixture.name()
            );
            assert_eq!(
                written.1.indexed().stats(),
                &fixture.stats(),
                "{}",
                fixture.name()
            );
            assert_eq!(written, again, "{}", fixture.name());
        }
    }

    #[test]
    fn stats_follow_the_rows() {
        let records = [
            record(
                40,
                2,
                Some(b"k"),
                None,
                vec![
                    Header::new("a", Some(Bytes::from_static(b"1"))),
                    Header::new("a", None),
                ],
            ),
            record(41, 1, None, Some(b"v"), Vec::new()),
            record(43, 3, None, None, vec![Header::new("b", None)]),
        ];

        let (bytes, built) = build_file(
            &file(7, 40, 44),
            &records,
            Granularity::None,
            &FileSchema::Binary,
        )
        .unwrap();
        let stats = built.indexed().stats().clone();

        let expected = Stats::new(3, bytes.len() as u64, 1)
            .with_bounds(1, 7i32.to_le_bytes().to_vec(), 7i32.to_le_bytes().to_vec())
            .with_bounds(
                2,
                40i64.to_le_bytes().to_vec(),
                43i64.to_le_bytes().to_vec(),
            )
            .with_bounds(
                3,
                1_000i64.to_le_bytes().to_vec(),
                3_000i64.to_le_bytes().to_vec(),
            )
            .with_nulls(1, 0)
            .with_nulls(2, 0)
            .with_nulls(3, 0)
            .with_nulls(4, 2)
            .with_nulls(7, 0)
            .with_nulls(8, 2)
            .with_nulls(9, 2);
        assert_eq!(stats, expected);
    }

    #[test]
    fn files_read_back_to_their_rows() {
        let records: Vec<_> = (0..131_073)
            .map(|offset| {
                let headers = if offset % 3 == 0 {
                    vec![Header::new("h", None)]
                } else {
                    Vec::new()
                };
                record(
                    offset,
                    offset,
                    (offset % 2 == 0).then_some(&b"k"[..]),
                    Some(b"v"),
                    headers,
                )
            })
            .collect();
        let file = file(0, 0, 131_073);

        let (bytes, _) =
            build_file(&file, &records, Granularity::None, &FileSchema::Binary).unwrap();

        let options = ArrowReaderOptions::new().with_schema(BINARY_ARROW.schema.clone());
        let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, options).unwrap();
        let metadata = reader.metadata().clone();
        let footer = metadata.file_metadata();

        let ids: Vec<_> = footer
            .schema_descr()
            .columns()
            .iter()
            .map(|column| column.self_type().get_basic_info().id())
            .collect();
        let compressions: Vec<_> = metadata
            .row_groups()
            .iter()
            .flat_map(|group| group.columns())
            .map(|column| column.compression())
            .collect();
        let row_groups: Vec<_> = metadata
            .row_groups()
            .iter()
            .map(|group| group.num_rows())
            .collect();

        let expected = record_batch(file.partition(), &records, &FileSchema::Binary).unwrap();
        let mut read = 0;
        for batch in reader.build().unwrap() {
            let batch = batch.unwrap();
            assert_eq!(
                batch.columns(),
                expected.slice(read, batch.num_rows()).columns()
            );
            read += batch.num_rows();
        }

        assert_eq!(read, records.len());
        assert_eq!(row_groups, [131_072, 1]);
        assert_eq!(ids, [1, 2, 3, 4, 7, 8, 9]);
        assert!(
            compressions
                .iter()
                .all(|compression| matches!(compression, Compression::ZSTD(_)))
        );
        assert_eq!(footer.version(), 1);
        assert_eq!(footer.created_by(), Some("mink file version 1"));
        assert_eq!(footer.key_value_metadata(), None);
    }

    #[test]
    fn a_file_needs_at_least_one_record() {
        let written = build_file(&file(0, 0, 10), &[], Granularity::None, &FileSchema::Binary);

        assert!(matches!(written, Err(Error::NoRecords(_))), "{written:?}");
    }

    #[test]
    fn records_must_lie_inside_their_file_in_offset_order() {
        let outside = build_file(
            &file(0, 5, 10),
            &[record(10, 0, None, None, Vec::new())],
            Granularity::None,
            &FileSchema::Binary,
        );
        let before = build_file(
            &file(0, 5, 10),
            &[record(4, 0, None, None, Vec::new())],
            Granularity::None,
            &FileSchema::Binary,
        );
        let repeated = build_file(
            &file(0, 5, 10),
            &[
                record(6, 0, None, None, Vec::new()),
                record(6, 0, None, None, Vec::new()),
            ],
            Granularity::None,
            &FileSchema::Binary,
        );

        assert!(
            matches!(outside, Err(Error::RecordOutsideFile { offset: 10 })),
            "{outside:?}"
        );
        assert!(
            matches!(before, Err(Error::RecordOutsideFile { offset: 4 })),
            "{before:?}"
        );
        assert!(
            matches!(repeated, Err(Error::RecordOutOfOrder { offset: 6 })),
            "{repeated:?}"
        );
    }

    #[test]
    fn typed_files_carry_bounds_for_their_primitive_columns() {
        let definition = r#"{"type":"record","name":"R","fields":[
            {"name":"s","type":"string"},
            {"name":"d","type":"double"},
            {"name":"opt","type":["null","long"]}]}"#;
        let registered = RegistrySchema::new(
            SchemaKey::Confluent(1),
            SchemaFormat::Avro,
            definition,
            Vec::new(),
        );
        let payload =
            Arc::new(PayloadSchema::parse(&SchemaSet::new(registered, Vec::new())).unwrap());
        let merged = merge(
            binary_schema(),
            &FieldIds::default(),
            Part::Value,
            &payload,
            SchemaKey::Confluent(1),
        )
        .unwrap();
        let mut decoders = Decoders::default();
        let schema_id = PartSchemaId::new(Part::Value, SchemaId::Key(SchemaKey::Confluent(1)));
        decoders.insert_typed(schema_id, payload, merged.field_ids().clone());
        let schema = merged.schema().unwrap().clone();

        let long = b"abcdefghijklmnopqrstuvwxyz";
        let first: Vec<u8> = [0, 0, 0, 0, 1, 52]
            .into_iter()
            .chain(long.iter().copied())
            .chain(f64::NAN.to_le_bytes())
            .chain([0])
            .collect();
        let second: Vec<u8> = [0, 0, 0, 0, 1, 2, b'b']
            .into_iter()
            .chain(2.5f64.to_le_bytes())
            .chain([2, 14])
            .collect();
        let records = [
            Record::new(0, 0, None, Some(Bytes::from(first)), Vec::new()).unwrap(),
            Record::new(1, 0, None, Some(Bytes::from(second)), Vec::new()).unwrap(),
        ];
        let file = File::v1(
            Partition::new("orders", 0).unwrap(),
            0,
            2,
            TimePartition::new(0, None),
            1,
        )
        .unwrap();

        let typed = FileSchema::Typed(Box::new(TypedSchema::new(schema, decoders).unwrap()));
        let (_, built) = build_file(&file, &records, Granularity::None, &typed).unwrap();
        let stats = built.indexed().stats();

        assert_eq!(stats.lower()[&10], b"abcdefghijklmnop");
        assert_eq!(stats.upper()[&10], b"b");
        assert_eq!(stats.nans()[&11], 1);
        assert_eq!(stats.lower()[&11], 2.5f64.to_le_bytes());
        assert_eq!(stats.nulls()[&12], 1);
        assert_eq!(stats.upper()[&12], 7i64.to_le_bytes());
        assert_eq!(stats.nulls()[&9], 2);
    }

    #[test]
    fn a_range_splits_into_one_file_per_bucket() {
        let records = hour_records(0..10, |offset| if offset == 6 { 4 } else { 5 });

        let buckets = Granularity::Hour.buckets(&records);
        let (_, late) = build_file(
            &hourly(0, 10, 4),
            &records,
            Granularity::Hour,
            &FileSchema::Binary,
        )
        .unwrap();
        let (_, current) = build_file(
            &hourly(0, 10, 5),
            &records,
            Granularity::Hour,
            &FileSchema::Binary,
        )
        .unwrap();
        let missing = build_file(
            &hourly(0, 10, 3),
            &records,
            Granularity::Hour,
            &FileSchema::Binary,
        );

        assert_eq!(buckets, BTreeSet::from([Some(4), Some(5)]));
        let records = |built: &BuiltFile| built.indexed().stats().records();
        assert_eq!((records(&late), records(&current)), (1, 9));
        assert!(matches!(missing, Err(Error::NoRecords(_))));
    }

    #[test]
    fn values_update_as_their_single_value_encoding() {
        let records = hour_records(0..3, |_| 5);
        let file = hourly(0, 3, 5);

        let (_, sketches) =
            index_file(&file, &records, Granularity::Hour, &FileSchema::Binary).unwrap();

        let by_field: BTreeMap<i32, &[u8]> = sketches
            .iter()
            .map(|sketch| (sketch.field_id(), sketch.sketch()))
            .collect();
        let expected = |datums: Vec<Datum>| {
            let mut sketch = Sketch::new();
            for datum in datums {
                sketch.update(&datum.to_bytes().unwrap());
            }
            sketch.serialize()
        };
        let offsets = (0..3).map(Datum::long).collect();
        let timestamps = (0..3)
            .map(|offset| Datum::timestamptz_micros((5 * 3_600_000 + offset) * 1000))
            .collect();
        let values = (0..3)
            .map(|offset| Datum::binary(format!("value-{offset}").into_bytes()))
            .collect();

        assert_eq!(by_field[&2], expected(offsets).as_slice());
        assert_eq!(by_field[&3], expected(timestamps).as_slice());
        assert_eq!(by_field[&9], expected(values).as_slice());
        assert_eq!(by_field[&1], expected(vec![Datum::int(0)]).as_slice());
        assert!(!by_field.contains_key(&4));
    }
}
