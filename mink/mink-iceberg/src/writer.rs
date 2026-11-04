use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_ord::sort::{SortColumn, SortOptions, lexsort_to_indices};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{
    DataFile, DataFileBuilder, DataFileFormat, Datum, NullOrder, PartitionKey, PrimitiveLiteral,
    Schema, SortDirection, SortOrder, Type, deserialize_data_file_from_json,
    serialize_data_file_to_json,
};
use iceberg::table::Table;
use iceberg::transform::create_transform_function;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use uuid::Uuid;

use crate::Error;
use crate::file::{STRING_BOUND, raised, truncated};

const BLOOM_ENABLED: &str = "write.parquet.bloom-filter-enabled.column.";
const BLOOM_FPP: &str = "write.parquet.bloom-filter-fpp.column.";
const METRICS_DEFAULT: &str = "write.metadata.metrics.default";
const METRICS_COLUMN: &str = "write.metadata.metrics.column.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricsMode {
    None,
    Counts,
    Truncate(usize),
    Full,
}

pub async fn write_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    prefix: &str,
) -> Result<Vec<DataFile>, Error> {
    let metadata = table.metadata();
    let schema = metadata.current_schema().clone();
    let properties = metadata.properties();
    let order = metadata.default_sort_order();
    let Some(arrow_schema) = batches.first().map(RecordBatch::schema) else {
        return Ok(Vec::new());
    };

    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        schema.clone(),
        metadata.default_partition_spec().clone(),
    )?;
    let mut parts: Vec<(PartitionKey, Vec<RecordBatch>)> = Vec::new();
    for batch in batches {
        for (key, part) in splitter.split(&batch)? {
            match parts
                .iter_mut()
                .find(|(known, _)| known.data() == key.data())
            {
                Some((_, batches)) => batches.push(part),
                None => parts.push((key, vec![part])),
            }
        }
    }

    let parquet =
        ParquetWriterBuilder::new(writer_properties(properties, &schema)?, schema.clone());
    let suffix = Uuid::new_v4().simple().to_string();
    let names =
        DefaultFileNameGenerator::new(prefix.to_owned(), Some(suffix), DataFileFormat::Parquet);
    let rolling = RollingFileWriterBuilder::new(
        parquet,
        metadata.table_properties()?.write_target_file_size_bytes,
        table.file_io().clone(),
        DefaultLocationGenerator::new(metadata)?,
        names,
    );
    let mut writer = ClusteredWriter::new(DataFileWriterBuilder::new(rolling));

    for (key, batches) in parts {
        let rows = concat_batches(&arrow_schema, &batches)?;
        drop(batches);
        writer.write(key, sorted(&rows, order)?).await?;
    }
    let written = writer.close().await?;

    let spec_id = metadata.default_partition_spec().spec_id();
    let sort_order_id = (!order.is_unsorted()).then_some(order.order_id as i32);
    written
        .into_iter()
        .map(|file| with_metrics_modes(file, &schema, properties, spec_id, sort_order_id))
        .collect()
}

pub fn encoded_data_files(table: &Table, files: Vec<DataFile>) -> Result<Vec<String>, Error> {
    let metadata = table.metadata();
    let partition_type = metadata
        .default_partition_spec()
        .partition_type(metadata.current_schema())?;

    files
        .into_iter()
        .map(|file| {
            Ok(serialize_data_file_to_json(
                file,
                &partition_type,
                metadata.format_version(),
            )?)
        })
        .collect()
}

pub fn data_files(table: &Table, spec_id: i32, encoded: &[String]) -> Result<Vec<DataFile>, Error> {
    let metadata = table.metadata();
    let spec = metadata
        .partition_spec_by_id(spec_id)
        .ok_or(Error::UnknownPartitionSpec(spec_id))?;
    let partition_type = spec.partition_type(metadata.current_schema())?;

    encoded
        .iter()
        .map(|json| {
            Ok(deserialize_data_file_from_json(
                json,
                spec_id,
                &partition_type,
                metadata.current_schema(),
            )?)
        })
        .collect()
}

fn writer_properties(
    properties: &HashMap<String, String>,
    schema: &Schema,
) -> Result<WriterProperties, Error> {
    let size = |key: &str, default: usize| -> Result<usize, Error> {
        match properties.get(key) {
            Some(value) => value.parse().map_err(|_| Error::table_property(key, value)),
            None => Ok(default),
        }
    };

    let mut builder = WriterProperties::builder()
        .set_compression(compression(properties)?)
        .set_max_row_group_bytes(Some(size(
            "write.parquet.row-group-size-bytes",
            128 * 1024 * 1024,
        )?))
        .set_data_page_size_limit(size("write.parquet.page-size-bytes", 1024 * 1024)?)
        .set_dictionary_page_size_limit(size("write.parquet.dict-size-bytes", 2 * 1024 * 1024)?);

    for (key, value) in properties {
        if let Some(column) = key.strip_prefix(BLOOM_ENABLED) {
            let path =
                parquet_path(schema, column).ok_or_else(|| Error::table_property(key, value))?;
            builder = builder.set_column_bloom_filter_enabled(path, value == "true");
        } else if let Some(column) = key.strip_prefix(BLOOM_FPP) {
            let path =
                parquet_path(schema, column).ok_or_else(|| Error::table_property(key, value))?;
            let fpp = value
                .parse()
                .map_err(|_| Error::table_property(key, value))?;
            builder = builder.set_column_bloom_filter_fpp(path, fpp);
        }
    }

    Ok(builder.build())
}

fn parquet_path(schema: &Schema, column: &str) -> Option<ColumnPath> {
    let mut path = Vec::new();
    let mut fields = schema.as_struct();
    let mut current: Option<&Type> = None;

    for name in column.split('.') {
        let field_type = match current {
            None | Some(Type::Struct(_)) => {
                let field = fields.field_by_name(name)?;
                path.push(name.to_owned());
                field.field_type.as_ref()
            }
            Some(Type::List(list)) if name == "element" => {
                path.extend(["list".to_owned(), "element".to_owned()]);
                list.element_field.field_type.as_ref()
            }
            Some(Type::Map(map)) if name == "key" => {
                path.extend(["key_value".to_owned(), "key".to_owned()]);
                map.key_field.field_type.as_ref()
            }
            Some(Type::Map(map)) if name == "value" => {
                path.extend(["key_value".to_owned(), "value".to_owned()]);
                map.value_field.field_type.as_ref()
            }
            Some(_) => return None,
        };

        if let Type::Struct(nested) = field_type {
            fields = nested;
        }
        current = Some(field_type);
    }

    matches!(current, Some(Type::Primitive(_))).then(|| ColumnPath::new(path))
}

fn compression(properties: &HashMap<String, String>) -> Result<Compression, Error> {
    let key = "write.parquet.compression-codec";
    let codec = properties.get(key).map_or("zstd", String::as_str);
    let level = properties
        .get("write.parquet.compression-level")
        .map(|level| {
            level
                .parse::<i32>()
                .map_err(|_| Error::table_property("write.parquet.compression-level", level))
        })
        .transpose()?;

    let invalid = || Error::table_property(key, codec);
    match codec {
        "zstd" => Ok(Compression::ZSTD(match level {
            Some(level) => ZstdLevel::try_new(level).map_err(|_| invalid())?,
            None => ZstdLevel::default(),
        })),
        "gzip" => Ok(Compression::GZIP(match level {
            Some(level) => GzipLevel::try_new(level as u32).map_err(|_| invalid())?,
            None => GzipLevel::default(),
        })),
        "brotli" => Ok(Compression::BROTLI(match level {
            Some(level) => BrotliLevel::try_new(level as u32).map_err(|_| invalid())?,
            None => BrotliLevel::default(),
        })),
        "snappy" => Ok(Compression::SNAPPY),
        "lz4" => Ok(Compression::LZ4_RAW),
        "uncompressed" => Ok(Compression::UNCOMPRESSED),
        _ => Err(invalid()),
    }
}

fn sorted(rows: &RecordBatch, order: &SortOrder) -> Result<RecordBatch, Error> {
    if order.is_unsorted() {
        return Ok(rows.clone());
    }

    let mut columns = Vec::with_capacity(order.fields.len());
    for field in &order.fields {
        let index =
            column_index(rows, field.source_id).ok_or_else(|| Error::SortField(field.source_id))?;
        let values =
            create_transform_function(&field.transform)?.transform(rows.column(index).clone())?;

        columns.push(SortColumn {
            values,
            options: Some(SortOptions {
                descending: field.direction == SortDirection::Descending,
                nulls_first: field.null_order == NullOrder::First,
            }),
        });
    }

    let indices = lexsort_to_indices(&columns, None)?;
    Ok(take_record_batch(rows, &indices)?)
}

fn column_index(rows: &RecordBatch, field_id: i32) -> Option<usize> {
    rows.schema().fields().iter().position(|field| {
        field.metadata().get(PARQUET_FIELD_ID_META_KEY) == Some(&field_id.to_string())
    })
}

fn with_metrics_modes(
    file: DataFile,
    schema: &Schema,
    properties: &HashMap<String, String>,
    spec_id: i32,
    sort_order_id: Option<i32>,
) -> Result<DataFile, Error> {
    let mut modes = HashMap::new();
    for field_id in file.value_counts().keys().chain(file.lower_bounds().keys()) {
        let mode = metrics_mode(properties, schema.name_by_field_id(*field_id))?;
        modes.insert(*field_id, mode);
    }
    let mode = |field_id: &i32| modes.get(field_id).copied().unwrap_or(MetricsMode::None);

    let counted = |counts: &HashMap<i32, u64>| -> HashMap<i32, u64> {
        counts
            .iter()
            .filter(|(field_id, _)| mode(field_id) != MetricsMode::None)
            .map(|(field_id, count)| (*field_id, *count))
            .collect()
    };
    let bounded = |bounds: &HashMap<i32, Datum>, upper: bool| -> HashMap<i32, Datum> {
        bounds
            .iter()
            .filter_map(|(field_id, bound)| {
                let kept = match mode(field_id) {
                    MetricsMode::None | MetricsMode::Counts => None,
                    MetricsMode::Truncate(length) => truncated_bound(bound, length, upper),
                    MetricsMode::Full => Some(bound.clone()),
                };

                kept.map(|bound| (*field_id, bound))
            })
            .collect()
    };

    let mut builder = DataFileBuilder::default();
    builder
        .content(file.content_type())
        .file_path(file.file_path().to_owned())
        .file_format(file.file_format())
        .partition(file.partition().clone())
        .partition_spec_id(spec_id)
        .record_count(file.record_count())
        .file_size_in_bytes(file.file_size_in_bytes())
        .column_sizes(counted(file.column_sizes()))
        .value_counts(counted(file.value_counts()))
        .null_value_counts(counted(file.null_value_counts()))
        .nan_value_counts(counted(file.nan_value_counts()))
        .lower_bounds(bounded(file.lower_bounds(), false))
        .upper_bounds(bounded(file.upper_bounds(), true))
        .split_offsets(file.split_offsets().map(<[i64]>::to_vec));
    if let Some(sort_order_id) = sort_order_id {
        builder.sort_order_id(sort_order_id);
    }

    builder
        .build()
        .map_err(|error| Error::DataFile(error.to_string()))
}

fn metrics_mode(
    properties: &HashMap<String, String>,
    column: Option<&str>,
) -> Result<MetricsMode, Error> {
    let column_key = column.map(|column| format!("{METRICS_COLUMN}{column}"));
    let configured = column_key
        .as_ref()
        .and_then(|key| properties.get(key).map(|mode| (key.as_str(), mode)))
        .or_else(|| {
            properties
                .get(METRICS_DEFAULT)
                .map(|mode| (METRICS_DEFAULT, mode))
        });

    let Some((key, mode)) = configured else {
        return Ok(MetricsMode::Truncate(STRING_BOUND));
    };
    match mode.as_str() {
        "none" => Ok(MetricsMode::None),
        "counts" => Ok(MetricsMode::Counts),
        "full" => Ok(MetricsMode::Full),
        _ => mode
            .strip_prefix("truncate(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|length| length.parse().ok())
            .map(MetricsMode::Truncate)
            .ok_or_else(|| Error::table_property(key, mode)),
    }
}

fn truncated_bound(bound: &Datum, length: usize, upper: bool) -> Option<Datum> {
    match bound.literal() {
        PrimitiveLiteral::String(value) if upper => raised(value, length).map(Datum::string),
        PrimitiveLiteral::String(value) => Some(Datum::string(truncated(value, length))),
        PrimitiveLiteral::Binary(value) if value.len() <= length => Some(bound.clone()),
        PrimitiveLiteral::Binary(value) if upper => {
            raised_bytes(&value[..length]).map(Datum::binary)
        }
        PrimitiveLiteral::Binary(value) => Some(Datum::binary(value[..length].to_vec())),
        _ => Some(bound.clone()),
    }
}

fn raised_bytes(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = prefix.to_vec();

    while let Some(last) = bytes.pop() {
        if let Some(next) = last.checked_add(1) {
            bytes.push(next);
            return Some(bytes);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::Int64Array;
    use bytes::Bytes;
    use mink_core::{Partition, Record};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::testing::external_table;
    use crate::{FileSchema, last_time_field_id};

    const DAY_MS: i64 = 86_400_000;

    fn records() -> Vec<Record> {
        (0..6)
            .map(|offset| {
                let day = if offset < 3 { 0 } else { 1 };
                let value = Some(Bytes::from(format!("value-{offset:020}")));

                Record::new(offset, day * DAY_MS + offset, None, value, Vec::new()).unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn files_follow_the_table_partitioning_and_sort_order() {
        let table = external_table(last_time_field_id() + 1, HashMap::new());
        let partition = Partition::new("orders", 0).unwrap();
        let rows = crate::record_batch(&partition, &records(), &FileSchema::Binary).unwrap();

        let files = write_data_files(&table, vec![rows], "0-0-6").await.unwrap();

        assert_eq!(files.len(), 2);
        for file in &files {
            let bytes = table
                .file_io()
                .new_input(file.file_path())
                .unwrap()
                .read()
                .await
                .unwrap();
            let batch = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let offsets: Vec<i64> = batch
                .column_by_name("_kafka_offset")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec();

            assert_eq!(file.record_count(), 3);
            assert_eq!(file.sort_order_id(), Some(1));
            assert!(
                offsets.windows(2).all(|pair| pair[0] > pair[1]),
                "{offsets:?}"
            );
        }
    }

    #[tokio::test]
    async fn bounds_follow_the_metrics_modes() {
        let properties = HashMap::from([
            (
                "write.metadata.metrics.default".to_owned(),
                "truncate(4)".to_owned(),
            ),
            (
                "write.metadata.metrics.column._kafka_partition".to_owned(),
                "none".to_owned(),
            ),
            (
                "write.metadata.metrics.column._kafka_offset".to_owned(),
                "counts".to_owned(),
            ),
        ]);
        let table = external_table(last_time_field_id() + 1, properties);
        let partition = Partition::new("orders", 0).unwrap();
        let rows = crate::record_batch(&partition, &records(), &FileSchema::Binary).unwrap();

        let files = write_data_files(&table, vec![rows], "0-0-6").await.unwrap();

        let file = &files[0];
        let value = file.lower_bounds()[&9].literal().clone();
        assert!(!file.null_value_counts().contains_key(&1));
        assert!(file.null_value_counts().contains_key(&2));
        assert!(!file.lower_bounds().contains_key(&2));
        assert_eq!(value, PrimitiveLiteral::Binary(b"valu".to_vec()));
        assert_eq!(
            file.upper_bounds()[&9].literal(),
            &PrimitiveLiteral::Binary(b"valv".to_vec())
        );
    }

    #[test]
    fn metrics_modes_are_read_per_column_and_refuse_unknown_modes() {
        let properties = HashMap::from([
            (METRICS_DEFAULT.to_owned(), "counts".to_owned()),
            (format!("{METRICS_COLUMN}id"), "truncate(8)".to_owned()),
            (format!("{METRICS_COLUMN}bad"), "sometimes".to_owned()),
        ]);

        assert_eq!(
            metrics_mode(&properties, Some("id")).unwrap(),
            MetricsMode::Truncate(8)
        );
        assert_eq!(
            metrics_mode(&properties, Some("other")).unwrap(),
            MetricsMode::Counts
        );
        assert_eq!(
            metrics_mode(&HashMap::new(), None).unwrap(),
            MetricsMode::Truncate(STRING_BOUND)
        );
        assert!(matches!(
            metrics_mode(&properties, Some("bad")),
            Err(Error::TableProperty { .. })
        ));
    }

    #[test]
    fn writer_properties_follow_the_table_properties() {
        let properties = HashMap::from([
            (
                "write.parquet.compression-codec".to_owned(),
                "gzip".to_owned(),
            ),
            ("write.parquet.compression-level".to_owned(), "9".to_owned()),
            (
                "write.parquet.row-group-size-bytes".to_owned(),
                "1024".to_owned(),
            ),
            (
                "write.parquet.bloom-filter-enabled.column._kafka_key".to_owned(),
                "true".to_owned(),
            ),
        ]);
        let key = ColumnPath::from("_kafka_key");

        let schema = mink_schema::binary_schema();
        let built = writer_properties(&properties, schema).unwrap();
        let invalid = writer_properties(
            &HashMap::from([(
                "write.parquet.compression-codec".to_owned(),
                "lzo".to_owned(),
            )]),
            schema,
        );
        let unknown = writer_properties(
            &HashMap::from([(format!("{BLOOM_ENABLED}_kafka_nothing"), "true".to_owned())]),
            schema,
        );

        assert_eq!(
            built.compression(&key),
            Compression::GZIP(GzipLevel::try_new(9).unwrap())
        );
        assert_eq!(built.max_row_group_bytes(), Some(1024));
        assert!(built.bloom_filter_properties(&key).is_some());
        assert!(matches!(invalid, Err(Error::TableProperty { .. })));
        assert!(matches!(unknown, Err(Error::TableProperty { .. })));
    }

    #[tokio::test]
    async fn bloom_filters_reach_nested_columns() {
        let properties = HashMap::from([(
            format!("{BLOOM_ENABLED}_kafka_headers.element.key"),
            "true".to_owned(),
        )]);
        let table = external_table(last_time_field_id() + 1, properties);
        let partition = Partition::new("orders", 0).unwrap();
        let rows = crate::record_batch(&partition, &records(), &FileSchema::Binary).unwrap();

        let files = write_data_files(&table, vec![rows], "0-0-6").await.unwrap();

        let bytes = table
            .file_io()
            .new_input(files[0].file_path())
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let columns = reader.metadata().row_group(0).columns();
        let filtered: Vec<String> = columns
            .iter()
            .filter(|column| column.bloom_filter_offset().is_some())
            .map(|column| column.column_path().string())
            .collect();
        assert_eq!(filtered, ["_kafka_headers.list.element.key"]);
    }

    #[tokio::test]
    async fn data_files_round_trip_through_their_encoding() {
        let table = external_table(last_time_field_id() + 1, HashMap::new());
        let partition = Partition::new("orders", 0).unwrap();
        let rows = crate::record_batch(&partition, &records(), &FileSchema::Binary).unwrap();
        let files = write_data_files(&table, vec![rows], "0-0-6").await.unwrap();
        let spec_id = table.metadata().default_partition_spec().spec_id();

        let encoded = encoded_data_files(&table, files.clone()).unwrap();
        let unknown = data_files(&table, spec_id + 7, &encoded);

        assert_eq!(data_files(&table, spec_id, &encoded).unwrap(), files);
        assert!(matches!(unknown, Err(Error::UnknownPartitionSpec(_))));
    }

    #[test]
    fn truncated_upper_bounds_stay_above_the_values_they_cover() {
        let string = Datum::string("abcdef");
        let binary = Datum::binary(vec![1, 2, 0xff, 9]);

        assert_eq!(
            truncated_bound(&string, 3, false),
            Some(Datum::string("abc"))
        );
        assert_eq!(
            truncated_bound(&string, 3, true),
            Some(Datum::string("abd"))
        );
        assert_eq!(
            truncated_bound(&binary, 3, true),
            Some(Datum::binary(vec![1, 3]))
        );
        assert_eq!(
            truncated_bound(&Datum::binary(vec![0xff]), 1, true),
            Some(Datum::binary(vec![0xff]))
        );
        assert_eq!(
            truncated_bound(&Datum::binary(vec![0xff, 1]), 1, true),
            None
        );
    }
}
