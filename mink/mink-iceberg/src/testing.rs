use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::Bytes;
use iceberg::io::{FileIOBuilder, MemoryStorageFactory};
use iceberg::spec::{
    DataFile, FormatVersion, MAIN_BRANCH, ManifestListWriter, ManifestWriterBuilder, NullOrder,
    Operation, Snapshot, SortDirection, SortField, SortOrder, Summary, TableMetadataBuilder,
    Transform, UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table as IcebergTable;
use iceberg::{Runtime, TableIdent};
use mink_core::{File, Partition, Record, TimePartition};
use mink_schema::{TIMESTAMP_ID, binary_schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uuid::Uuid;

use crate::table::end_offset_key;

pub(crate) fn hour_records(
    offsets: std::ops::Range<i64>,
    hour_of: impl Fn(i64) -> i64,
) -> Vec<Record> {
    offsets
        .map(|offset| {
            let timestamp = hour_of(offset) * 3_600_000 + offset;
            let value = Bytes::from(format!("value-{offset}"));

            Record::new(offset, timestamp, None, Some(value), Vec::new()).unwrap()
        })
        .collect()
}

pub(crate) fn inside(records: &[Record], file: &File) -> Vec<Record> {
    records
        .iter()
        .filter(|record| file.range().includes(record.offset()))
        .cloned()
        .collect()
}

pub(crate) fn hourly(start: i64, end: i64, bucket: i32) -> File {
    let partition = Partition::new("orders", 0).unwrap();

    File::v1(
        partition,
        start,
        end,
        TimePartition::new(0, Some(bucket)),
        0,
    )
    .unwrap()
}

pub(crate) fn rows_of(bytes: &Bytes) -> RecordBatch {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let schema = reader.schema().clone();
    let batches: Vec<RecordBatch> = reader.build().unwrap().collect::<Result<_, _>>().unwrap();

    arrow_select::concat::concat_batches(&schema, &batches).unwrap()
}

pub(crate) fn external_table(
    day_field_id: i32,
    properties: HashMap<String, String>,
) -> IcebergTable {
    let spec = UnboundPartitionSpec::builder()
        .add_partition_fields([UnboundPartitionField {
            source_id: TIMESTAMP_ID,
            field_id: Some(day_field_id),
            name: "day".to_owned(),
            transform: Transform::Day,
        }])
        .unwrap()
        .build();
    let order = SortOrder::builder()
        .with_order_id(1)
        .with_sort_field(
            SortField::builder()
                .source_id(2)
                .transform(Transform::Identity)
                .direction(SortDirection::Descending)
                .null_order(NullOrder::Last)
                .build(),
        )
        .build_unbound()
        .unwrap();

    let created = TableMetadataBuilder::new(
        binary_schema().clone(),
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        "memory://tables/orders".to_owned(),
        FormatVersion::V2,
        properties,
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let metadata = TableMetadataBuilder::new_from_metadata(created, None)
        .add_current_schema(binary_schema().clone())
        .unwrap()
        .add_default_partition_spec(spec)
        .unwrap()
        .add_sort_order(order)
        .unwrap()
        .set_default_sort_order(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;

    IcebergTable::builder()
        .identifier(TableIdent::from_strs(["kafka", "orders"]).unwrap())
        .metadata(metadata)
        .file_io(FileIOBuilder::new(Arc::new(MemoryStorageFactory)).build())
        .runtime(Runtime::try_current().unwrap())
        .build()
        .unwrap()
}

pub(crate) async fn appended(
    table: &IcebergTable,
    data_files: Vec<DataFile>,
    delete_files: Vec<DataFile>,
    end_offsets: &[(i32, i64)],
) -> IcebergTable {
    let metadata = table.metadata();
    let io = table.file_io();
    let parent = metadata.current_snapshot_id();
    let snapshot_id = parent.map_or(1, |id| id + 1);
    let sequence_number = metadata.last_sequence_number() + 1;
    let schema = metadata.current_schema().clone();
    let spec = metadata.default_partition_spec().as_ref().clone();

    let mut manifests = Vec::new();
    for (index, files) in [data_files, delete_files].into_iter().enumerate() {
        if files.is_empty() {
            continue;
        }

        let path = format!(
            "{}/metadata/{}-m{index}.avro",
            metadata.location(),
            Uuid::new_v4()
        );
        let builder = ManifestWriterBuilder::new(
            io.new_output(&path).unwrap(),
            Some(snapshot_id),
            schema.clone(),
            spec.clone(),
        );
        let mut writer = match index {
            0 => builder.build_v2_data(),
            _ => builder.build_v2_deletes(),
        };
        for file in files {
            writer.add_file(file, sequence_number).unwrap();
        }
        manifests.push(writer.write_manifest_file().await.unwrap());
    }
    if let Some(current) = metadata.current_snapshot() {
        let listed = table.manifest_list_reader(current).load().await.unwrap();
        manifests.extend(listed.consume_entries());
    }

    let list = format!(
        "{}/metadata/snap-{}.avro",
        metadata.location(),
        Uuid::new_v4()
    );
    let output = io.new_output(&list).unwrap().writer().await.unwrap();
    let mut writer = ManifestListWriter::v2(output, snapshot_id, parent, sequence_number);
    writer.add_manifests(manifests.into_iter()).unwrap();
    writer.close().await.unwrap();

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(metadata.last_updated_ms() + 1)
        .with_manifest_list(list)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(schema.schema_id())
        .build();
    let properties = end_offsets
        .iter()
        .map(|(partition, end)| (end_offset_key(*partition), end.to_string()))
        .collect();
    let metadata = TableMetadataBuilder::new_from_metadata(metadata.clone(), None)
        .set_branch_snapshot(snapshot, MAIN_BRANCH)
        .unwrap()
        .set_properties(properties)
        .unwrap()
        .build()
        .unwrap()
        .metadata;

    IcebergTable::builder()
        .identifier(table.identifier().clone())
        .metadata(metadata)
        .file_io(io.clone())
        .runtime(Runtime::try_current().unwrap())
        .build()
        .unwrap()
}
