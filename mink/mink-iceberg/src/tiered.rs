use std::collections::{BTreeMap, HashMap};

use iceberg::TableUpdate;
use iceberg::spec::{
    DataFile, PartitionSpec, Schema, UnboundPartitionField, UnboundPartitionSpec,
    deserialize_data_file_from_json, serialize_data_file_to_json,
};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use mink_core::{Offsets, TieredFiles};

use crate::Error;
use crate::table::{end_offset_key, last_time_field_id, start_offset_key, stored_end_offsets};

pub fn end_offsets(table: &IcebergTable) -> Result<BTreeMap<i32, i64>, Error> {
    stored_end_offsets(table.metadata().properties())
}

pub fn end_offsets_update(ends: &BTreeMap<i32, i64>) -> TableUpdate {
    let updates = ends
        .iter()
        .map(|(partition, end)| (end_offset_key(*partition), end.to_string()))
        .collect();

    TableUpdate::SetProperties { updates }
}

pub fn append(
    table: &IcebergTable,
    files: Vec<DataFile>,
    spans: &[Offsets],
) -> Result<Transaction, Error> {
    let mut summary = HashMap::new();
    let mut properties = HashMap::new();
    for span in spans {
        let partition = span.partition().index();
        let end = (end_offset_key(partition), span.end_offset().to_string());

        summary.insert(start_offset_key(partition), span.start_offset().to_string());
        summary.insert(end.0.clone(), end.1.clone());
        properties.insert(end.0, end.1);
    }

    let transaction = Transaction::new(table);
    let transaction = transaction
        .fast_append()
        .with_check_duplicate(false)
        .add_data_files(files)
        .set_snapshot_properties(summary)
        .apply(transaction)?;

    let mut updates = transaction.update_table_properties();
    for (key, value) in properties {
        updates = updates.set(key, value);
    }
    Ok(updates.apply(transaction)?)
}

pub fn external_partition_spec(
    schema: &Schema,
    fields: Vec<UnboundPartitionField>,
) -> Result<UnboundPartitionSpec, Error> {
    let first = last_time_field_id() + 1;
    let fields = fields
        .into_iter()
        .zip(first..)
        .map(|(field, field_id)| UnboundPartitionField {
            field_id: Some(field_id),
            ..field
        });

    let spec = UnboundPartitionSpec::builder()
        .add_partition_fields(fields)?
        .build();
    PartitionSpec::builder(schema.clone())
        .add_unbound_fields(spec.fields().iter().cloned())?
        .build()?;

    Ok(spec)
}

pub fn encode_data_files(table: &IcebergTable, files: &[DataFile]) -> Result<TieredFiles, Error> {
    let metadata = table.metadata();
    let spec = metadata.default_partition_spec();
    let partition_type = spec.partition_type(metadata.current_schema())?;

    let encoded = files
        .iter()
        .map(|file| {
            Ok(serialize_data_file_to_json(
                file.clone(),
                &partition_type,
                metadata.format_version(),
            )?)
        })
        .collect::<Result<_, Error>>()?;

    Ok(TieredFiles::new(spec.spec_id(), encoded))
}

pub fn decode_data_files(
    table: &IcebergTable,
    files: &TieredFiles,
) -> Result<Vec<DataFile>, Error> {
    let metadata = table.metadata();
    let spec_id = files.spec_id();
    let spec = metadata
        .partition_spec_by_id(spec_id)
        .ok_or(Error::UnknownPartitionSpec(spec_id))?;
    let partition_type = spec.partition_type(metadata.current_schema())?;

    files
        .encoded()
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use mink_core::{Partition, Record};

    use super::*;
    use crate::testing::external_table;
    use crate::{FileSchema, write_data_files};

    #[tokio::test]
    async fn data_files_round_trip_through_their_encoding() {
        let table = external_table(last_time_field_id() + 1, HashMap::new());
        let partition = Partition::new("orders", 0).unwrap();
        let records: Vec<Record> = (0..3)
            .map(|offset| Record::new(offset, offset, None, Some(Bytes::from("v")), Vec::new()))
            .collect::<Result<_, _>>()
            .unwrap();
        let rows = crate::record_batch(&partition, &records, &FileSchema::Binary).unwrap();
        let files = write_data_files(&table, rows, "0-0-3").await.unwrap();

        let encoded = encode_data_files(&table, &files).unwrap();
        let unknown = TieredFiles::new(encoded.spec_id() + 7, encoded.encoded().to_vec());

        assert_eq!(
            encoded.spec_id(),
            table.metadata().default_partition_spec().spec_id()
        );
        assert_eq!(decode_data_files(&table, &encoded).unwrap(), files);
        assert!(matches!(
            decode_data_files(&table, &unknown),
            Err(Error::UnknownPartitionSpec(_))
        ));
    }

    #[test]
    fn external_partition_fields_take_ids_after_the_time_fields() {
        let schema = mink_schema::binary_schema().clone();
        let field = UnboundPartitionField {
            source_id: mink_schema::TIMESTAMP_ID,
            field_id: None,
            name: "day".to_owned(),
            transform: iceberg::spec::Transform::Day,
        };

        let spec = external_partition_spec(&schema, vec![field]).unwrap();

        assert_eq!(spec.fields()[0].field_id, Some(last_time_field_id() + 1));
    }

    #[test]
    fn end_offsets_are_set_as_table_properties() {
        let update = end_offsets_update(&BTreeMap::from([(0, 6), (2, 9)]));

        assert_eq!(
            update,
            TableUpdate::SetProperties {
                updates: HashMap::from([
                    ("mink.partition.0.end-offset".to_owned(), "6".to_owned()),
                    ("mink.partition.2.end-offset".to_owned(), "9".to_owned()),
                ])
            }
        );
    }
}
