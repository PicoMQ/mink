use iceberg::spec::{PartitionSpec, Transform, UnboundPartitionField};
use mink_core::{Granularity, TimeSpecs};
use mink_schema::{PARTITION, PARTITION_ID, TIMESTAMP, TIMESTAMP_ID, binary_schema};

use crate::Error;

const PARTITION_FIELD: i32 = 1000;

pub(super) fn partition_specs(specs: &TimeSpecs) -> Result<Vec<PartitionSpec>, Error> {
    specs
        .granularities()
        .iter()
        .zip(time_field_ids(specs))
        .enumerate()
        .map(|(spec_id, (granularity, field_id))| {
            let mut fields = vec![UnboundPartitionField {
                source_id: PARTITION_ID,
                field_id: Some(PARTITION_FIELD),
                name: PARTITION.to_owned(),
                transform: Transform::Identity,
            }];

            if let (Some(transform), Some(field_id)) = (transform(*granularity), field_id) {
                fields.push(UnboundPartitionField {
                    source_id: TIMESTAMP_ID,
                    field_id: Some(field_id),
                    name: format!("{TIMESTAMP}_{}", granularity.name()),
                    transform,
                });
            }

            Ok(PartitionSpec::builder(binary_schema().clone())
                .with_spec_id(i32::try_from(spec_id).expect("spec ids fit in an i32"))
                .add_unbound_fields(fields)?
                .build()?)
        })
        .collect()
}

pub(super) fn last_partition_id(specs: &TimeSpecs) -> i32 {
    time_field_ids(specs)
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(PARTITION_FIELD)
}

fn time_field_ids(specs: &TimeSpecs) -> Vec<Option<i32>> {
    let mut seen: Vec<Granularity> = Vec::new();

    specs
        .granularities()
        .iter()
        .map(|granularity| {
            if *granularity == Granularity::None {
                return None;
            }

            let index = match seen.iter().position(|known| known == granularity) {
                Some(index) => index,
                None => {
                    seen.push(*granularity);
                    seen.len() - 1
                }
            };

            Some(PARTITION_FIELD + 1 + i32::try_from(index).expect("few granularities"))
        })
        .collect()
}

fn transform(granularity: Granularity) -> Option<Transform> {
    match granularity {
        Granularity::Hour => Some(Transform::Hour),
        Granularity::Day => Some(Transform::Day),
        Granularity::Month => Some(Transform::Month),
        Granularity::None => None,
    }
}
