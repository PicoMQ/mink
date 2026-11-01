use std::cmp::Ordering;
use std::collections::BTreeSet;

use bytes::Bytes;
#[cfg(test)]
use bytes::BytesMut;
use iceberg::spec::{Datum, PrimitiveType, Schema};
use mink_core::{Assembly, BuiltFile, File, IndexedFile, OffsetRange, Span, Stats};
use parquet::file::metadata::{
    FileMetaData, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter, RowGroupMetaData,
};

use crate::Error;
#[cfg(test)]
use crate::slice::{MAGIC, PADDING, body_length, footer};
use crate::slice::{TRAILER, aligned, bodies, file_length};

#[derive(Debug, Clone, PartialEq, Eq)]
struct MergedFooter {
    footer: Bytes,
    length: u64,
}

struct Replaced<'a> {
    footer: &'a [u8],
    start: u64,
    length: u64,
}

impl MergedFooter {
    fn footer(&self) -> &Bytes {
        &self.footer
    }

    fn length(&self) -> u64 {
        self.length
    }
}

impl<'a> Replaced<'a> {
    fn new(footer: &'a [u8], start: u64, length: u64) -> Replaced<'a> {
        Replaced {
            footer,
            start,
            length,
        }
    }

    fn of(built: &'a BuiltFile) -> Replaced<'a> {
        let bodies = bodies(built);

        Replaced::new(built.footer(), bodies.start, bodies.end - bodies.start)
    }
}

pub fn assemble(run: &[BuiltFile], schema: &Schema) -> Result<Assembly, Error> {
    let replaced: Vec<File> = run
        .iter()
        .map(|built| built.indexed().file().clone())
        .collect();
    let first = replaced
        .first()
        .ok_or_else(|| Error::Assembly("an assembly needs files".to_owned()))?;

    let sized: Vec<Replaced> = run.iter().map(Replaced::of).collect();
    let merged = merged_footer(&sized)?;

    let spans: Vec<Span> = run.iter().flat_map(BuiltFile::spans).copied().collect();
    let ranges: Vec<OffsetRange> = spans.iter().map(Span::range).collect();

    let stats: Vec<&Stats> = run.iter().map(|built| built.indexed().stats()).collect();
    let stats = merged_stats(schema, &stats, merged.length())?;

    let file = File::assembled(
        first.partition().clone(),
        first.time(),
        first.schema_id(),
        first.version(),
        &ranges,
    )?;
    let built = BuiltFile::new(
        IndexedFile::new(file, stats),
        merged.footer().clone(),
        spans,
    )?;

    Ok(Assembly::new(built, replaced)?)
}

fn merged_footer(files: &[Replaced]) -> Result<MergedFooter, Error> {
    let mut row_groups = Vec::new();
    let mut first: Option<ParquetMetaData> = None;
    let starts = aligned(files.iter().map(|file| file.length));
    let too_long = || Error::Assembly("an assembly is too long".to_owned());

    for (file, start) in files.iter().zip(&starts) {
        let metadata = ParquetMetaDataReader::decode_metadata(file.footer)?;
        let shift = i64::try_from(start - file.start).map_err(|_| too_long())?;

        for row_group in metadata.row_groups() {
            row_groups.push(shifted(row_group, shift, row_groups.len())?);
        }

        first.get_or_insert(metadata);
    }

    let first = first.ok_or_else(|| Error::Assembly("an assembly needs parts".to_owned()))?;
    let merged = merged(&first, row_groups);

    let mut written = Vec::new();
    ParquetMetaDataWriter::new(&mut written, &merged).finish()?;
    written.truncate(written.len() - TRAILER);

    let (last, start) = files.iter().zip(&starts).next_back().ok_or_else(too_long)?;
    let length = file_length(start + last.length, written.len());

    Ok(MergedFooter {
        footer: Bytes::from(written),
        length,
    })
}

fn merged_stats(schema: &Schema, replaced: &[&Stats], length: u64) -> Result<Stats, Error> {
    let records = replaced.iter().map(|stats| stats.records()).sum();
    let first_timestamp_ms = replaced
        .iter()
        .map(|stats| stats.first_timestamp_ms())
        .min()
        .unwrap_or_default();
    let mut merged = Stats::new(records, length, first_timestamp_ms);

    for stats in replaced {
        for (field, count) in stats.nulls() {
            let total = merged.nulls().get(field).copied().unwrap_or(0) + count;

            merged.set_nulls(*field, total);
        }

        for (field, count) in stats.nans() {
            let total = merged.nans().get(field).copied().unwrap_or(0) + count;

            merged.set_nans(*field, total);
        }
    }

    let fields: BTreeSet<i32> = replaced
        .iter()
        .flat_map(|stats| stats.lower().keys().copied())
        .collect();

    for field in fields {
        let primitive = schema
            .field_by_id(field)
            .and_then(|nested| nested.field_type.as_primitive_type().cloned())
            .ok_or(Error::StatsField(field))?;

        let lower = extreme(
            replaced.iter().map(|stats| stats.lower().get(&field)),
            &primitive,
            Ordering::Less,
        )?;
        let upper = extreme(
            replaced.iter().map(|stats| stats.upper().get(&field)),
            &primitive,
            Ordering::Greater,
        )?;

        if let (Some(lower), Some(upper)) = (lower, upper) {
            merged.set_bounds(field, lower, upper);
        }
    }

    Ok(merged)
}

#[cfg(test)]
pub(crate) fn assembled_bytes(parts: &[Bytes]) -> Result<Bytes, Error> {
    let sized = parts
        .iter()
        .map(|part| {
            let footer = footer(part)?;

            Ok(Replaced::new(
                footer,
                MAGIC.len() as u64,
                body_length(part.len() as u64, footer.len())?,
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let assembled = merged_footer(&sized)?;
    let starts = aligned(sized.iter().map(|sized| sized.length));

    let mut bytes = BytesMut::with_capacity(assembled.length as usize);
    bytes.extend_from_slice(MAGIC);

    for ((part, sized), start) in parts.iter().zip(&sized).zip(starts) {
        let body = sized.length as usize;

        bytes.resize(start as usize, 0);
        bytes.extend_from_slice(&part[MAGIC.len()..MAGIC.len() + body]);
    }

    bytes.resize(bytes.len() + PADDING, 0);
    bytes.extend_from_slice(&assembled.footer);
    bytes.extend_from_slice(&(assembled.footer.len() as u32).to_le_bytes());
    bytes.extend_from_slice(MAGIC);

    Ok(bytes.freeze())
}

fn extreme<'a>(
    bounds: impl Iterator<Item = Option<&'a Vec<u8>>>,
    primitive: &PrimitiveType,
    wanted: Ordering,
) -> Result<Option<Vec<u8>>, Error> {
    let mut chosen: Option<(Datum, &Vec<u8>)> = None;

    for bound in bounds.flatten() {
        let datum = Datum::try_from_bytes(bound, primitive.clone())?;

        let better = match &chosen {
            Some((current, _)) => datum.partial_cmp(current) == Some(wanted),
            None => true,
        };
        if better {
            chosen = Some((datum, bound));
        }
    }

    Ok(chosen.map(|(_, bound)| bound.clone()))
}

fn shifted(
    row_group: &RowGroupMetaData,
    shift: i64,
    ordinal: usize,
) -> Result<RowGroupMetaData, Error> {
    let columns = row_group
        .columns()
        .iter()
        .map(|column| {
            column
                .clone()
                .into_builder()
                .set_data_page_offset(column.data_page_offset() + shift)
                .set_dictionary_page_offset(
                    column.dictionary_page_offset().map(|offset| offset + shift),
                )
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ordinal = i16::try_from(ordinal)
        .map_err(|_| Error::Assembly("an assembly holds too many row groups".to_owned()))?;

    let mut builder = row_group
        .clone()
        .into_builder()
        .set_column_metadata(columns)
        .set_ordinal(ordinal);
    if let Some(offset) = row_group.file_offset() {
        builder = builder.set_file_offset(offset + shift);
    }

    Ok(builder.build()?)
}

fn merged(first: &ParquetMetaData, row_groups: Vec<RowGroupMetaData>) -> ParquetMetaData {
    let file = first.file_metadata();
    let rows = row_groups.iter().map(RowGroupMetaData::num_rows).sum();

    let file_metadata = FileMetaData::new(
        file.version(),
        rows,
        file.created_by().map(str::to_owned),
        file.key_value_metadata().cloned(),
        file.schema_descr_ptr(),
        file.column_orders().cloned(),
    );

    ParquetMetaData::new(file_metadata, row_groups)
}

#[cfg(test)]
mod tests {
    use mink_core::{FileVersion, Granularity, OffsetRange, Partition, TimePartition};
    use mink_schema::binary_schema;

    use super::*;
    use crate::file::build_file;
    use crate::schema::FileSchema;
    use crate::slice::{ALIGNMENT, body_starts};
    use crate::testing::{hour_records, hourly, inside, rows_of};

    fn built(file: &File, records: &[mink_core::Record]) -> (Bytes, BuiltFile) {
        build_file(
            file,
            &inside(records, file),
            Granularity::Hour,
            &FileSchema::Binary,
        )
        .unwrap()
    }

    #[test]
    fn assembled_files_read_back_as_their_parts() {
        let records = hour_records(0..300, |_| 5);
        let parts: Vec<Bytes> = [(0, 100), (100, 120), (150, 300)]
            .iter()
            .map(|(start, end)| built(&hourly(*start, *end, 5), &records).0)
            .collect();

        let assembled = assembled_bytes(&parts).unwrap();

        let parts: Vec<_> = parts.iter().map(rows_of).collect();
        let expected = arrow_select::concat::concat_batches(&parts[0].schema(), &parts).unwrap();
        assert_eq!(rows_of(&assembled), expected);
    }

    proptest::proptest! {
        #[test]
        fn an_assembly_matches_the_bytes_of_its_parts(lengths in proptest::collection::vec(1i64..400, 3..6)) {
            let mut start = 0;
            let mut files = Vec::new();
            for length in lengths {
                files.push(hourly(start, start + length, 5));
                start += length + 3;
            }
            let records = hour_records(0..start, |_| 5);
            let parts: Vec<(Bytes, BuiltFile)> = files.iter().map(|file| built(file, &records)).collect();
            let run: Vec<BuiltFile> = parts.iter().map(|(_, built)| built.clone()).collect();
            let ranges: Vec<OffsetRange> = files.iter().map(File::range).collect();

            let assembly = assemble(&run, binary_schema()).unwrap();
            let first = assemble(&run[..2], binary_schema()).unwrap();
            let again_run: Vec<BuiltFile> = [first.built().clone()].into_iter().chain(run[2..].iter().cloned()).collect();
            let again = assemble(&again_run, binary_schema()).unwrap();

            let bytes = assembled_bytes(&parts.iter().map(|(bytes, _)| bytes.clone()).collect::<Vec<_>>()).unwrap();
            let whole = File::assembled(Partition::new("orders", 0).unwrap(), TimePartition::new(0, Some(5)), 0, FileVersion::V1, &ranges).unwrap();
            let starts = body_starts(assembly.built());
            let end = bodies(assembly.built()).end;
            let (_, direct) = built(&hourly(whole.start_offset(), whole.end_offset(), 5), &records);
            let stats = assembly.built().indexed().stats();

            proptest::prop_assert_eq!(again.built(), assembly.built());
            proptest::prop_assert!(starts.iter().all(|start| start % ALIGNMENT == 0), "{starts:?}");
            proptest::prop_assert_eq!(file_length(end, assembly.built().footer().len()), stats.bytes());
            proptest::prop_assert_eq!(bytes.len() as u64, stats.bytes());
            proptest::prop_assert_eq!(footer(&bytes).unwrap(), &assembly.built().footer()[..]);
            proptest::prop_assert_eq!(stats.records(), run.iter().map(|built| built.indexed().stats().records()).sum::<u64>());
            proptest::prop_assert_eq!(stats.lower().get(&2), direct.indexed().stats().lower().get(&2));
            proptest::prop_assert_eq!(stats.upper().get(&3), direct.indexed().stats().upper().get(&3));
        }
    }
}
