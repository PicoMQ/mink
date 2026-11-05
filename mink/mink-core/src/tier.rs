use crate::error::Error;
use crate::file::IndexedFile;
use crate::offsets::{OffsetRange, Offsets};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieredRange {
    offsets: Offsets,
    generation: i32,
    spec_id: i32,
    encoded_data_files: Vec<String>,
}

impl TieredRange {
    pub fn new(
        offsets: Offsets,
        generation: i32,
        spec_id: i32,
        encoded_data_files: Vec<String>,
    ) -> TieredRange {
        TieredRange {
            offsets,
            generation,
            spec_id,
            encoded_data_files,
        }
    }

    pub fn offsets(&self) -> &Offsets {
        &self.offsets
    }

    pub fn generation(&self) -> i32 {
        self.generation
    }

    pub fn spec_id(&self) -> i32 {
        self.spec_id
    }

    pub fn encoded_data_files(&self) -> &[String] {
        &self.encoded_data_files
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierFlush {
    range: OffsetRange,
    reads: Vec<OffsetRange>,
}

struct Segment {
    range: OffsetRange,
    pieces: Vec<(OffsetRange, u64)>,
    oldest_ms: i64,
}

impl TierFlush {
    pub fn range(&self) -> OffsetRange {
        self.range
    }

    pub fn reads(&self) -> &[OffsetRange] {
        &self.reads
    }
}

pub fn tier_flushes(
    files: &[IndexedFile],
    start: i64,
    cutoff_ms: i64,
    overdue_ms: i64,
    limit: u64,
) -> Result<Vec<TierFlush>, Error> {
    let mut flushes = Vec::new();
    let mut pending: Vec<Segment> = Vec::new();

    for segment in segments(files, start, cutoff_ms)? {
        pending.push(segment);

        let bytes: u64 = pending
            .iter()
            .flat_map(|segment| &segment.pieces)
            .map(|(_, bytes)| bytes)
            .sum();
        if bytes >= limit {
            flushes.push(flush(std::mem::take(&mut pending), limit)?);
        }
    }

    let overdue = pending.iter().any(|segment| segment.oldest_ms < overdue_ms);
    if overdue {
        flushes.push(flush(pending, limit)?);
    }

    Ok(flushes)
}

pub fn oldest_untiered_ms(files: &[IndexedFile], start: i64) -> Option<i64> {
    files
        .iter()
        .filter(|indexed| indexed.file().start_offset() >= start)
        .map(|indexed| indexed.stats().first_timestamp_ms())
        .min()
}

fn segments(files: &[IndexedFile], start: i64, cutoff_ms: i64) -> Result<Vec<Segment>, Error> {
    let mut after: Vec<&IndexedFile> = files
        .iter()
        .filter(|indexed| indexed.file().start_offset() >= start)
        .collect();
    after.sort_by_key(|indexed| indexed.file().start_offset());

    let mut segments = Vec::new();
    let mut segment_start = start;
    let mut end = start;
    let mut pieces = Vec::new();
    let mut oldest_ms = i64::MAX;
    for (index, indexed) in after.iter().enumerate() {
        let first_ms = indexed.stats().first_timestamp_ms();
        if first_ms >= cutoff_ms {
            break;
        }

        let file = indexed.file();
        if file.end_offset() > end {
            let piece = OffsetRange::new(file.start_offset().max(end), file.end_offset())?;
            pieces.push((piece, indexed.stats().bytes()));
            end = file.end_offset();
        }
        oldest_ms = oldest_ms.min(first_ms);

        let next_start = after
            .get(index + 1)
            .map_or(i64::MAX, |next| next.file().start_offset());
        if next_start >= end && end > segment_start {
            segments.push(Segment {
                range: OffsetRange::new(segment_start, end)?,
                pieces: std::mem::take(&mut pieces),
                oldest_ms,
            });
            segment_start = end;
            oldest_ms = i64::MAX;
        }
    }

    Ok(segments)
}

fn flush(segments: Vec<Segment>, limit: u64) -> Result<TierFlush, Error> {
    let first = segments.first().map_or(0, |segment| segment.range.start());
    let last = segments.last().map_or(0, |segment| segment.range.end());

    let mut reads: Vec<(OffsetRange, u64)> = Vec::new();
    for (piece, bytes) in segments.into_iter().flat_map(|segment| segment.pieces) {
        for (part, part_bytes) in split(piece, bytes, limit)? {
            match reads.last_mut() {
                Some((open, open_bytes))
                    if *open_bytes + part_bytes <= limit && open.end() == part.start() =>
                {
                    *open = OffsetRange::new(open.start(), part.end())?;
                    *open_bytes += part_bytes;
                }
                _ => reads.push((part, part_bytes)),
            }
        }
    }

    Ok(TierFlush {
        range: OffsetRange::new(first, last)?,
        reads: reads.into_iter().map(|(read, _)| read).collect(),
    })
}

fn split(piece: OffsetRange, bytes: u64, limit: u64) -> Result<Vec<(OffsetRange, u64)>, Error> {
    let length = u64::try_from(piece.end() - piece.start()).unwrap_or(0);
    let parts = bytes.div_ceil(limit.max(1)).clamp(1, length.max(1));
    let step = length.div_ceil(parts);

    (0..parts)
        .map(|part| piece.start() + (part * step) as i64)
        .take_while(|from| *from < piece.end())
        .map(|from| {
            let to = (from + step as i64).min(piece.end());
            Ok((OffsetRange::new(from, to)?, bytes / parts))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{File, Partition, Stats, TimePartition};

    fn file(start: i64, end: i64, bytes: u64, first_ms: i64) -> IndexedFile {
        let partition = Partition::new("orders", 0).unwrap();
        let file = File::v1(partition, start, end, TimePartition::new(0, None), 0).unwrap();

        IndexedFile::new(file, Stats::new(1, bytes, first_ms))
    }

    fn range(start: i64, end: i64) -> OffsetRange {
        OffsetRange::new(start, end).unwrap()
    }

    fn ranges(flushes: &[TierFlush]) -> Vec<OffsetRange> {
        flushes.iter().map(TierFlush::range).collect()
    }

    #[test]
    fn full_flushes_are_written_and_the_rest_waits_until_overdue() {
        let files = [
            file(0, 10, 60, 100),
            file(10, 20, 60, 200),
            file(20, 30, 60, 300),
            file(30, 40, 60, 900),
        ];

        assert_eq!(
            ranges(&tier_flushes(&files, 0, 500, 0, 100).unwrap()),
            [range(0, 20)]
        );
        assert_eq!(
            ranges(&tier_flushes(&files, 0, 500, 350, 100).unwrap()),
            [range(0, 20), range(20, 30)]
        );
        assert_eq!(ranges(&tier_flushes(&files, 20, 500, 0, 100).unwrap()), []);
        assert_eq!(ranges(&tier_flushes(&files, 0, 50, 0, 100).unwrap()), []);
    }

    #[test]
    fn a_flush_never_ends_inside_a_file_that_spans_others() {
        let files = [
            file(0, 30, 300, 100),
            file(10, 20, 100, 100),
            file(30, 40, 100, 900),
        ];
        let young_span = [file(0, 30, 300, 900), file(10, 20, 100, 100)];

        assert_eq!(
            ranges(&tier_flushes(&files, 0, 500, 0, 100).unwrap()),
            [range(0, 30)]
        );
        assert_eq!(
            ranges(&tier_flushes(&young_span, 0, 500, 1000, 100).unwrap()),
            []
        );
    }

    #[test]
    fn reads_stay_within_the_limit_and_split_a_large_file() {
        let files = [
            file(0, 10, 40, 100),
            file(10, 20, 40, 100),
            file(20, 120, 250, 100),
        ];

        let flushes = tier_flushes(&files, 0, 500, 1000, 100).unwrap();

        assert_eq!(ranges(&flushes), [range(0, 120)]);
        assert_eq!(
            flushes[0].reads(),
            [range(0, 20), range(20, 54), range(54, 88), range(88, 120)]
        );
    }

    #[test]
    fn the_oldest_untiered_record_is_the_earliest_first_timestamp_from_the_start() {
        let files = [
            file(0, 10, 1, 100),
            file(10, 20, 1, 50),
            file(20, 30, 1, 300),
        ];

        assert_eq!(oldest_untiered_ms(&files, 0), Some(50));
        assert_eq!(oldest_untiered_ms(&files, 20), Some(300));
        assert_eq!(oldest_untiered_ms(&files, 30), None);
    }

    proptest! {
        #[test]
        fn flushes_end_on_boundaries_of_old_files_and_reads_cover_them(
            spans in prop::collection::vec((1i64..20, 1u64..200, 0i64..1000), 1..20),
            cutoff in 0i64..1000,
            limit in 1u64..400,
        ) {
            let mut start = 0;
            let files: Vec<IndexedFile> = spans
                .iter()
                .map(|(length, bytes, first)| {
                    let built = file(start, start + length, *bytes, *first);
                    start += length;
                    built
                })
                .collect();

            let flushes = tier_flushes(&files, 0, cutoff, i64::MAX, limit).unwrap();

            let mut from = 0;
            for flush in &flushes {
                let end = flush.range().end();
                let inside = files.iter().filter(|indexed| indexed.file().start_offset() < end);

                prop_assert_eq!(flush.range().start(), from);
                prop_assert!(files.iter().any(|indexed| indexed.file().end_offset() == end));
                prop_assert!(inside.clone().all(|indexed| indexed.stats().first_timestamp_ms() < cutoff));
                prop_assert!(inside.clone().all(|indexed| indexed.file().end_offset() <= end));
                prop_assert_eq!(flush.reads().first().map(OffsetRange::start), Some(from));
                prop_assert_eq!(flush.reads().last().map(OffsetRange::end), Some(end));
                prop_assert!(flush.reads().windows(2).all(|pair| pair[0].end() == pair[1].start()));
                from = end;
            }
        }
    }
}
