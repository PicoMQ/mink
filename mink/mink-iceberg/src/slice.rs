use std::ops::Range;

use bytes::Bytes;
use mink_core::{BuiltFile, File, Span};

use crate::Error;

pub(crate) const MAGIC: &[u8; 4] = b"PAR1";
pub(crate) const TRAILER: usize = 8;
pub(crate) const PADDING: usize = 69_632;
pub(crate) const ALIGNMENT: u64 = 4_096;

static ZEROS: [u8; PADDING] = [0; PADDING];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Slice {
    Bytes(Bytes),
    Built { file: File, range: Range<u64> },
}

struct Cuts {
    range: Range<u64>,
    position: u64,
    slices: Vec<Slice>,
}

impl Slice {
    fn cut(&self, within: Range<u64>) -> Slice {
        match self {
            Slice::Bytes(bytes) => {
                Slice::Bytes(bytes.slice(within.start as usize..within.end as usize))
            }
            Slice::Built { file, range } => Slice::Built {
                file: file.clone(),
                range: range.start + within.start..range.start + within.end,
            },
        }
    }
}

impl Cuts {
    fn take(
        &mut self,
        length: u64,
        whole: impl FnOnce() -> Result<Slice, Error>,
    ) -> Result<(), Error> {
        let end = self.position + length;
        let (start, stop) = (self.range.start.max(self.position), self.range.end.min(end));

        if start < stop {
            self.slices
                .push(whole()?.cut(start - self.position..stop - self.position));
        }
        self.position = end;
        Ok(())
    }
}

pub fn slices(built: &BuiltFile, range: Range<u64>) -> Result<Vec<Slice>, Error> {
    let file = built.indexed().file();
    let footer = built.footer();
    let body = MAGIC.len() as u64;
    let zeros = |length: u64| Ok(Slice::Bytes(Bytes::from_static(&ZEROS[..length as usize])));

    let mut cuts = Cuts {
        range,
        position: 0,
        slices: Vec::new(),
    };
    cuts.take(body, || Ok(Slice::Bytes(Bytes::from_static(MAGIC))))?;
    for (span, start) in built.spans().iter().zip(body_starts(built)) {
        let gap = start - cuts.position;
        cuts.take(gap, || zeros(gap))?;
        cuts.take(span.body_length(), || {
            Ok(Slice::Built {
                file: file.part(span.range())?,
                range: body..body + span.body_length(),
            })
        })?;
    }

    let trailer = [&(footer.len() as u32).to_le_bytes()[..], MAGIC].concat();
    cuts.take(PADDING as u64, || zeros(PADDING as u64))?;
    cuts.take(footer.len() as u64, || Ok(Slice::Bytes(footer.clone())))?;
    cuts.take(TRAILER as u64, || Ok(Slice::Bytes(Bytes::from(trailer))))?;

    Ok(cuts.slices)
}

pub(crate) fn bodies(built: &BuiltFile) -> Range<u64> {
    let starts = body_starts(built);
    let last = starts.len() - 1;

    starts[0]..starts[last] + built.spans()[last].body_length()
}

pub(crate) fn body_starts(built: &BuiltFile) -> Vec<u64> {
    match built.indexed().file().assembly() {
        Some(_) => aligned(built.spans().iter().map(Span::body_length)),
        None => vec![MAGIC.len() as u64],
    }
}

pub(crate) fn aligned(lengths: impl IntoIterator<Item = u64>) -> Vec<u64> {
    let mut next = ALIGNMENT;

    lengths
        .into_iter()
        .map(|length| {
            let start = next;
            next = (start + length).next_multiple_of(ALIGNMENT);
            start
        })
        .collect()
}

pub fn unpadded(length: u64) -> u64 {
    length.saturating_sub(PADDING as u64)
}

pub(crate) fn body_length(length: u64, footer_length: usize) -> Result<u64, Error> {
    length
        .checked_sub(MAGIC.len() as u64 + ending(footer_length))
        .ok_or_else(|| Error::Framing("a file is shorter than its framing".to_owned()))
}

pub(crate) fn file_length(bodies_end: u64, footer_length: usize) -> u64 {
    bodies_end + ending(footer_length)
}

fn ending(footer_length: usize) -> u64 {
    (PADDING + footer_length + TRAILER) as u64
}

pub(crate) fn footer(bytes: &[u8]) -> Result<&[u8], Error> {
    let invalid = || Error::Framing("the bytes are not a Parquet file".to_owned());

    let trailer = bytes
        .len()
        .checked_sub(TRAILER)
        .filter(|start| *start >= MAGIC.len())
        .ok_or_else(invalid)?;
    if &bytes[trailer + 4..] != MAGIC || &bytes[..MAGIC.len()] != MAGIC {
        return Err(invalid());
    }

    let length = u32::from_le_bytes(
        bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| invalid())?,
    );
    let start = trailer
        .checked_sub(length as usize)
        .filter(|start| *start >= MAGIC.len())
        .ok_or_else(invalid)?;

    Ok(&bytes[start..trailer])
}

pub(crate) fn padded(mut bytes: Vec<u8>) -> Result<Vec<u8>, Error> {
    let body = bytes.len() - footer(&bytes)?.len() - TRAILER;
    bytes.splice(body..body, std::iter::repeat_n(0, PADDING));

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use mink_core::{Granularity, Record};
    use mink_schema::binary_schema;
    use proptest::collection::vec;
    use proptest::prelude::any;

    use super::*;
    use crate::assembly::{assemble, assembled_bytes};
    use crate::file::build_file;
    use crate::schema::FileSchema;
    use crate::testing::{hour_records, hourly, inside};

    fn built(file: &File, records: &[Record]) -> (Bytes, BuiltFile) {
        build_file(
            file,
            &inside(records, file),
            Granularity::Hour,
            &FileSchema::Binary,
        )
        .unwrap()
    }

    proptest::proptest! {
        #[test]
        fn slices_join_to_the_built_bytes(lengths in vec(1i64..300, 1..5), cuts in (any::<u64>(), any::<u64>())) {
            let mut start = 0;
            let mut files = Vec::new();
            for length in lengths {
                files.push(hourly(start, start + length, 5));
                start += length + 3;
            }
            let records = hour_records(0..start, |_| 5);
            let parts: Vec<(Bytes, BuiltFile)> = files.iter().map(|file| built(file, &records)).collect();
            let by_file: HashMap<&File, &Bytes> = parts
                .iter()
                .map(|(bytes, built)| (built.indexed().file(), bytes))
                .collect();

            let (bytes, file) = match parts.as_slice() {
                [part] => part.clone(),
                _ => {
                    let run: Vec<BuiltFile> = parts.iter().map(|(_, built)| built.clone()).collect();
                    let bytes = assembled_bytes(&parts.iter().map(|(bytes, _)| bytes.clone()).collect::<Vec<_>>()).unwrap();

                    (bytes, assemble(&run, binary_schema()).unwrap().built().clone())
                }
            };
            let length = bytes.len() as u64;
            let (a, b) = (cuts.0 % (length + 1), cuts.1 % (length + 1));
            let range = a.min(b)..a.max(b);

            let joined: Vec<u8> = slices(&file, range.clone())
                .unwrap()
                .iter()
                .flat_map(|slice| match slice {
                    Slice::Bytes(bytes) => bytes.to_vec(),
                    Slice::Built { file, range } => by_file[file][range.start as usize..range.end as usize].to_vec(),
                })
                .collect();
            let footer_read = slices(&file, length - 65_536..length).unwrap();

            proptest::prop_assert_eq!(joined, bytes[range.start as usize..range.end as usize].to_vec());
            proptest::prop_assert!(footer_read.iter().all(|slice| matches!(slice, Slice::Bytes(_))));
        }
    }
}
