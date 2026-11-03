use std::ops::Range;

use mink_core::{BuiltFile, Span};

use crate::Error;

pub(crate) const MAGIC: &[u8; 4] = b"PAR1";
pub(crate) const TRAILER: usize = 8;
pub(crate) const ALIGNMENT: u64 = 4_096;
const LONGEST_FIXED_READ: u64 = 65_536;
const DUCKDB_SHARE: u64 = 1_024;
const PADDING_SHARE: u64 = DUCKDB_SHARE / 2;

pub(crate) fn body_range(built: &BuiltFile) -> Range<u64> {
    let starts = body_starts(built);
    let last = starts.len() - 1;

    starts[0]..starts[last] + built.spans()[last].body_length()
}

pub(crate) fn body_starts(built: &BuiltFile) -> Vec<u64> {
    match built.indexed().file().assembly() {
        Some(_) => aligned_starts(built.spans().iter().map(Span::body_length)),
        None => vec![MAGIC.len() as u64],
    }
}

pub(crate) fn aligned_starts(lengths: impl IntoIterator<Item = u64>) -> Vec<u64> {
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

pub(crate) fn padding(unpadded_length: u64) -> u64 {
    unpadded_length
        .div_ceil(PADDING_SHARE)
        .next_power_of_two()
        .max(LONGEST_FIXED_READ)
        + ALIGNMENT
}

pub fn unpadded(length: u64) -> u64 {
    length.saturating_sub(padding(length.saturating_sub(padding(length))))
}

pub(crate) fn body_length(length: u64, footer_length: usize) -> Result<u64, Error> {
    unpadded(length)
        .checked_sub((MAGIC.len() + footer_length + TRAILER) as u64)
        .ok_or_else(|| Error::Framing("a file is shorter than its framing".to_owned()))
}

pub(crate) fn file_length(bodies_end: u64, footer_length: usize) -> u64 {
    let unpadded_length = bodies_end + (footer_length + TRAILER) as u64;

    unpadded_length + padding(unpadded_length)
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

pub(crate) fn padded(mut bytes: Vec<u8>) -> Result<(Vec<u8>, u64), Error> {
    let body_end = bytes.len() - footer(&bytes)?.len() - TRAILER;
    let padding = padding(bytes.len() as u64) as usize;
    bytes.splice(body_end..body_end, std::iter::repeat_n(0, padding));

    Ok((bytes, (body_end - MAGIC.len()) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUCKDB_SMALLEST_READ: u64 = 16_384;

    proptest::proptest! {
        #[test]
        fn padding_covers_every_first_read_and_comes_back_off(bits in 1u32..41, raw in proptest::prelude::any::<u64>()) {
            let unpadded_length = raw % (1u64 << bits);
            let length = unpadded_length + padding(unpadded_length);
            let duckdb = length.div_ceil(DUCKDB_SHARE).next_power_of_two().max(DUCKDB_SMALLEST_READ);
            let duckdb_start = length.saturating_sub(duckdb) / ALIGNMENT * ALIGNMENT;

            proptest::prop_assert_eq!(unpadded(length), unpadded_length);
            proptest::prop_assert!(padding(unpadded_length) >= LONGEST_FIXED_READ + ALIGNMENT);
            proptest::prop_assert!(duckdb >= length || duckdb_start + TRAILER as u64 >= unpadded_length);
        }
    }
}
