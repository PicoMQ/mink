use std::borrow::Cow;
use std::collections::BTreeSet;

use mink_core::{Header, Part, Record};
use uuid::Uuid;

use crate::registry::SchemaRef;

const GUID_HEADER_LENGTH: usize = 17;
const PREFIX_LENGTH: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartSchemaRef {
    part: Part,
    schema_ref: SchemaRef,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct WireHeader<'a> {
    pub(crate) schema_ref: SchemaRef,
    pub(crate) payload: Cow<'a, [u8]>,
}

impl PartSchemaRef {
    pub fn new(part: Part, schema_ref: SchemaRef) -> PartSchemaRef {
        PartSchemaRef { part, schema_ref }
    }

    pub fn part(&self) -> Part {
        self.part
    }

    pub fn schema_ref(&self) -> SchemaRef {
        self.schema_ref
    }
}

pub fn schema_refs(records: &[Record]) -> BTreeSet<PartSchemaRef> {
    let mut refs = BTreeSet::new();

    for record in records {
        for part in Part::ALL {
            let bytes = record.part(part).map(AsRef::as_ref);

            if let Some(header) = bytes.and_then(|bytes| read_header(part, bytes, record.headers()))
            {
                refs.insert(PartSchemaRef::new(part, header.schema_ref));
            }
        }
    }

    refs
}

pub(crate) fn read_header<'a>(
    part: Part,
    bytes: &'a [u8],
    headers: &'a [Header],
) -> Option<WireHeader<'a>> {
    let header = format!("__{}_schema_id", part.name());
    let guid_header = headers
        .iter()
        .rev()
        .find(|candidate| candidate.key() == header)
        .and_then(|header| header.value())
        .filter(|value| value.len() >= GUID_HEADER_LENGTH && value[0] == 1);

    if let Some(value) = guid_header {
        let guid = Uuid::from_slice(&value[1..GUID_HEADER_LENGTH]).ok()?;
        let payload = match &value[GUID_HEADER_LENGTH..] {
            [] => Cow::Borrowed(bytes),
            leading => Cow::Owned([leading, bytes].concat()),
        };

        return Some(WireHeader {
            schema_ref: SchemaRef::ConfluentGuid(guid),
            payload,
        });
    }

    if bytes.len() < PREFIX_LENGTH || bytes[0] != 0 {
        return None;
    }

    let id = u32::from_be_bytes(bytes[1..PREFIX_LENGTH].try_into().ok()?);

    Some(WireHeader {
        schema_ref: SchemaRef::ConfluentId(i32::try_from(id).ok()?),
        payload: Cow::Borrowed(&bytes[PREFIX_LENGTH..]),
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    const GUID: [u8; 16] = [7; 16];

    fn header(key: &str, value: &[u8]) -> Header {
        Header::new(key, Some(Bytes::copy_from_slice(value)))
    }

    fn guid_header(part: Part) -> Header {
        let value: Vec<u8> = [1].into_iter().chain(GUID).collect();

        header(&format!("__{}_schema_id", part.name()), &value)
    }

    #[test]
    fn wire_formats_find_the_schema() {
        let prefixed = [0, 0, 0, 0, 7, 42];
        let guid = Uuid::from_bytes(GUID);
        let value_guid = [guid_header(Part::Value)];
        let key_guid = [guid_header(Part::Key)];
        let malformed_guid = [header("__value_schema_id", &[1, 2, 3])];
        let indexed_guid: Vec<u8> = [1].into_iter().chain(GUID).chain([0]).collect();
        let indexed = [header("__value_schema_id", &indexed_guid)];
        let malformed_last = [
            guid_header(Part::Value),
            header("__value_schema_id", &[1, 2, 3]),
        ];

        let by_prefix = read_header(Part::Value, &prefixed, &[]);
        let by_header = read_header(Part::Value, &prefixed, &value_guid);
        let other_part = read_header(Part::Value, &prefixed, &key_guid);
        let malformed = read_header(Part::Value, &prefixed, &malformed_guid);
        let last_wins = read_header(Part::Value, &prefixed, &malformed_last);
        let with_indexes = read_header(Part::Value, &[8, 2], &indexed);
        let unnamed = [
            read_header(Part::Value, &[1, 0, 0, 0, 7], &[]),
            read_header(Part::Value, &[0, 0, 0], &[]),
        ];

        assert_eq!(
            by_prefix,
            Some(WireHeader {
                schema_ref: SchemaRef::ConfluentId(7),
                payload: Cow::Borrowed(&prefixed[5..]),
            })
        );
        assert_eq!(
            by_header,
            Some(WireHeader {
                schema_ref: SchemaRef::ConfluentGuid(guid),
                payload: Cow::Borrowed(&prefixed[..]),
            })
        );
        assert_eq!(other_part, by_prefix);
        assert_eq!(malformed, by_prefix);
        assert_eq!(last_wins, by_prefix);
        assert_eq!(
            with_indexes,
            Some(WireHeader {
                schema_ref: SchemaRef::ConfluentGuid(guid),
                payload: Cow::Owned(vec![0, 8, 2]),
            })
        );
        assert_eq!(unnamed, [None, None]);
    }
}
