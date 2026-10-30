use std::borrow::Cow;
use std::collections::BTreeSet;
use std::io::Read;

use flate2::read::ZlibDecoder;
use mink_core::{Header, Part, Record};
use uuid::Uuid;

use crate::registry::{SchemaId, SchemaKey};

const CONFLUENT_HEADER_LENGTH: usize = 17;
const CONFLUENT_PREFIX_LENGTH: usize = 5;
const GLUE_HEADER_VERSION: u8 = 3;
const GLUE_UNCOMPRESSED: u8 = 0;
const GLUE_ZLIB: u8 = 5;
const GLUE_PREFIX_LENGTH: usize = 18;
const GLUE_INFLATED_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartSchemaId {
    part: Part,
    schema_id: SchemaId,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct WireHeader<'a> {
    pub(crate) schema_id: SchemaId,
    pub(crate) payload: Cow<'a, [u8]>,
}

impl PartSchemaId {
    pub fn new(part: Part, schema_id: SchemaId) -> PartSchemaId {
        PartSchemaId { part, schema_id }
    }

    pub fn part(&self) -> Part {
        self.part
    }

    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }
}

pub fn schema_ids(records: &[Record]) -> BTreeSet<PartSchemaId> {
    let mut schema_ids = BTreeSet::new();

    for record in records {
        for part in Part::ALL {
            let bytes = record.part(part).map(AsRef::as_ref);

            if let Some(header) = bytes.and_then(|bytes| read_header(part, bytes, record.headers()))
            {
                schema_ids.insert(PartSchemaId::new(part, header.schema_id));
            }
        }
    }

    schema_ids
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
        .filter(|value| value.len() >= CONFLUENT_HEADER_LENGTH && value[0] == 1);

    if let Some(value) = guid_header {
        let guid = Uuid::from_slice(&value[1..CONFLUENT_HEADER_LENGTH]).ok()?;
        let payload = match &value[CONFLUENT_HEADER_LENGTH..] {
            [] => Cow::Borrowed(bytes),
            leading => Cow::Owned([leading, bytes].concat()),
        };

        return Some(WireHeader {
            schema_id: SchemaId::ConfluentGuid(guid),
            payload,
        });
    }

    if bytes.len() >= GLUE_PREFIX_LENGTH && bytes[0] == GLUE_HEADER_VERSION {
        let version = Uuid::from_slice(&bytes[2..GLUE_PREFIX_LENGTH]).ok()?;
        let payload = match bytes[1] {
            GLUE_UNCOMPRESSED => Cow::Borrowed(&bytes[GLUE_PREFIX_LENGTH..]),
            GLUE_ZLIB => Cow::Owned(inflated(&bytes[GLUE_PREFIX_LENGTH..])?),
            _ => return None,
        };

        return Some(WireHeader {
            schema_id: SchemaId::Key(SchemaKey::Glue(version)),
            payload,
        });
    }

    if bytes.len() < CONFLUENT_PREFIX_LENGTH || bytes[0] != 0 {
        return None;
    }

    let id = u32::from_be_bytes(bytes[1..CONFLUENT_PREFIX_LENGTH].try_into().ok()?);

    Some(WireHeader {
        schema_id: SchemaId::Key(SchemaKey::Confluent(i32::try_from(id).ok()?)),
        payload: Cow::Borrowed(&bytes[CONFLUENT_PREFIX_LENGTH..]),
    })
}

fn inflated(compressed: &[u8]) -> Option<Vec<u8>> {
    let mut inflated = Vec::new();

    ZlibDecoder::new(compressed)
        .take(GLUE_INFLATED_BYTES + 1)
        .read_to_end(&mut inflated)
        .ok()?;

    (inflated.len() as u64 <= GLUE_INFLATED_BYTES).then_some(inflated)
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
                schema_id: SchemaId::Key(SchemaKey::Confluent(7)),
                payload: Cow::Borrowed(&prefixed[5..]),
            })
        );
        assert_eq!(
            by_header,
            Some(WireHeader {
                schema_id: SchemaId::ConfluentGuid(guid),
                payload: Cow::Borrowed(&prefixed[..]),
            })
        );
        assert_eq!(other_part, by_prefix);
        assert_eq!(malformed, by_prefix);
        assert_eq!(last_wins, by_prefix);
        assert_eq!(
            with_indexes,
            Some(WireHeader {
                schema_id: SchemaId::ConfluentGuid(guid),
                payload: Cow::Owned(vec![0, 8, 2]),
            })
        );
        assert_eq!(unnamed, [None, None]);
    }

    fn glue(compression: u8, payload: &[u8]) -> Vec<u8> {
        [GLUE_HEADER_VERSION, compression]
            .into_iter()
            .chain(GUID)
            .chain(payload.iter().copied())
            .collect()
    }

    fn zlib(payload: &[u8]) -> Vec<u8> {
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, payload).unwrap();

        encoder.finish().unwrap()
    }

    #[test]
    fn glue_prefixes_name_a_schema_version_and_may_be_zlib_compressed() {
        let version = SchemaId::Key(SchemaKey::Glue(Uuid::from_bytes(GUID)));
        let plain = glue(GLUE_UNCOMPRESSED, &[2, 4]);
        let compressed = glue(GLUE_ZLIB, &zlib(&[2, 4]));
        let unknown = glue(1, &[2, 4]);
        let too_large = glue(GLUE_ZLIB, &zlib(&vec![0; GLUE_INFLATED_BYTES as usize + 1]));

        let read = |bytes: &[u8]| {
            read_header(Part::Value, bytes, &[])
                .map(|header| (header.schema_id, header.payload.into_owned()))
        };

        assert_eq!(read(&plain), Some((version, vec![2, 4])));
        assert_eq!(read(&compressed), Some((version, vec![2, 4])));
        assert_eq!(read(&unknown), None);
        assert_eq!(read(&plain[..GLUE_PREFIX_LENGTH - 1]), None);
        assert_eq!(read(&too_large), None);
    }
}
