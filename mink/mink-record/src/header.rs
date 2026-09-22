//! The fixed-size log batch header: field layout, read and write, checksum, and the fields the server rewrites.

use mink_table::SchemaId;

use crate::Error;

pub const MAGIC: u8 = 1;
pub const HEADER_SIZE: usize = 52;
pub const LOG_OVERHEAD: usize = 12;

pub const NO_WRITER_ID: i64 = -1;
pub const NO_BATCH_SEQUENCE: i32 = -1;
pub const NO_LEADER_EPOCH: i32 = -1;

// Little-endian field offsets. `length` counts the bytes after itself; the CRC covers
// everything from the schema id to the end, so the fields before it are the server's to rewrite.
const BASE_OFFSET: usize = 0;
const LENGTH: usize = 8;
const MAGIC_OFFSET: usize = 12;
const COMMIT_TIMESTAMP: usize = 13;
const LEADER_EPOCH: usize = 21;
const CRC: usize = 25;
const SCHEMA_ID: usize = 29;
const ATTRIBUTES: usize = 31;
const LAST_OFFSET_DELTA: usize = 32;
const WRITER_ID: usize = 36;
const BATCH_SEQUENCE: usize = 44;
const RECORD_COUNT: usize = 48;

const APPEND_ONLY_FLAG: u8 = 0x01;

pub const MAX_SCHEMA_ID: u32 = i16::MAX as u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub base_offset: i64,
    pub size: usize,
    pub commit_timestamp: i64,
    pub leader_epoch: i32,
    pub crc: u32,
    pub schema_id: SchemaId,
    pub append_only: bool,
    pub last_offset_delta: i32,
    pub writer_id: i64,
    pub batch_sequence: i32,
    pub record_count: i32,
}

impl Header {
    pub fn read(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < HEADER_SIZE {
            return Err(Error::Truncated {
                needed: HEADER_SIZE,
                found: bytes.len(),
            });
        }

        let magic = bytes[MAGIC_OFFSET];
        if magic != MAGIC {
            return Err(Error::Magic(magic));
        }

        let length = i32(bytes, LENGTH);
        let size = usize::try_from(length)
            .ok()
            .and_then(|length| length.checked_add(LOG_OVERHEAD))
            .filter(|size| *size >= HEADER_SIZE)
            .ok_or(Error::Length(length))?;

        let schema_id = i16(bytes, SCHEMA_ID);
        let schema_id = u32::try_from(schema_id)
            .map(SchemaId)
            .map_err(|_| Error::SchemaId(i64::from(schema_id)))?;

        let record_count = i32(bytes, RECORD_COUNT);
        if record_count < 0 {
            return Err(Error::RecordCount(record_count));
        }

        Ok(Header {
            base_offset: i64(bytes, BASE_OFFSET),
            size,
            commit_timestamp: i64(bytes, COMMIT_TIMESTAMP),
            leader_epoch: i32(bytes, LEADER_EPOCH),
            crc: u32(bytes, CRC),
            schema_id,
            append_only: bytes[ATTRIBUTES] & APPEND_ONLY_FLAG != 0,
            last_offset_delta: i32(bytes, LAST_OFFSET_DELTA),
            writer_id: i64(bytes, WRITER_ID),
            batch_sequence: i32(bytes, BATCH_SEQUENCE),
            record_count,
        })
    }

    pub fn write(&self, bytes: &mut [u8]) -> Result<(), Error> {
        if self.schema_id.0 > MAX_SCHEMA_ID {
            return Err(Error::SchemaId(i64::from(self.schema_id.0)));
        }
        let length =
            i32::try_from(self.size - LOG_OVERHEAD).map_err(|_| Error::BatchTooLarge(self.size))?;

        put(bytes, BASE_OFFSET, &self.base_offset.to_le_bytes());
        put(bytes, LENGTH, &length.to_le_bytes());
        bytes[MAGIC_OFFSET] = MAGIC;
        put(
            bytes,
            COMMIT_TIMESTAMP,
            &self.commit_timestamp.to_le_bytes(),
        );
        put(bytes, LEADER_EPOCH, &self.leader_epoch.to_le_bytes());
        put(bytes, CRC, &self.crc.to_le_bytes());
        put(bytes, SCHEMA_ID, &(self.schema_id.0 as i16).to_le_bytes());
        bytes[ATTRIBUTES] = if self.append_only {
            APPEND_ONLY_FLAG
        } else {
            0
        };
        put(
            bytes,
            LAST_OFFSET_DELTA,
            &self.last_offset_delta.to_le_bytes(),
        );
        put(bytes, WRITER_ID, &self.writer_id.to_le_bytes());
        put(bytes, BATCH_SEQUENCE, &self.batch_sequence.to_le_bytes());
        put(bytes, RECORD_COUNT, &self.record_count.to_le_bytes());

        Ok(())
    }

    pub fn last_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }

    pub fn next_offset(&self) -> i64 {
        self.last_offset() + 1
    }
}

pub fn checksum(batch: &[u8]) -> u32 {
    crc32c::crc32c(&batch[SCHEMA_ID..])
}

pub fn seal(batch: &mut [u8]) {
    let crc = checksum(batch);
    put(batch, CRC, &crc.to_le_bytes());
}

pub fn set_base_offset(batch: &mut [u8], base_offset: i64) {
    put(batch, BASE_OFFSET, &base_offset.to_le_bytes());
}

pub fn set_commit_timestamp(batch: &mut [u8], timestamp: i64) {
    put(batch, COMMIT_TIMESTAMP, &timestamp.to_le_bytes());
}

pub fn set_leader_epoch(batch: &mut [u8], epoch: i32) {
    put(batch, LEADER_EPOCH, &epoch.to_le_bytes());
}

pub(crate) fn set_size(batch: &mut [u8], size: usize) -> Result<(), Error> {
    let length = i32::try_from(size - LOG_OVERHEAD).map_err(|_| Error::BatchTooLarge(size))?;
    put(batch, LENGTH, &length.to_le_bytes());
    Ok(())
}

fn put(bytes: &mut [u8], at: usize, value: &[u8]) {
    bytes[at..at + value.len()].copy_from_slice(value);
}

fn i16(bytes: &[u8], at: usize) -> i16 {
    i16::from_le_bytes(bytes[at..at + 2].try_into().unwrap())
}

fn i32(bytes: &[u8], at: usize) -> i32 {
    i32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

fn u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

fn i64(bytes: &[u8], at: usize) -> i64 {
    i64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> Header {
        Header {
            base_offset: 100,
            size: HEADER_SIZE + 10,
            commit_timestamp: 1_700_000_000_123,
            leader_epoch: 2,
            crc: 0,
            schema_id: SchemaId(1),
            append_only: true,
            last_offset_delta: 3,
            writer_id: 7,
            batch_sequence: 3,
            record_count: 4,
        }
    }

    #[test]
    fn round_trips() {
        let mut bytes = vec![0; HEADER_SIZE + 10];
        header().write(&mut bytes).unwrap();
        assert_eq!(Header::read(&bytes).unwrap(), header());
        assert_eq!(header().last_offset(), 103);
        assert_eq!(header().next_offset(), 104);
    }

    #[test]
    fn server_fields_are_outside_the_crc() {
        let mut bytes = vec![0xab; HEADER_SIZE + 10];
        header().write(&mut bytes).unwrap();
        seal(&mut bytes);
        let sealed = Header::read(&bytes).unwrap().crc;
        set_base_offset(&mut bytes, 999);
        set_commit_timestamp(&mut bytes, 1);
        set_leader_epoch(&mut bytes, 5);
        assert_eq!(checksum(&bytes), sealed);
        let read = Header::read(&bytes).unwrap();
        assert_eq!(
            (read.base_offset, read.commit_timestamp, read.leader_epoch),
            (999, 1, 5)
        );
    }

    #[test]
    fn rejects_bad_headers() {
        let mut bytes = vec![0; HEADER_SIZE];
        header().write(&mut bytes).unwrap();
        assert!(matches!(
            Header::read(&bytes[..10]).unwrap_err(),
            Error::Truncated {
                needed: HEADER_SIZE,
                ..
            }
        ));
        let mut wrong_magic = bytes.clone();
        wrong_magic[MAGIC_OFFSET] = 0;
        assert_eq!(Header::read(&wrong_magic).unwrap_err(), Error::Magic(0));
        let mut short = bytes.clone();
        put(&mut short, LENGTH, &10i32.to_le_bytes());
        assert_eq!(Header::read(&short).unwrap_err(), Error::Length(10));
        let mut negative = bytes.clone();
        put(&mut negative, RECORD_COUNT, &(-1i32).to_le_bytes());
        assert_eq!(Header::read(&negative).unwrap_err(), Error::RecordCount(-1));
        let mut wide = header();
        wide.schema_id = SchemaId(MAX_SCHEMA_ID + 1);
        assert!(matches!(
            wide.write(&mut bytes).unwrap_err(),
            Error::SchemaId(_)
        ));
    }
}
