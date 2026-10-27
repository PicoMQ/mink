use bytes::Bytes;

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Part {
    Key,
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    offset: i64,
    timestamp_ms: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<Header>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    key: String,
    value: Option<Bytes>,
}

impl Part {
    pub const ALL: [Part; 2] = [Part::Key, Part::Value];

    pub fn name(self) -> &'static str {
        match self {
            Part::Key => "key",
            Part::Value => "value",
        }
    }

    pub fn from_name(name: &str) -> Option<Part> {
        Part::ALL.into_iter().find(|part| part.name() == name)
    }
}

impl Record {
    pub fn new(
        offset: i64,
        timestamp_ms: i64,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<Header>,
    ) -> Result<Record, Error> {
        if offset < 0 {
            return Err(Error::RecordOffset(offset));
        }

        Ok(Record {
            offset,
            timestamp_ms,
            key,
            value,
            headers,
        })
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    pub fn key(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    pub fn part(&self, part: Part) -> Option<&Bytes> {
        match part {
            Part::Key => self.key(),
            Part::Value => self.value(),
        }
    }

    pub fn size(&self) -> u64 {
        let length = |bytes: Option<&Bytes>| bytes.map_or(0, |bytes| bytes.len() as u64);
        let headers: u64 = self
            .headers
            .iter()
            .map(|header| header.key().len() as u64 + length(header.value()))
            .sum();

        length(self.key()) + length(self.value()) + headers
    }

    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}

impl Header {
    pub fn new(key: impl Into<String>, value: Option<Bytes>) -> Header {
        Header {
            key: key.into(),
            value,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_offset_cannot_be_negative() {
        assert_eq!(
            Record::new(-1, 0, None, None, Vec::new()),
            Err(Error::RecordOffset(-1))
        );
    }
}
