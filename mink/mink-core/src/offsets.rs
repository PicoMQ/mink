use crate::Error;
use crate::file::File;
use crate::topic::Partition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OffsetRange {
    start: i64,
    end: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Offsets {
    partition: Partition,
    range: OffsetRange,
}

impl OffsetRange {
    pub fn new(start: i64, end: i64) -> Result<OffsetRange, Error> {
        if start < 0 || end < start {
            return Err(Error::OffsetRange { start, end });
        }

        Ok(OffsetRange { start, end })
    }

    pub fn start(&self) -> i64 {
        self.start
    }

    pub fn end(&self) -> i64 {
        self.end
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    pub fn includes(&self, offset: i64) -> bool {
        self.start <= offset && offset < self.end
    }

    pub fn contains(&self, other: OffsetRange) -> bool {
        self.start <= other.start && other.end <= self.end
    }
}

impl Offsets {
    pub fn new(partition: Partition, start_offset: i64, end_offset: i64) -> Result<Offsets, Error> {
        let range = OffsetRange::new(start_offset, end_offset)?;

        Ok(Offsets { partition, range })
    }

    pub fn partition(&self) -> &Partition {
        &self.partition
    }

    pub fn range(&self) -> OffsetRange {
        self.range
    }

    pub fn start_offset(&self) -> i64 {
        self.range.start
    }

    pub fn end_offset(&self) -> i64 {
        self.range.end
    }

    pub fn covers(&self, file: &File) -> bool {
        file.partition() == &self.partition && self.range.contains(file.range())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_ranges_are_non_negative_and_ordered() {
        assert!(OffsetRange::new(5, 5).unwrap().is_empty());
        assert!(OffsetRange::new(5, 7).unwrap().includes(6));
        assert!(!OffsetRange::new(5, 7).unwrap().includes(7));
        assert_eq!(
            OffsetRange::new(6, 5),
            Err(Error::OffsetRange { start: 6, end: 5 })
        );
        assert_eq!(
            OffsetRange::new(-1, 5),
            Err(Error::OffsetRange { start: -1, end: 5 })
        );
        assert_eq!(
            Offsets::new(Partition::new("orders", 0).unwrap(), 6, 5),
            Err(Error::OffsetRange { start: 6, end: 5 })
        );
    }
}
