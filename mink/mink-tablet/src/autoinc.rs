//! Auto-increment id assignment from a shared sequence, with a local cache and recovery of reserved ranges.

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use mink_record::{Row, Scalar};
use mink_table::Schema;
use mink_types::Root;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::schema::Version;

#[async_trait]
pub trait Sequence: Send + Sync {
    async fn get_and_add(&self, count: u64) -> Result<u64, Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdRange {
    pub column_id: u32,
    pub start: i64,
    pub end: i64,
}

impl IdRange {
    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }
}

pub struct AutoIncrement {
    sequence: Arc<dyn Sequence>,
    column_id: u32,
    name: String,
    cache_size: u64,
    max: i64,
    segments: VecDeque<(i64, i64)>,
}

impl AutoIncrement {
    pub fn new(
        schema: &Schema,
        sequence: Arc<dyn Sequence>,
        cache_size: u64,
    ) -> Result<Option<Self>, Error> {
        let Some(name) = schema.auto_increment() else {
            return Ok(None);
        };
        let column = schema
            .column(name)
            .ok_or_else(|| Error::AutoIncrementColumn(name.to_owned()))?;
        let column_id = column
            .id()
            .ok_or_else(|| Error::AutoIncrementColumn(name.to_owned()))?
            .0;
        let max = match column.data_type().root() {
            Root::Int => i64::from(i32::MAX),
            Root::BigInt => i64::MAX,
            _ => return Err(Error::AutoIncrementColumn(name.to_owned())),
        };
        if cache_size == 0 {
            return Err(Error::AutoIncrementCache);
        }
        Ok(Some(AutoIncrement {
            sequence,
            column_id,
            name: name.to_owned(),
            cache_size,
            max,
            segments: VecDeque::new(),
        }))
    }

    pub fn column_id(&self) -> u32 {
        self.column_id
    }

    pub fn cache_size(&self) -> u64 {
        self.cache_size
    }

    pub fn validate_targets(
        &self,
        targets: Option<&[usize]>,
        latest: &Version,
    ) -> Result<(), Error> {
        let position = self.position(latest)?;
        match targets {
            None => Err(Error::AutoIncrementTargets(self.name.clone())),
            Some(targets) if targets.contains(&position) => {
                Err(Error::AutoIncrementTarget(self.name.clone()))
            }
            Some(_) => Ok(()),
        }
    }

    pub fn position(&self, version: &Version) -> Result<usize, Error> {
        version
            .auto_increment
            .ok_or_else(|| Error::AutoIncrementColumn(self.name.clone()))
    }

    pub async fn reserve(&mut self, count: usize) -> Result<(), Error> {
        while self.available() < count as u64 {
            let start = self.sequence.get_and_add(self.cache_size).await?;
            let start = i64::try_from(start).map_err(|_| self.overflow())?;
            let end = start
                .checked_add(i64::try_from(self.cache_size).map_err(|_| self.overflow())?)
                .ok_or_else(|| self.overflow())?;
            self.segments.push_back((start + 1, end));
        }
        Ok(())
    }

    fn available(&self) -> u64 {
        self.segments
            .iter()
            .map(|(current, end)| (end - current + 1).max(0) as u64)
            .sum()
    }

    pub fn next_value(&mut self) -> Result<i64, Error> {
        loop {
            let Some((current, end)) = self.segments.front_mut() else {
                return Err(Error::AutoIncrementExhausted);
            };
            if *current > *end {
                self.segments.pop_front();
                continue;
            }
            let id = *current;
            *current += 1;
            if id > self.max {
                return Err(self.overflow());
            }
            return Ok(id);
        }
    }

    pub fn fill(&mut self, row: &mut Row<'_>, version: &Version) -> Result<(), Error> {
        let position = self.position(version)?;
        let id = self.next_value()?;
        let field = row
            .get_mut(position)
            .ok_or_else(|| Error::AutoIncrementColumn(self.name.clone()))?;
        *field = Some(if self.max == i64::from(i32::MAX) {
            Scalar::Int(id as i32)
        } else {
            Scalar::BigInt(id)
        });
        Ok(())
    }

    pub fn range(&self) -> IdRange {
        let (start, end) = self.segments.front().copied().unwrap_or((0, -1));
        IdRange {
            column_id: self.column_id,
            start,
            end,
        }
    }

    pub fn update(&mut self, range: IdRange) -> Result<(), Error> {
        if range.column_id != self.column_id {
            return Err(Error::AutoIncrementRange {
                expected: self.column_id,
                found: range.column_id,
            });
        }
        self.segments.clear();
        self.segments.push_back((range.start, range.end));
        Ok(())
    }

    fn overflow(&self) -> Error {
        Error::AutoIncrementOverflow {
            column: self.name.clone(),
            max: self.max,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Tracker {
    range: IdRange,
    cache_size: u64,
    current: i64,
}

impl Tracker {
    pub fn new(range: IdRange, cache_size: u64) -> Self {
        Tracker {
            range,
            cache_size,
            current: range.start - 1,
        }
    }

    pub fn inserted(&mut self, id: i64) {
        self.current = id;
    }

    pub fn range(&self) -> IdRange {
        let start = self.current + 1;
        let mut end = self.range.end;
        while start > end {
            end += self.cache_size as i64;
        }
        IdRange {
            column_id: self.range.column_id,
            start,
            end,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    struct Counter(AtomicU64);

    #[async_trait]
    impl Sequence for Counter {
        async fn get_and_add(&self, count: u64) -> Result<u64, Error> {
            Ok(self.0.fetch_add(count, Ordering::SeqCst))
        }
    }

    fn range(start: i64, end: i64) -> IdRange {
        IdRange {
            column_id: 1,
            start,
            end,
        }
    }

    #[test]
    fn tracker_continues_inside_the_segment() {
        let mut tracker = Tracker::new(range(1, 100), 100);
        assert_eq!(tracker.range(), range(1, 100));
        tracker.inserted(7);
        assert_eq!(tracker.range(), range(8, 100));
    }

    #[test]
    fn tracker_steps_to_the_segment_the_inserts_came_from() {
        let mut tracker = Tracker::new(range(50, 100), 100);
        tracker.inserted(301);
        assert_eq!(tracker.range(), range(302, 400));
        tracker.inserted(400);
        assert_eq!(tracker.range(), range(401, 500));
    }

    #[test]
    fn tracker_with_exhausted_snapshot_range() {
        let tracker = Tracker::new(range(101, 100), 100);
        assert_eq!(tracker.range(), range(101, 200));
    }

    #[tokio::test]
    async fn segments_are_consumed_in_order_without_gaps() {
        let counter = Arc::new(Counter(AtomicU64::new(0)));
        let mut generator = generator(counter, 3);
        generator.reserve(5).await.unwrap();
        assert_eq!(generator.range(), range(1, 3));
        let ids: Vec<i64> = (0..5).map(|_| generator.next_value().unwrap()).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
        assert_eq!(generator.range(), range(6, 6));
        generator.next_value().unwrap();
        assert!(matches!(
            generator.next_value(),
            Err(Error::AutoIncrementExhausted)
        ));
    }

    #[tokio::test]
    async fn update_replaces_the_segments() {
        let counter = Arc::new(Counter(AtomicU64::new(0)));
        let mut generator = generator(counter, 3);
        generator.reserve(1).await.unwrap();
        generator.update(range(40, 41)).unwrap();
        assert_eq!(generator.next_value().unwrap(), 40);
        assert_eq!(generator.next_value().unwrap(), 41);
        generator.reserve(1).await.unwrap();
        assert_eq!(generator.next_value().unwrap(), 4);
        let err = generator
            .update(IdRange {
                column_id: 9,
                start: 0,
                end: 0,
            })
            .unwrap_err();
        assert!(matches!(err, Error::AutoIncrementRange { .. }));
    }

    fn generator(sequence: Arc<dyn Sequence>, cache_size: u64) -> AutoIncrement {
        AutoIncrement {
            sequence,
            column_id: 1,
            name: "seq".into(),
            cache_size,
            max: i64::MAX,
            segments: VecDeque::new(),
        }
    }
}
