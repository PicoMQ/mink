//! Idempotent producers: id allocation, epoch bumps, and per bucket the sequence window that
//! turns retried batches into duplicates and skipped sequences into errors.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};
use mink_common::sync::lock;
use mink_table::Bucket;

use crate::Kafka;
use crate::dispatch::Request;
use crate::error::Error;

const REMEMBERED_BATCHES: usize = 5;

#[derive(Default)]
pub(crate) struct Producers {
    states: Mutex<HashMap<(Bucket, i64), State>>,
}

struct State {
    epoch: i16,
    next: i32,
    recent: VecDeque<Accepted>,
}

struct Accepted {
    first: i32,
    last: i32,
    base_offset: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Check {
    Append,
    Duplicate { base_offset: i64 },
}

fn last_of(first: i32, count: i32) -> i32 {
    first.wrapping_add(count - 1)
}

fn after(last: i32) -> i32 {
    if last == i32::MAX { 0 } else { last + 1 }
}

impl Producers {
    pub fn check(
        &self,
        bucket: Bucket,
        producer_id: i64,
        epoch: i16,
        first: i32,
        count: i32,
    ) -> Result<Check, Error> {
        let states = lock(&self.states);
        let Some(state) = states.get(&(bucket, producer_id)) else {
            return Ok(Check::Append);
        };
        if epoch < state.epoch {
            return Err(Error::ProducerFenced {
                producer_id,
                given: epoch,
                current: state.epoch,
            });
        }
        if epoch > state.epoch || first == state.next {
            return Ok(Check::Append);
        }
        let last = last_of(first, count);
        if let Some(seen) = state
            .recent
            .iter()
            .find(|a| a.first == first && a.last == last)
        {
            return Ok(Check::Duplicate {
                base_offset: seen.base_offset,
            });
        }

        Err(Error::OutOfOrderSequence {
            bucket,
            producer_id,
            sequence: first,
            expected: state.next,
        })
    }

    pub fn accept(
        &self,
        bucket: Bucket,
        producer_id: i64,
        epoch: i16,
        first: i32,
        count: i32,
        base_offset: i64,
    ) {
        let mut states = lock(&self.states);
        let state = states.entry((bucket, producer_id)).or_insert(State {
            epoch,
            next: first,
            recent: VecDeque::with_capacity(REMEMBERED_BATCHES),
        });
        if epoch > state.epoch {
            state.epoch = epoch;
            state.recent.clear();
        }
        let last = last_of(first, count);
        state.next = after(last);
        if state.recent.len() == REMEMBERED_BATCHES {
            state.recent.pop_front();
        }
        state.recent.push_back(Accepted {
            first,
            last,
            base_offset,
        });
    }
}

impl Kafka {
    pub(crate) async fn init_producer_id(
        &self,
        _request: &Request,
        init: InitProducerIdRequest,
    ) -> Result<InitProducerIdResponse, Error> {
        if init
            .transactional_id
            .as_ref()
            .is_some_and(|id| !id.as_str().is_empty())
        {
            return Ok(InitProducerIdResponse::default()
                .with_error_code(ResponseError::InvalidRequest.code())
                .with_producer_id(ProducerId(-1))
                .with_producer_epoch(-1));
        }
        if init.producer_id.0 >= 0 {
            return Ok(InitProducerIdResponse::default()
                .with_producer_id(init.producer_id)
                .with_producer_epoch(init.producer_epoch.saturating_add(1)));
        }

        let id = self.inner.service.init_writer().await?;

        Ok(InitProducerIdResponse::default()
            .with_producer_id(ProducerId(id))
            .with_producer_epoch(0))
    }
}

#[cfg(test)]
mod tests {
    use mink_table::{BucketId, Id};

    use super::*;

    fn bucket() -> Bucket {
        Bucket::new(Id(1), BucketId(0))
    }

    #[test]
    fn duplicates_return_the_original_offset_and_gaps_fail() {
        let producers = Producers::default();
        let b = bucket();
        assert_eq!(producers.check(b, 7, 0, 0, 3).unwrap(), Check::Append);
        producers.accept(b, 7, 0, 0, 3, 100);
        assert_eq!(
            producers.check(b, 7, 0, 0, 3).unwrap(),
            Check::Duplicate { base_offset: 100 }
        );
        assert_eq!(producers.check(b, 7, 0, 3, 2).unwrap(), Check::Append);
        assert!(matches!(
            producers.check(b, 7, 0, 5, 1),
            Err(Error::OutOfOrderSequence { expected: 3, .. })
        ));
        assert!(matches!(
            producers.check(b, 7, 0, 1, 2),
            Err(Error::OutOfOrderSequence { .. })
        ));
    }

    #[test]
    fn epochs_fence_and_reset() {
        let producers = Producers::default();
        let b = bucket();
        producers.accept(b, 7, 1, 0, 1, 0);
        assert!(matches!(
            producers.check(b, 7, 0, 1, 1),
            Err(Error::ProducerFenced { .. })
        ));
        assert_eq!(producers.check(b, 7, 2, 0, 1).unwrap(), Check::Append);
        producers.accept(b, 7, 2, 0, 1, 5);
        assert_eq!(producers.check(b, 7, 2, 1, 1).unwrap(), Check::Append);
    }

    #[test]
    fn sequences_wrap_at_i32_max() {
        let producers = Producers::default();
        let b = bucket();
        producers.accept(b, 1, 0, i32::MAX - 1, 2, 0);
        assert_eq!(producers.check(b, 1, 0, 0, 1).unwrap(), Check::Append);
    }
}
