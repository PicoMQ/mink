use std::collections::HashSet;

use kafka_protocol::messages::fetch_response::AbortedTransaction;
use mink_core::Record;

use crate::batch::{Batch, BatchType, ControlType};

pub(crate) fn committed(
    batches: Vec<Batch>,
    aborted: &[AbortedTransaction],
    fetch_offset: i64,
) -> Vec<Record> {
    let mut upcoming: Vec<_> = aborted.iter().collect();
    upcoming.sort_by_key(|transaction| transaction.first_offset);
    let mut upcoming = upcoming.into_iter().peekable();
    let mut aborting = HashSet::new();
    let mut records = Vec::new();

    for batch in batches {
        while let Some(transaction) =
            upcoming.next_if(|transaction| transaction.first_offset <= batch.last_offset)
        {
            aborting.insert(transaction.producer_id.0);
        }

        match batch.batch_type {
            BatchType::Control(ControlType::Abort) => {
                aborting.remove(&batch.producer_id);
            }
            BatchType::Control(_) => {}
            BatchType::Data(_) if batch.transactional && aborting.contains(&batch.producer_id) => {}
            BatchType::Data(data) => records.extend(
                data.into_iter()
                    .filter(|record| record.offset() >= fetch_offset),
            ),
        }
    }

    records
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kafka_protocol::messages::ProducerId;
    use proptest::prelude::*;

    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum Step {
        Plain(i64),
        Transactional { producer: i64, count: i64 },
        End { producer: i64, commit: bool },
    }

    struct Log {
        batches: Vec<Batch>,
        aborted: Vec<(AbortedTransaction, i64)>,
    }

    impl Log {
        fn new(steps: &[Step]) -> Log {
            let mut log = Log {
                batches: Vec::new(),
                aborted: Vec::new(),
            };
            let mut open: Vec<(i64, i64)> = Vec::new();

            for step in steps {
                match *step {
                    Step::Plain(count) => log.data(-1, false, count),
                    Step::Transactional { producer, count } => {
                        if !open.iter().any(|(p, _)| *p == producer) {
                            open.push((producer, log.end()));
                        }

                        log.data(producer, true, count);
                    }
                    Step::End { producer, commit } => {
                        if let Some(index) = open.iter().position(|(p, _)| *p == producer) {
                            let (producer, first) = open.remove(index);
                            log.marker(producer, first, commit);
                        }
                    }
                }
            }

            for (producer, first) in open {
                log.marker(producer, first, false);
            }

            log
        }

        fn end(&self) -> i64 {
            self.batches.last().map_or(0, |batch| batch.last_offset + 1)
        }

        fn data(&mut self, producer_id: i64, transactional: bool, count: i64) {
            let base_offset = self.end();
            let records = (base_offset..base_offset + count)
                .map(|offset| {
                    Record::new(
                        offset,
                        offset,
                        None,
                        Some(Bytes::from(offset.to_string())),
                        Vec::new(),
                    )
                    .unwrap()
                })
                .collect();

            self.batches.push(Batch {
                base_offset,
                last_offset: base_offset + count - 1,
                producer_id,
                transactional,
                batch_type: BatchType::Data(records),
            });
        }

        fn marker(&mut self, producer_id: i64, first_offset: i64, commit: bool) {
            let offset = self.end();
            let control = if commit {
                ControlType::Commit
            } else {
                ControlType::Abort
            };

            self.batches.push(Batch {
                base_offset: offset,
                last_offset: offset,
                producer_id,
                transactional: true,
                batch_type: BatchType::Control(control),
            });
            if !commit {
                let transaction = AbortedTransaction::default()
                    .with_producer_id(ProducerId(producer_id))
                    .with_first_offset(first_offset);
                self.aborted.push((transaction, offset));
            }
        }

        fn fetch(&self, from: i64, to: i64) -> Vec<Record> {
            let batches: Vec<_> = self
                .batches
                .iter()
                .filter(|batch| batch.last_offset >= from && batch.base_offset < to)
                .cloned()
                .collect();
            let aborted: Vec<_> = self
                .aborted
                .iter()
                .filter(|(transaction, marker)| *marker >= from && transaction.first_offset < to)
                .map(|(transaction, _)| transaction.clone())
                .collect();

            committed(batches, &aborted, from)
                .into_iter()
                .filter(|record| record.offset() < to)
                .collect()
        }
    }

    fn offsets(records: &[Record]) -> Vec<i64> {
        records.iter().map(Record::offset).collect()
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            (1..4i64).prop_map(Step::Plain),
            (1..4i64, 1..4i64)
                .prop_map(|(producer, count)| Step::Transactional { producer, count }),
            (1..4i64, any::<bool>()).prop_map(|(producer, commit)| Step::End { producer, commit }),
        ]
    }

    #[test]
    fn aborted_records_are_dropped_and_committed_ones_kept() {
        let log = Log::new(&[
            Step::Transactional {
                producer: 1,
                count: 2,
            },
            Step::Transactional {
                producer: 2,
                count: 2,
            },
            Step::End {
                producer: 1,
                commit: true,
            },
            Step::End {
                producer: 2,
                commit: false,
            },
            Step::Plain(1),
        ]);

        let records = log.fetch(0, i64::MAX);

        assert_eq!(offsets(&records), [0, 1, 6]);
    }

    #[test]
    fn markers_never_become_records() {
        let log = Log::new(&[
            Step::Transactional {
                producer: 1,
                count: 1,
            },
            Step::End {
                producer: 1,
                commit: true,
            },
        ]);

        let records = log.fetch(0, i64::MAX);

        assert_eq!(offsets(&records), [0]);
    }

    #[test]
    fn fixtures_keep_exactly_the_committed_records() {
        for fixture in mink_conformance::record_fixtures()
            .into_iter()
            .filter(|fixture| fixture.corrupt_at().is_none())
        {
            let batches = Batch::parse(fixture.batches().clone()).unwrap();

            let records = committed(batches, fixture.aborted(), fixture.fetch_offset());

            assert_eq!(records, fixture.expected(), "{}", fixture.name());
        }
    }

    proptest! {
        #[test]
        fn committed_records_do_not_depend_on_the_fetch_start(
            steps in prop::collection::vec(step(), 1..40),
            start in 0..200i64,
            bounds_at in prop::collection::vec(0..200i64, 0..6),
        ) {
            let log = Log::new(&steps);
            let end = log.end();
            let start = start % (end + 1);
            let mut bounds: Vec<_> = bounds_at.into_iter().map(|at| start + at % (end - start + 1)).collect();
            bounds.extend([start, end]);
            bounds.sort();
            bounds.dedup();

            let whole = log.fetch(start, end);
            let split: Vec<_> = bounds.windows(2).flat_map(|w| log.fetch(w[0], w[1])).collect();

            prop_assert_eq!(whole, split);
        }
    }
}
