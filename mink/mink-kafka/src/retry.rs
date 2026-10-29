use std::time::{Duration, Instant};

use crate::Error;
use crate::codes::{
    FENCED_LEADER_EPOCH, LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER, UNKNOWN_LEADER_EPOCH,
    UNKNOWN_TOPIC_OR_PARTITION,
};

const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const RETRIABLE: [i16; 5] = [
    UNKNOWN_TOPIC_OR_PARTITION,
    LEADER_NOT_AVAILABLE,
    NOT_LEADER_OR_FOLLOWER,
    FENCED_LEADER_EPOCH,
    UNKNOWN_LEADER_EPOCH,
];

pub(crate) struct Backoff {
    deadline: Instant,
    delay: Duration,
}

impl Backoff {
    pub(crate) fn new(deadline: Duration) -> Backoff {
        Backoff {
            deadline: Instant::now() + deadline,
            delay: FIRST_BACKOFF,
        }
    }

    pub(crate) async fn wait(&mut self) -> bool {
        if Instant::now() + self.delay >= self.deadline {
            return false;
        }

        tokio::time::sleep(self.delay).await;
        self.delay = (self.delay * 2).min(MAX_BACKOFF);

        true
    }
}

pub(crate) fn retriable(error: &Error) -> bool {
    match error {
        Error::Broker { code } => RETRIABLE.contains(code),
        Error::NoLeader(_)
        | Error::UnknownBroker(_)
        | Error::Unreachable(_)
        | Error::Io(_)
        | Error::Timeout
        | Error::Closed
        | Error::Correlation { .. }
        | Error::Decode(_)
        | Error::FrameSize { .. } => true,
        Error::Encode(_)
        | Error::RequestTooLarge { .. }
        | Error::UnsupportedVersion { .. }
        | Error::MissingPartition(_)
        | Error::UnknownPartition(_)
        | Error::InvalidTimestamp(_)
        | Error::BeyondStableOffset { .. }
        | Error::Stalled { .. }
        | Error::CorruptBatch { .. }
        | Error::InvalidHeaderKey { .. }
        | Error::EmptyBootstrap
        | Error::MaxBytes(_)
        | Error::MaxWait(_)
        | Error::Tls(_)
        | Error::CaFile { .. }
        | Error::ServerName(_)
        | Error::Authentication(_)
        | Error::Scram(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use kafka_protocol::messages::BrokerId;
    use mink_core::Partition;

    use super::*;

    fn orders() -> Partition {
        Partition::new("orders", 0).unwrap()
    }

    #[test]
    fn errors_are_retried_as_the_error_table_says() {
        let retried = [
            Error::Broker {
                code: UNKNOWN_TOPIC_OR_PARTITION,
            },
            Error::Broker {
                code: LEADER_NOT_AVAILABLE,
            },
            Error::Broker {
                code: NOT_LEADER_OR_FOLLOWER,
            },
            Error::Broker {
                code: FENCED_LEADER_EPOCH,
            },
            Error::Broker {
                code: UNKNOWN_LEADER_EPOCH,
            },
            Error::NoLeader(orders()),
            Error::UnknownBroker(BrokerId(3)),
            Error::Unreachable(Vec::new()),
            Error::Io(io::Error::other("lost")),
            Error::Timeout,
            Error::Closed,
            Error::Correlation {
                expected: 1,
                actual: 2,
            },
            Error::Decode(anyhow::anyhow!("bad")),
            Error::FrameSize { length: -1 },
        ];
        let returned = [
            Error::Broker { code: 1 },
            Error::Broker { code: 2 },
            Error::Encode(anyhow::anyhow!("bad")),
            Error::RequestTooLarge { length: 1 },
            Error::UnsupportedVersion { key: 1 },
            Error::MissingPartition(orders()),
            Error::UnknownPartition(orders()),
            Error::BeyondStableOffset { end: 2, stable: 1 },
            Error::Stalled { offset: 0 },
            Error::CorruptBatch { base_offset: 0 },
            Error::InvalidHeaderKey { offset: 0 },
            Error::EmptyBootstrap,
            Error::MaxBytes(0),
            Error::MaxWait(Duration::MAX),
        ];

        for error in &retried {
            assert!(retriable(error), "{error:?}");
        }

        for error in &returned {
            assert!(!retriable(error), "{error:?}");
        }
    }
}
