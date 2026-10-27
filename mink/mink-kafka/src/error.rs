use std::io;
use std::time::Duration;

use kafka_protocol::messages::BrokerId;
use mink_core::Partition;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("encoding a request: {0}")]
    Encode(anyhow::Error),
    #[error("decoding a response: {0}")]
    Decode(anyhow::Error),
    #[error("a request of {length} bytes exceeds the frame limit")]
    RequestTooLarge { length: usize },
    #[error("a response frame of {length} bytes is out of range")]
    FrameSize { length: i64 },
    #[error("response correlation id {actual} does not match request {expected}")]
    Correlation { expected: i32, actual: i32 },
    #[error("the broker did not answer within the request timeout")]
    Timeout,
    #[error("the connection was closed after an earlier failure")]
    Closed,
    #[error("broker supports no usable version of api key {key}")]
    UnsupportedVersion { key: i16 },
    #[error("broker error code {code}")]
    Broker { code: i16 },
    #[error("response has no data for {0:?}")]
    MissingPartition(Partition),
    #[error("{0:?} has no leader")]
    NoLeader(Partition),
    #[error("broker {} is not in the cluster metadata", .0.0)]
    UnknownBroker(BrokerId),
    #[error("{0:?} is not in the cluster metadata")]
    UnknownPartition(Partition),
    #[error("no broker answered: {0:?}")]
    Unreachable(Vec<String>),
    #[error("timestamp {0} is negative")]
    InvalidTimestamp(i64),
    #[error("cannot read to offset {end}, past the last stable offset {stable}")]
    BeyondStableOffset { end: i64, stable: i64 },
    #[error("fetches at offset {offset} returned no batches before the retry deadline")]
    Stalled { offset: i64 },
    #[error("corrupt batch at offset {base_offset}")]
    CorruptBatch { base_offset: i64 },
    #[error("header key at offset {offset} is not utf-8")]
    InvalidHeaderKey { offset: i64 },
    #[error("bootstrap needs at least one address")]
    EmptyBootstrap,
    #[error("max_bytes must be between 1 and the frame limit, not {0}")]
    MaxBytes(i32),
    #[error("max_wait {0:?} does not fit in 32-bit milliseconds")]
    MaxWait(Duration),
}
