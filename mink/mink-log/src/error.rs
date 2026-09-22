//! Failures of log appends, reads and writer tracking.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("offset {offset} is outside the log range [{start}, {end}]")]
    OutOfRange { offset: i64, start: i64, end: i64 },
    #[error("timestamp {timestamp} is in the future (now {now})")]
    InvalidTimestamp { timestamp: i64, now: i64 },
    #[error("corrupt batch: {0}")]
    Corrupt(#[from] mink_record::Error),
    #[error("batch carries no records")]
    EmptyBatch,
    #[error("batch offsets are not monotonic")]
    NotMonotonic,
    #[error(
        "out of order batch sequence for writer {writer_id}: {incoming} (incoming), {current} (current)"
    )]
    OutOfOrderSequence {
        writer_id: i64,
        incoming: i32,
        current: i32,
    },
    #[error("stream assigned offset {assigned}, expected {expected}")]
    OffsetMismatch { expected: i64, assigned: i64 },
    #[error("writer snapshot: {0}")]
    Snapshot(String),
    #[error(transparent)]
    Stream(#[from] s3stream::Error),
}
