//! Read isolation levels and the batches and offsets a read returns.

use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FetchIsolation {
    #[default]
    HighWatermark,
    LogEnd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchInfo {
    pub fetch_offset: i64,
    pub high_watermark: i64,
    pub log_end_offset: i64,
    pub batches: Vec<Bytes>,
}

impl FetchInfo {
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn size(&self) -> usize {
        self.batches.iter().map(Bytes::len).sum()
    }
}
