//! The three offsets that bound a log: start, high watermark and end.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetSnapshot {
    pub log_start: i64,
    pub high_watermark: i64,
    pub log_end: i64,
}

impl OffsetSnapshot {
    pub fn is_empty(&self) -> bool {
        self.log_start == self.log_end
    }
}
