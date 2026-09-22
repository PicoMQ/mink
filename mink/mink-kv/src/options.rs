//! Sizing knobs for the persistent engine with production defaults.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    pub memtable_bytes: usize,
    pub block_bytes: usize,
    pub block_cache_bytes: u64,
    pub write_batch_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_bytes: 64 << 20,
            block_bytes: 4 << 10,
            block_cache_bytes: 8 << 20,
            write_batch_bytes: 2 << 20,
        }
    }
}
