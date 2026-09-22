//! Body compression choices for Arrow log batches.

use std::fmt;

use arrow_ipc::CompressionType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Compression {
    None,
    Lz4Frame,
    #[default]
    Zstd,
}

impl Compression {
    pub(crate) fn ipc(self) -> Option<CompressionType> {
        match self {
            Compression::None => None,
            Compression::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            Compression::Zstd => Some(CompressionType::ZSTD),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Lz4Frame => "lz4_frame",
            Compression::Zstd => "zstd",
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
