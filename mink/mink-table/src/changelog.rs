//! Whether the changelog carries full before-and-after images or only what was written.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangelogImage {
    #[default]
    Full,
    Wal,
}
