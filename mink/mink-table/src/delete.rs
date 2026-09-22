//! How deletes are treated on a primary-key table.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteBehavior {
    Allow,
    Ignore,
    Disable,
}
