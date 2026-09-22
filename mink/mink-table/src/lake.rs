//! The lake table formats a table can be tiered to.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LakeFormat {
    Paimon,
    Iceberg,
    Lance,
}

impl fmt::Display for LakeFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LakeFormat::Paimon => "paimon",
            LakeFormat::Iceberg => "iceberg",
            LakeFormat::Lance => "lance",
        })
    }
}
