//! Merge engines that decide how a new row combines with the existing row for the same key.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeEngine {
    FirstRow,
    Versioned { column: String },
    Aggregation,
}

impl fmt::Display for MergeEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MergeEngine::FirstRow => f.write_str("first_row"),
            MergeEngine::Versioned { column } => write!(f, "versioned({column})"),
            MergeEngine::Aggregation => f.write_str("aggregation"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_shape() {
        assert_eq!(
            serde_json::to_string(&MergeEngine::FirstRow).unwrap(),
            "\"first_row\""
        );
        let versioned = MergeEngine::Versioned {
            column: "ts".into(),
        };
        let json = serde_json::to_string(&versioned).unwrap();
        assert_eq!(json, r#"{"versioned":{"column":"ts"}}"#);
        assert_eq!(
            serde_json::from_str::<MergeEngine>(&json).unwrap(),
            versioned
        );
    }
}
