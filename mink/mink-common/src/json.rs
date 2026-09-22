//! Flattens nested JSON objects into dotted keys and decodes string-valued enums from JSON.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde_json::{Error, Value};

pub fn flatten(value: &Value, prefix: String, out: &mut BTreeMap<String, String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten(value, key, out);
            }
        }
        Value::Null => {}
        Value::String(text) => {
            out.insert(prefix, text.clone());
        }
        other => {
            out.insert(prefix, other.to_string());
        }
    }
}

pub fn enum_value<T: DeserializeOwned>(text: &str) -> Result<T, Error> {
    serde_json::from_value(Value::String(text.to_owned()))
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "lowercase")]
    enum Kind {
        Iceberg,
    }

    #[test]
    fn flatten_nests_and_skips_null() {
        let value = serde_json::json!({"a": {"b": 1}, "c": null, "d": "x"});
        let mut out = BTreeMap::new();
        flatten(&value, String::new(), &mut out);
        assert_eq!(out.get("a.b").map(String::as_str), Some("1"));
        assert_eq!(out.get("d").map(String::as_str), Some("x"));
        assert!(!out.contains_key("c"));
    }

    #[test]
    fn enum_value_reads_a_string_variant() {
        assert_eq!(enum_value::<Kind>("iceberg").unwrap(), Kind::Iceberg);
    }
}
