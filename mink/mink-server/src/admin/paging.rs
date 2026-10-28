use axum::Json;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::reply::Failure;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_PAGE_BYTES: usize = 1024 * 1024;
const ENVELOPE_BYTES: usize = 1024;

#[derive(Deserialize)]
pub(super) struct PageQuery {
    limit: Option<String>,
    cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    after: Value,
    filters: String,
}

pub(super) struct Paging {
    limit: usize,
    after: Option<Value>,
    filters: String,
}

impl Paging {
    pub(super) fn new(query: &PageQuery, filters: &[(&str, &str)]) -> Result<Paging, Failure> {
        let limit = match query.limit.as_deref().filter(|limit| !limit.is_empty()) {
            Some(limit) => limit
                .parse()
                .map_err(|_| Failure::bad(format!("limit {limit:?} is not a number")))?,
            None => DEFAULT_LIMIT,
        };
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Err(Failure::bad(format!("limit must be 1 to {MAX_LIMIT}")));
        }

        let filters = filters
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let after = match query.cursor.as_deref().filter(|cursor| !cursor.is_empty()) {
            Some(cursor) => {
                let cursor =
                    decode(cursor).ok_or_else(|| Failure::bad("the cursor is not valid"))?;
                if cursor.filters != filters {
                    return Err(Failure::bad("the cursor belongs to a different query"));
                }

                Some(cursor.after)
            }
            None => None,
        };

        Ok(Paging {
            limit,
            after,
            filters,
        })
    }

    pub(super) fn wanted(&self) -> usize {
        self.limit + 1
    }

    pub(super) fn after<K: DeserializeOwned>(&self) -> Result<Option<K>, Failure> {
        self.after
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| Failure::bad("the cursor is not valid"))
    }

    pub(super) fn split<T>(&self, mut rows: Vec<T>) -> (Vec<T>, bool) {
        let more = rows.len() > self.limit;

        rows.truncate(self.limit);
        (rows, more)
    }

    pub(super) fn page<K: Serialize, T: Serialize>(
        &self,
        items: Vec<(K, T)>,
        more: bool,
    ) -> Json<Value> {
        let mut shown = Vec::with_capacity(items.len());
        let mut bytes = ENVELOPE_BYTES;
        let mut last = None;
        let mut cut_short = false;

        for (key, item) in &items {
            let value = serde_json::to_value(item).expect("admin items serialize");
            let size = serde_json::to_vec(&value).map_or(0, |text| text.len()) + 1;
            if !shown.is_empty() && bytes + size > MAX_PAGE_BYTES {
                cut_short = true;
                break;
            }

            bytes += size;
            shown.push(value);
            last = Some(key);
        }

        let next = match (more || cut_short, last) {
            (true, Some(key)) => Some(encode(&Cursor {
                after: serde_json::to_value(key).expect("page keys serialize"),
                filters: self.filters.clone(),
            })),
            _ => None,
        };
        Json(json!({ "items": shown, "next": next }))
    }
}

fn encode(cursor: &Cursor) -> String {
    hex::encode(serde_json::to_vec(cursor).expect("a cursor serializes"))
}

fn decode(text: &str) -> Option<Cursor> {
    let bytes = hex::decode(text).ok()?;

    serde_json::from_slice(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(limit: Option<&str>, cursor: Option<String>) -> PageQuery {
        PageQuery {
            limit: limit.map(str::to_owned),
            cursor,
        }
    }

    fn keyed(items: &[&str]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|item| (item.to_string(), item.to_string()))
            .collect()
    }

    #[test]
    fn lists_page_by_cursor_and_refuse_a_cursor_of_other_filters() {
        let filters = [("prefix", "t")];
        let first = Paging::new(&query(Some("2"), None), &filters).unwrap();

        let (rows, more) = first.split(keyed(&["t0", "t1", "t2"]));
        let page = first.page(rows, more).0;
        let cursor = page["next"].as_str().unwrap().to_owned();
        let second = Paging::new(&query(Some("2"), Some(cursor.clone())), &filters).unwrap();
        let other = Paging::new(&query(Some("2"), Some(cursor)), &[("prefix", "u")]);
        let last = Paging::new(&query(None, None), &[]).unwrap();
        let (rows, more) = last.split(keyed(&["t0", "t1"]));

        assert_eq!(page["items"], json!(["t0", "t1"]));
        assert_eq!(second.after::<String>().unwrap(), Some("t1".to_owned()));
        assert!(other.is_err());
        assert_eq!(last.page(rows, more).0["next"], Value::Null);
        assert!(Paging::new(&query(Some("1001"), None), &[]).is_err());
        assert!(Paging::new(&query(Some("abc"), None), &[]).is_err());
        assert!(Paging::new(&query(Some(""), None), &[]).is_ok());
    }

    #[test]
    fn a_page_stops_at_1_mib_but_holds_one_item() {
        let big = "x".repeat(700 * 1024);
        let items: Vec<(usize, String)> = (0..3).map(|index| (index, big.clone())).collect();
        let paging = Paging::new(&query(None, None), &[]).unwrap();

        let page = paging.page(items, false).0;

        assert_eq!(page["items"].as_array().unwrap().len(), 1);
        assert!(page["next"].is_string());
        assert!(serde_json::to_vec(&page).unwrap().len() <= MAX_PAGE_BYTES);
    }
}
