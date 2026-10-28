use serde_json::{Value, json};
use uuid::Uuid;

pub async fn register_schema(registry: &str, subject: &str, body: &Value) -> i32 {
    let client = reqwest::Client::new();
    let config = json!({ "compatibility": "NONE" });

    client
        .put(format!("{registry}/config/{subject}"))
        .json(&config)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let registered: Value = client
        .post(format!("{registry}/subjects/{subject}/versions"))
        .json(body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    i32::try_from(registered["id"].as_i64().unwrap()).unwrap()
}

pub async fn schema_guid(registry: &str, id: i32) -> Uuid {
    let schema: Value = reqwest::get(format!("{registry}/schemas/ids/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    Uuid::parse_str(schema["guid"].as_str().unwrap()).unwrap()
}

pub fn prefixed(id: i32, payload: &[u8]) -> Vec<u8> {
    [0].into_iter()
        .chain(id.to_be_bytes())
        .chain(payload.iter().copied())
        .collect()
}
