use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, NaiveDate};
use iceberg::spec::PrimitiveType;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;

use crate::Error;
use crate::limits::{Limits, SCHEMA_TOO_LARGE};
use crate::registry::SchemaSet;
use crate::types::{PayloadField, PayloadType};
use crate::value::PayloadValue;

const COMBINATORS: [&str; 5] = ["oneOf", "anyOf", "allOf", "not", "if"];

pub struct JsonSchema {
    root: JsonType,
    payload_type: PayloadType,
}

#[derive(Debug, Clone, PartialEq)]
enum JsonType {
    Boolean,
    Long,
    Double,
    String,
    Timestamp,
    Date,
    Uuid,
    Object(Vec<(String, JsonType)>),
    Map(Box<JsonType>),
    Array(Box<JsonType>),
    Json,
}

#[derive(Debug, Clone)]
enum SchemaNode {
    Null,
    Scalar,
    String(String),
    Array(Vec<SchemaNode>),
    Object(Vec<(String, SchemaNode)>),
}

struct SchemaNodeVisitor;

struct Builder<'a> {
    documents: &'a HashMap<String, SchemaNode>,
    stack: Vec<String>,
    limits: Limits,
}

impl JsonSchema {
    pub(crate) fn parse(schema_set: &SchemaSet) -> Result<JsonSchema, Error> {
        let schema = schema_set.schema();
        let invalid = |message: String| Error::PayloadSchema {
            id: schema.id(),
            message,
        };
        let unparsable = |error: serde_json::Error| invalid(error.to_string());

        let mut documents = HashMap::from([(
            String::new(),
            parse(schema.definition()).map_err(unparsable)?,
        )]);
        for (name, definition) in schema_set.definitions_by_name() {
            documents.insert(name.to_owned(), parse(definition).map_err(unparsable)?);
        }

        let mut builder = Builder {
            documents: &documents,
            stack: Vec::new(),
            limits: Limits::schema(),
        };
        let root = builder
            .build(&documents[""], "")
            .ok_or_else(|| invalid(SCHEMA_TOO_LARGE.to_owned()))?;

        let payload_type = payload_type(&root);
        Ok(JsonSchema { root, payload_type })
    }

    pub(crate) fn payload_type(&self) -> &PayloadType {
        &self.payload_type
    }

    pub(crate) fn decode(&self, payload: &[u8]) -> Option<PayloadValue> {
        let value: Value = serde_json::from_slice(payload).ok()?;

        decode(&value, &self.root)
    }
}

impl Builder<'_> {
    fn build(&mut self, node: &SchemaNode, document: &str) -> Option<JsonType> {
        if let Some(target) = node.get("$ref").and_then(SchemaNode::as_str) {
            return self.reference(target, document);
        }
        if COMBINATORS
            .iter()
            .any(|combinator| node.get(combinator).is_some())
        {
            return Some(JsonType::Json);
        }

        let built = match single_type(node) {
            Some("object") => self.object(node, document)?,
            Some("array") => match node.get("items") {
                Some(items @ SchemaNode::Object(_)) => {
                    JsonType::Array(Box::new(self.nested(items, document)?))
                }
                _ => JsonType::Json,
            },
            Some("string") => match node.get("format").and_then(SchemaNode::as_str) {
                Some("date-time") => JsonType::Timestamp,
                Some("date") => JsonType::Date,
                Some("uuid") => JsonType::Uuid,
                _ => JsonType::String,
            },
            Some("integer") => JsonType::Long,
            Some("number") => JsonType::Double,
            Some("boolean") => JsonType::Boolean,
            _ => JsonType::Json,
        };

        Some(built)
    }

    fn object(&mut self, node: &SchemaNode, document: &str) -> Option<JsonType> {
        if let Some(SchemaNode::Object(properties)) = node.get("properties") {
            let mut fields = Vec::with_capacity(properties.len());

            for (name, property) in properties {
                self.limits.take(1)?;

                fields.push((name.clone(), self.nested(property, document)?));
            }

            return Some(JsonType::Object(fields));
        }

        match node.get("additionalProperties") {
            Some(values @ SchemaNode::Object(_)) => {
                Some(JsonType::Map(Box::new(self.nested(values, document)?)))
            }
            _ => Some(JsonType::Json),
        }
    }

    fn nested(&mut self, node: &SchemaNode, document: &str) -> Option<JsonType> {
        self.limits.descend()?;

        let built = self.build(node, document)?;

        self.limits.ascend();
        Some(built)
    }

    fn reference(&mut self, target: &str, document: &str) -> Option<JsonType> {
        let (name, pointer) = match target.split_once('#') {
            Some(("", pointer)) => (document, pointer),
            Some((name, pointer)) => (name, pointer),
            None => (target, ""),
        };
        let key = format!("{name}#{pointer}");

        if self.stack.contains(&key) {
            return Some(JsonType::Json);
        }

        let Some(resolved) = self
            .documents
            .get(name)
            .and_then(|root| root.pointer(pointer))
        else {
            return Some(JsonType::Json);
        };

        self.stack.push(key);
        let built = self.build(resolved, name)?;
        self.stack.pop();

        Some(built)
    }
}

fn single_type(node: &SchemaNode) -> Option<&str> {
    match node.get("type") {
        Some(SchemaNode::String(name)) => Some(name),
        Some(SchemaNode::Array(names)) => {
            let mut kept = names
                .iter()
                .filter_map(SchemaNode::as_str)
                .filter(|name| *name != "null");
            let name = kept.next()?;

            kept.next().is_none().then_some(name)
        }
        Some(_) => None,
        None if node.get("properties").is_some() => Some("object"),
        None => node
            .get("enum")
            .and_then(SchemaNode::as_array)
            .filter(|values| values.iter().all(|value| value.as_str().is_some()))
            .map(|_| "string"),
    }
}

fn payload_type(json_type: &JsonType) -> PayloadType {
    let primitive = PayloadType::Primitive;

    match json_type {
        JsonType::Boolean => primitive(PrimitiveType::Boolean),
        JsonType::Long => primitive(PrimitiveType::Long),
        JsonType::Double => primitive(PrimitiveType::Double),
        JsonType::String => primitive(PrimitiveType::String),
        JsonType::Timestamp => primitive(PrimitiveType::Timestamptz),
        JsonType::Date => primitive(PrimitiveType::Date),
        JsonType::Uuid => primitive(PrimitiveType::Uuid),
        JsonType::Object(fields) => PayloadType::Struct(
            fields
                .iter()
                .map(|(name, field)| PayloadField::new(name, name, Vec::new(), payload_type(field)))
                .collect(),
        ),
        JsonType::Map(values) => PayloadType::Map(
            Box::new(primitive(PrimitiveType::String)),
            Box::new(payload_type(values)),
        ),
        JsonType::Array(items) => PayloadType::List(Box::new(payload_type(items))),
        JsonType::Json => PayloadType::Json,
    }
}

fn decode(value: &Value, json_type: &JsonType) -> Option<PayloadValue> {
    if value.is_null() {
        return Some(PayloadValue::Null);
    }

    match (json_type, value) {
        (JsonType::Json, value) => Some(PayloadValue::json_text(value)),
        (JsonType::Boolean, Value::Bool(value)) => Some(PayloadValue::Boolean(*value)),
        (JsonType::Long, Value::Number(number)) => number.as_i64().map(PayloadValue::Long),
        (JsonType::Double, Value::Number(number)) => number.as_f64().map(PayloadValue::Double),
        (JsonType::String, Value::String(text)) => Some(PayloadValue::String(text.clone())),
        (JsonType::Timestamp, Value::String(text)) => {
            let time = DateTime::parse_from_rfc3339(text).ok()?;

            Some(PayloadValue::Timestamp(time.timestamp_micros()))
        }
        (JsonType::Date, Value::String(text)) => {
            let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;

            i32::try_from((date - epoch).num_days())
                .ok()
                .map(PayloadValue::Date)
        }
        (JsonType::Uuid, Value::String(text)) => {
            let uuid = uuid::Uuid::parse_str(text).ok()?;

            Some(PayloadValue::Uuid(*uuid.as_bytes()))
        }
        (JsonType::Object(fields), Value::Object(object)) => {
            let mut decoded = Vec::with_capacity(fields.len());

            for (name, field) in fields {
                let value = match object.get(name) {
                    Some(value) => decode(value, field)?,
                    None => PayloadValue::Null,
                };
                decoded.push((name.clone(), value));
            }

            Some(PayloadValue::Struct(decoded))
        }
        (JsonType::Map(values), Value::Object(object)) => {
            let mut sorted: Vec<(&String, &Value)> = object.iter().collect();
            sorted.sort_by_key(|(key, _)| *key);
            let mut entries = Vec::with_capacity(sorted.len());

            for (key, value) in sorted {
                entries.push((PayloadValue::String(key.clone()), decode(value, values)?));
            }

            Some(PayloadValue::Map(entries))
        }
        (JsonType::Array(items), Value::Array(values)) => {
            let decoded = values
                .iter()
                .map(|value| decode(value, items))
                .collect::<Option<_>>()?;

            Some(PayloadValue::List(decoded))
        }
        _ => None,
    }
}

fn parse(text: &str) -> Result<SchemaNode, serde_json::Error> {
    serde_json::from_str(text)
}

impl SchemaNode {
    fn get(&self, key: &str) -> Option<&SchemaNode> {
        match self {
            SchemaNode::Object(members) => members
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            SchemaNode::String(text) => Some(text),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&[SchemaNode]> {
        match self {
            SchemaNode::Array(items) => Some(items),
            _ => None,
        }
    }

    fn pointer(&self, pointer: &str) -> Option<&SchemaNode> {
        pointer
            .split('/')
            .skip(1)
            .map(|segment| segment.replace("~1", "/").replace("~0", "~"))
            .try_fold(self, |node, segment| node.get(&segment))
    }
}

impl<'de> Deserialize<'de> for SchemaNode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<SchemaNode, D::Error> {
        deserializer.deserialize_any(SchemaNodeVisitor)
    }
}

impl<'de> Visitor<'de> for SchemaNodeVisitor {
    type Value = SchemaNode;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<SchemaNode, E> {
        Ok(SchemaNode::Null)
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<SchemaNode, E> {
        Ok(SchemaNode::Scalar)
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<SchemaNode, E> {
        Ok(SchemaNode::Scalar)
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<SchemaNode, E> {
        Ok(SchemaNode::Scalar)
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<SchemaNode, E> {
        Ok(SchemaNode::Scalar)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<SchemaNode, E> {
        Ok(SchemaNode::String(value.to_owned()))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<SchemaNode, A::Error> {
        let mut items = Vec::new();

        while let Some(item) = sequence.next_element()? {
            items.push(item);
        }

        Ok(SchemaNode::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<SchemaNode, A::Error> {
        let mut members = Vec::new();

        while let Some(member) = map.next_entry()? {
            members.push(member);
        }

        Ok(SchemaNode::Object(members))
    }
}
