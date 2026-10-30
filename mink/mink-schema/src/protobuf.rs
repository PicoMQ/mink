use std::collections::HashMap;
use std::path::Path;

use iceberg::spec::PrimitiveType;
use prost_reflect::{
    DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, ReflectMessage, Value,
};
use protox::Compiler;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

use crate::Error;
use crate::limits::{Limits, SCHEMA_TOO_LARGE};
use crate::registry::SchemaSet;
use crate::types::{PayloadField, PayloadType};
use crate::value::PayloadValue;
use crate::varint::zigzag;

const MICROS_PER_SECOND: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1000;
const JSON_MESSAGES: [&str; 5] = [
    "google.protobuf.Any",
    "google.protobuf.Struct",
    "google.protobuf.Value",
    "google.protobuf.ListValue",
    "google.protobuf.Duration",
];

pub struct ProtobufSchema {
    message: MessageDescriptor,
    payload_type: PayloadType,
}

struct Sources {
    by_name: HashMap<String, String>,
}

impl FileResolver for Sources {
    fn resolve_path(&self, path: &Path) -> Option<String> {
        let name = path.to_str()?;

        self.by_name.contains_key(name).then(|| name.to_owned())
    }

    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.by_name.get(name) {
            Some(source) => File::from_source(name, source),
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

impl ProtobufSchema {
    pub(crate) fn parse(schema_set: &SchemaSet) -> Result<ProtobufSchema, Error> {
        let schema = schema_set.schema();
        let invalid = |message: String| Error::PayloadSchema {
            key: schema.key(),
            message,
        };
        let main = format!("registry/{}.proto", schema.key());

        let mut by_name: HashMap<String, String> = schema_set
            .definitions_by_name()
            .into_iter()
            .map(|(name, definition)| (name.to_owned(), definition.to_owned()))
            .collect();
        by_name.insert(main.clone(), schema.definition().to_owned());

        let mut resolver = ChainFileResolver::new();
        resolver.add(Sources { by_name });
        resolver.add(GoogleFileResolver::new());

        let mut compiler = Compiler::with_file_resolver(resolver);
        compiler
            .open_file(&main)
            .map_err(|error| invalid(error.to_string()))?;

        let pool = compiler.descriptor_pool();
        let file = pool
            .get_file_by_name(&main)
            .ok_or_else(|| invalid(format!("{main} did not compile")))?;
        let message = file
            .messages()
            .next()
            .ok_or_else(|| invalid("the schema defines no message".to_owned()))?;

        let payload_type = map_message(&message, &mut Vec::new(), &mut Limits::schema())
            .ok_or_else(|| invalid(SCHEMA_TOO_LARGE.to_owned()))?;
        Ok(ProtobufSchema {
            message,
            payload_type,
        })
    }

    pub(crate) fn payload_type(&self) -> &PayloadType {
        &self.payload_type
    }

    pub(crate) fn decode(&self, payload: &[u8]) -> Option<PayloadValue> {
        let mut bytes = payload;

        let indexes = message_indexes(&mut bytes)?;
        if indexes != [0] {
            return None;
        }

        let message = DynamicMessage::decode(self.message.clone(), bytes).ok()?;
        message_value(&message, &mut Vec::new())
    }
}

fn message_indexes(bytes: &mut &[u8]) -> Option<Vec<i64>> {
    let count = zigzag(bytes)?;

    if count == 0 {
        return Some(vec![0]);
    }

    let count = usize::try_from(count)
        .ok()
        .filter(|count| *count <= bytes.len())?;
    (0..count).map(|_| zigzag(bytes)).collect()
}

fn map_message(
    message: &MessageDescriptor,
    stack: &mut Vec<String>,
    limits: &mut Limits,
) -> Option<PayloadType> {
    if let Some(payload_type) = well_known(message.full_name()) {
        return Some(payload_type);
    }
    if stack.iter().any(|name| name == message.full_name()) {
        return Some(PayloadType::Json);
    }

    stack.push(message.full_name().to_owned());
    limits.descend()?;

    let mut fields = Vec::new();
    for field in sorted_fields(message) {
        limits.take(1)?;
        let number = field.number().to_string();
        let payload_type = map_field(&field, stack, limits)?;

        fields.push(PayloadField::new(
            field.name(),
            &number,
            Vec::new(),
            payload_type,
        ));
    }

    limits.ascend();
    stack.pop();

    Some(PayloadType::Struct(fields))
}

fn map_field(
    field: &FieldDescriptor,
    stack: &mut Vec<String>,
    limits: &mut Limits,
) -> Option<PayloadType> {
    if field.is_map() {
        let Kind::Message(entry) = field.kind() else {
            return None;
        };
        let key = map_kind(&entry.map_entry_key_field().kind(), stack, limits)?;
        let value = map_kind(&entry.map_entry_value_field().kind(), stack, limits)?;

        return Some(PayloadType::Map(Box::new(key), Box::new(value)));
    }

    let kind = map_kind(&field.kind(), stack, limits)?;
    match field.is_list() {
        true => Some(PayloadType::List(Box::new(kind))),
        false => Some(kind),
    }
}

fn map_kind(kind: &Kind, stack: &mut Vec<String>, limits: &mut Limits) -> Option<PayloadType> {
    let primitive = |primitive| Some(PayloadType::Primitive(primitive));

    match kind {
        Kind::Double => primitive(PrimitiveType::Double),
        Kind::Float => primitive(PrimitiveType::Float),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => primitive(PrimitiveType::Int),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint32 | Kind::Fixed32 => {
            primitive(PrimitiveType::Long)
        }
        Kind::Uint64 | Kind::Fixed64 => primitive(PrimitiveType::Decimal {
            precision: 20,
            scale: 0,
        }),
        Kind::Bool => primitive(PrimitiveType::Boolean),
        Kind::String | Kind::Enum(_) => primitive(PrimitiveType::String),
        Kind::Bytes => primitive(PrimitiveType::Binary),
        Kind::Message(message) => map_message(message, stack, limits),
    }
}

fn well_known(full_name: &str) -> Option<PayloadType> {
    let primitive = |primitive| Some(PayloadType::Primitive(primitive));

    match full_name {
        "google.protobuf.Timestamp" => primitive(PrimitiveType::Timestamptz),
        "google.protobuf.BoolValue" => primitive(PrimitiveType::Boolean),
        "google.protobuf.Int32Value" => primitive(PrimitiveType::Int),
        "google.protobuf.Int64Value" | "google.protobuf.UInt32Value" => {
            primitive(PrimitiveType::Long)
        }
        "google.protobuf.UInt64Value" => primitive(PrimitiveType::Decimal {
            precision: 20,
            scale: 0,
        }),
        "google.protobuf.FloatValue" => primitive(PrimitiveType::Float),
        "google.protobuf.DoubleValue" => primitive(PrimitiveType::Double),
        "google.protobuf.StringValue" => primitive(PrimitiveType::String),
        "google.protobuf.BytesValue" => primitive(PrimitiveType::Binary),
        name if JSON_MESSAGES.contains(&name) => Some(PayloadType::Json),
        _ => None,
    }
}

fn message_value(message: &DynamicMessage, stack: &mut Vec<String>) -> Option<PayloadValue> {
    let descriptor = message.descriptor();
    let full_name = descriptor.full_name();

    if well_known(full_name).is_some() {
        return well_known_value(message);
    }
    if stack.iter().any(|name| name == full_name) {
        return json(message);
    }

    stack.push(full_name.to_owned());

    let mut fields = Vec::new();
    for field in sorted_fields(&descriptor) {
        let value = match field.supports_presence() && !message.has_field(&field) {
            true => PayloadValue::Null,
            false => field_value(&field, &message.get_field(&field), stack)?,
        };

        fields.push((field.number().to_string(), value));
    }

    stack.pop();

    Some(PayloadValue::Struct(fields))
}

fn field_value(
    field: &FieldDescriptor,
    value: &Value,
    stack: &mut Vec<String>,
) -> Option<PayloadValue> {
    match (value, field.kind()) {
        (Value::List(items), kind) => {
            let items = items
                .iter()
                .map(|item| scalar_value(item, &kind, stack))
                .collect::<Option<_>>()?;

            Some(PayloadValue::List(items))
        }
        (Value::Map(entries), Kind::Message(entry)) => {
            let value_kind = entry.map_entry_value_field().kind();
            let mut sorted: Vec<(&MapKey, &Value)> = entries.iter().collect();
            sorted.sort_by_key(|(key, _)| *key);

            let entries = sorted
                .into_iter()
                .map(|(key, value)| Some((map_key(key), scalar_value(value, &value_kind, stack)?)))
                .collect::<Option<_>>()?;
            Some(PayloadValue::Map(entries))
        }
        (Value::Map(_), _) => None,
        (value, kind) => scalar_value(value, &kind, stack),
    }
}

fn scalar_value(value: &Value, kind: &Kind, stack: &mut Vec<String>) -> Option<PayloadValue> {
    let decoded = match (value, kind) {
        (Value::Bool(value), _) => PayloadValue::Boolean(*value),
        (Value::I32(value), _) => PayloadValue::Int(*value),
        (Value::I64(value), _) => PayloadValue::Long(*value),
        (Value::U32(value), _) => PayloadValue::Long(i64::from(*value)),
        (Value::U64(value), _) => PayloadValue::Decimal(i128::from(*value)),
        (Value::F32(value), _) => PayloadValue::Float(*value),
        (Value::F64(value), _) => PayloadValue::Double(*value),
        (Value::String(value), _) => PayloadValue::String(value.clone()),
        (Value::Bytes(value), _) => PayloadValue::Binary(value.to_vec()),
        (Value::EnumNumber(number), Kind::Enum(enumeration)) => {
            let name = enumeration
                .get_value(*number)
                .map_or_else(|| number.to_string(), |value| value.name().to_owned());

            PayloadValue::String(name)
        }
        (Value::EnumNumber(number), _) => PayloadValue::String(number.to_string()),
        (Value::Message(message), _) => return message_value(message, stack),
        (Value::List(_) | Value::Map(_), _) => return None,
    };

    Some(decoded)
}

fn well_known_value(message: &DynamicMessage) -> Option<PayloadValue> {
    let descriptor = message.descriptor();

    match descriptor.full_name() {
        "google.protobuf.Timestamp" => timestamp(message),
        name if JSON_MESSAGES.contains(&name) => json(message),
        _ => {
            let wrapped = descriptor.get_field(1)?;

            scalar_value(
                &message.get_field(&wrapped),
                &wrapped.kind(),
                &mut Vec::new(),
            )
        }
    }
}

fn timestamp(message: &DynamicMessage) -> Option<PayloadValue> {
    let descriptor = message.descriptor();
    let seconds = descriptor
        .get_field(1)
        .and_then(|field| message.get_field(&field).as_i64())?;
    let nanos = descriptor
        .get_field(2)
        .and_then(|field| message.get_field(&field).as_i32())?;

    let micros = seconds
        .checked_mul(MICROS_PER_SECOND)?
        .checked_add(i64::from(nanos) / NANOS_PER_MICRO)?;
    Some(PayloadValue::Timestamp(micros))
}

fn json(message: &DynamicMessage) -> Option<PayloadValue> {
    let value = serde_json::to_value(message).ok()?;

    Some(PayloadValue::json_text(&value))
}

fn sorted_fields(message: &MessageDescriptor) -> Vec<FieldDescriptor> {
    let mut fields: Vec<FieldDescriptor> = message.fields().collect();

    fields.sort_by_key(FieldDescriptor::number);
    fields
}

fn map_key(key: &MapKey) -> PayloadValue {
    match key {
        MapKey::Bool(value) => PayloadValue::Boolean(*value),
        MapKey::I32(value) => PayloadValue::Int(*value),
        MapKey::I64(value) => PayloadValue::Long(*value),
        MapKey::U32(value) => PayloadValue::Long(i64::from(*value)),
        MapKey::U64(value) => PayloadValue::Decimal(i128::from(*value)),
        MapKey::String(value) => PayloadValue::String(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::message_indexes;

    #[test]
    fn message_indexes_name_the_message() {
        let mut first: &[u8] = &[0, 8];
        let mut nested: &[u8] = &[4, 2, 6, 8];
        let mut truncated: &[u8] = &[6, 2];

        assert_eq!(message_indexes(&mut first), Some(vec![0]));
        assert_eq!(first, [8]);
        assert_eq!(message_indexes(&mut nested), Some(vec![1, 3]));
        assert_eq!(nested, [8]);
        assert_eq!(message_indexes(&mut truncated), None);
    }
}
