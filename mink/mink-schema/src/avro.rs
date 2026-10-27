use std::collections::HashMap;

use apache_avro::schema::{
    DecimalSchema, FixedSchema, InnerDecimalSchema, Name, Schema, UnionSchema, UuidSchema,
};
use iceberg::spec::PrimitiveType;

use crate::Error;
use crate::limits::{Limits, SCHEMA_TOO_LARGE};
use crate::registry::SchemaSet;
use crate::types::{PayloadField, PayloadType};
use crate::value::PayloadValue;
use crate::varint::zigzag;

const MAX_PRECISION: usize = 38;

pub struct AvroSchema {
    root: Schema,
    names: HashMap<Name, Schema>,
    payload_type: PayloadType,
}

struct Reader<'a> {
    bytes: &'a [u8],
    limits: Limits,
}

#[derive(Debug, Clone, Copy)]
enum Encoding {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
}

impl AvroSchema {
    pub(crate) fn parse(schema_set: &SchemaSet) -> Result<AvroSchema, Error> {
        let schema = schema_set.schema();
        let invalid = |message: String| Error::PayloadSchema {
            id: schema.id(),
            message,
        };

        let referenced: Vec<&str> = schema_set
            .referenced()
            .iter()
            .map(|referenced| referenced.definition())
            .collect();
        let (root, referenced) = Schema::parse_str_with_list(schema.definition(), &referenced)
            .map_err(|error| invalid(error.to_string()))?;

        let mut names = HashMap::new();
        for schema in referenced.iter().chain([&root]) {
            collect(schema, &mut names);
        }

        let mut avro = AvroSchema {
            root,
            names,
            payload_type: PayloadType::Json,
        };
        avro.payload_type = avro
            .map(&avro.root, &mut Vec::new(), &mut Limits::schema())
            .ok_or_else(|| invalid(SCHEMA_TOO_LARGE.to_owned()))?;

        Ok(avro)
    }

    pub(crate) fn payload_type(&self) -> &PayloadType {
        &self.payload_type
    }

    pub(crate) fn decode(&self, payload: &[u8]) -> Option<PayloadValue> {
        let mut reader = Reader {
            bytes: payload,
            limits: Limits::payload(payload.len()),
        };

        let decoded = self.typed(&self.root, &mut Vec::new(), &mut reader)?;

        reader.bytes.is_empty().then_some(decoded)
    }

    fn resolve<'s>(&'s self, schema: &'s Schema) -> Option<&'s Schema> {
        match schema {
            Schema::Ref { name } => self.names.get(name),
            schema => Some(schema),
        }
    }

    fn map(
        &self,
        schema: &Schema,
        stack: &mut Vec<Name>,
        limits: &mut Limits,
    ) -> Option<PayloadType> {
        let primitive = |primitive| Some(PayloadType::Primitive(primitive));

        match self.resolve(schema)? {
            Schema::Null | Schema::BigDecimal => Some(PayloadType::Json),
            Schema::Boolean => primitive(PrimitiveType::Boolean),
            Schema::Int => primitive(PrimitiveType::Int),
            Schema::Long | Schema::TimestampNanos | Schema::LocalTimestampNanos => {
                primitive(PrimitiveType::Long)
            }
            Schema::Float => primitive(PrimitiveType::Float),
            Schema::Double => primitive(PrimitiveType::Double),
            Schema::Bytes => primitive(PrimitiveType::Binary),
            Schema::String | Schema::Enum(_) => primitive(PrimitiveType::String),
            Schema::Fixed(fixed) => primitive(PrimitiveType::Fixed(fixed.size as u64)),
            Schema::Duration(_) => primitive(PrimitiveType::Fixed(12)),
            Schema::Uuid(_) => primitive(PrimitiveType::Uuid),
            Schema::Date => primitive(PrimitiveType::Date),
            Schema::TimeMillis | Schema::TimeMicros => primitive(PrimitiveType::Time),
            Schema::TimestampMillis | Schema::TimestampMicros => {
                primitive(PrimitiveType::Timestamptz)
            }
            Schema::LocalTimestampMillis | Schema::LocalTimestampMicros => {
                primitive(PrimitiveType::Timestamp)
            }
            Schema::Decimal(decimal) if decimal.precision <= MAX_PRECISION => {
                primitive(PrimitiveType::Decimal {
                    precision: decimal.precision as u32,
                    scale: decimal.scale as u32,
                })
            }
            Schema::Decimal(_) => Some(PayloadType::Json),
            Schema::Array(array) => {
                let items = self.nested(&array.items, stack, limits)?;

                Some(PayloadType::List(Box::new(items)))
            }
            Schema::Map(map) => {
                let values = self.nested(&map.types, stack, limits)?;

                Some(PayloadType::Map(
                    Box::new(PayloadType::Primitive(PrimitiveType::String)),
                    Box::new(values),
                ))
            }
            Schema::Union(union) => match self.single_branch(union) {
                Some(branch) => self.map(branch, stack, limits),
                None => Some(PayloadType::Json),
            },
            Schema::Record(record) if stack.contains(&record.name) => Some(PayloadType::Json),
            Schema::Record(record) => {
                stack.push(record.name.clone());

                let mut fields = Vec::with_capacity(record.fields.len());
                for field in &record.fields {
                    limits.take(1)?;
                    let payload_type = self.nested(&field.schema, stack, limits)?;

                    fields.push(PayloadField::new(
                        &field.name,
                        &field.name,
                        field.aliases.clone(),
                        payload_type,
                    ));
                }

                stack.pop();

                Some(PayloadType::Struct(fields))
            }
            Schema::Ref { .. } => None,
        }
    }

    fn nested(
        &self,
        schema: &Schema,
        stack: &mut Vec<Name>,
        limits: &mut Limits,
    ) -> Option<PayloadType> {
        limits.descend()?;

        let payload_type = self.map(schema, stack, limits)?;

        limits.ascend();
        Some(payload_type)
    }

    fn single_branch<'s>(&'s self, union: &'s UnionSchema) -> Option<&'s Schema> {
        let mut branches = union
            .variants()
            .iter()
            .filter(|variant| !matches!(self.resolve(variant), Some(Schema::Null)));

        let branch = branches.next()?;

        branches.next().is_none().then_some(branch)
    }

    fn typed(
        &self,
        schema: &Schema,
        stack: &mut Vec<Name>,
        reader: &mut Reader,
    ) -> Option<PayloadValue> {
        match self.resolve(schema)? {
            Schema::Null => Some(PayloadValue::Null),
            Schema::Boolean => reader.boolean().map(PayloadValue::Boolean),
            Schema::Int => reader.int().map(PayloadValue::Int),
            Schema::Long | Schema::TimestampNanos | Schema::LocalTimestampNanos => {
                reader.long().map(PayloadValue::Long)
            }
            Schema::Float => reader.float().map(PayloadValue::Float),
            Schema::Double => reader.double().map(PayloadValue::Double),
            Schema::Bytes => reader
                .bytes()
                .map(|bytes| PayloadValue::Binary(bytes.to_vec())),
            Schema::String => reader.string().map(PayloadValue::String),
            Schema::Enum(enumeration) => {
                let index = usize::try_from(reader.int()?).ok()?;

                enumeration
                    .symbols
                    .get(index)
                    .map(|symbol| PayloadValue::String(symbol.clone()))
            }
            Schema::Fixed(fixed) => reader
                .take(fixed.size)
                .map(|bytes| PayloadValue::Binary(bytes.to_vec())),
            Schema::Duration(_) => reader
                .take(12)
                .map(|bytes| PayloadValue::Binary(bytes.to_vec())),
            Schema::Uuid(UuidSchema::String) => {
                let text = reader.string()?;

                uuid::Uuid::parse_str(&text)
                    .ok()
                    .map(|uuid| PayloadValue::Uuid(*uuid.as_bytes()))
            }
            Schema::Uuid(UuidSchema::Fixed(_)) => {
                let bytes = reader.take(16)?;

                bytes.try_into().ok().map(PayloadValue::Uuid)
            }
            Schema::Uuid(UuidSchema::Bytes) => {
                let bytes = reader.bytes()?;

                bytes.try_into().ok().map(PayloadValue::Uuid)
            }
            Schema::Date => reader.int().map(PayloadValue::Date),
            Schema::TimeMillis => reader
                .int()
                .map(|millis| PayloadValue::Time(i64::from(millis) * 1000)),
            Schema::TimeMicros => reader.long().map(PayloadValue::Time),
            Schema::TimestampMillis | Schema::LocalTimestampMillis => reader
                .long()?
                .checked_mul(1000)
                .map(PayloadValue::Timestamp),
            Schema::TimestampMicros | Schema::LocalTimestampMicros => {
                reader.long().map(PayloadValue::Timestamp)
            }
            Schema::Decimal(decimal) if decimal.precision <= MAX_PRECISION => {
                let bytes = match &decimal.inner {
                    InnerDecimalSchema::Bytes => reader.bytes()?,
                    InnerDecimalSchema::Fixed(fixed) => reader.take(fixed.size)?,
                };

                unscaled(bytes).map(PayloadValue::Decimal)
            }
            Schema::Array(array) => {
                let mut items = Vec::new();

                reader.blocks(|reader| {
                    items.push(self.typed(&array.items, stack, reader)?);

                    Some(())
                })?;

                Some(PayloadValue::List(items))
            }
            Schema::Map(map) => {
                let mut entries = Vec::new();

                reader.blocks(|reader| {
                    let key = PayloadValue::String(reader.string()?);
                    let value = self.typed(&map.types, stack, reader)?;

                    entries.push((key, value));
                    Some(())
                })?;

                Some(PayloadValue::Map(entries))
            }
            Schema::Union(union) => {
                let index = usize::try_from(reader.long()?).ok()?;
                let branch = union.variants().get(index)?;

                match (self.resolve(branch)?, self.single_branch(union)) {
                    (Schema::Null, _) => Some(PayloadValue::Null),
                    (_, Some(_)) => self.typed(branch, stack, reader),
                    (_, None) => {
                        let mut json = String::new();

                        self.json_branch(branch, reader, &mut json)?;

                        Some(PayloadValue::String(json))
                    }
                }
            }
            Schema::Record(record) if stack.contains(&record.name) => {
                self.json_text(schema, reader).map(PayloadValue::String)
            }
            Schema::Record(record) => {
                stack.push(record.name.clone());

                let mut fields = Vec::with_capacity(record.fields.len());
                for field in &record.fields {
                    let value = self.typed(&field.schema, stack, reader)?;

                    fields.push((field.name.clone(), value));
                }

                stack.pop();

                Some(PayloadValue::Struct(fields))
            }
            Schema::Decimal(_) | Schema::BigDecimal => {
                self.json_text(schema, reader).map(PayloadValue::String)
            }
            Schema::Ref { .. } => None,
        }
    }

    fn json(&self, schema: &Schema, reader: &mut Reader, out: &mut String) -> Option<()> {
        let resolved = self.resolve(schema)?;

        if let Some(encoding) = Encoding::of(resolved) {
            let text = match encoding {
                Encoding::Null => "null".to_owned(),
                Encoding::Boolean => reader.boolean()?.to_string(),
                Encoding::Int => reader.int()?.to_string(),
                Encoding::Long => reader.long()?.to_string(),
                Encoding::Float => number(f64::from(reader.float()?)),
                Encoding::Double => number(reader.double()?),
                Encoding::Bytes => latin1(reader.bytes()?),
                Encoding::String => quoted(&reader.string()?),
            };

            out.push_str(&text);
            return Some(());
        }

        reader.limits.descend()?;

        match resolved {
            Schema::Fixed(FixedSchema { size, .. })
            | Schema::Duration(FixedSchema { size, .. })
            | Schema::Uuid(UuidSchema::Fixed(FixedSchema { size, .. }))
            | Schema::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Fixed(FixedSchema { size, .. }),
                ..
            }) => out.push_str(&latin1(reader.take(*size)?)),
            Schema::Enum(enumeration) => {
                let index = usize::try_from(reader.int()?).ok()?;

                out.push_str(&quoted(enumeration.symbols.get(index)?));
            }
            Schema::Array(array) => {
                let mut first = true;
                out.push('[');

                reader.blocks(|reader| {
                    if !std::mem::take(&mut first) {
                        out.push(',');
                    }

                    self.json(&array.items, reader, out)
                })?;

                out.push(']');
            }
            Schema::Map(map) => {
                let mut first = true;
                out.push('{');

                reader.blocks(|reader| {
                    if !std::mem::take(&mut first) {
                        out.push(',');
                    }

                    out.push_str(&quoted(&reader.string()?));
                    out.push(':');
                    self.json(&map.types, reader, out)
                })?;

                out.push('}');
            }
            Schema::Union(union) => {
                let index = usize::try_from(reader.long()?).ok()?;
                let branch = union.variants().get(index)?;

                match self.resolve(branch)? {
                    Schema::Null => out.push_str("null"),
                    _ => self.json_branch(branch, reader, out)?,
                }
            }
            Schema::Record(record) => {
                out.push('{');

                for (index, field) in record.fields.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }

                    out.push_str(&quoted(&field.name));
                    out.push(':');
                    self.json(&field.schema, reader, out)?;
                }

                out.push('}');
            }
            _ => return None,
        }

        reader.limits.ascend();
        Some(())
    }

    fn json_text(&self, schema: &Schema, reader: &mut Reader) -> Option<String> {
        let mut json = String::new();

        self.json(schema, reader, &mut json)?;

        Some(json)
    }

    fn json_branch(&self, branch: &Schema, reader: &mut Reader, out: &mut String) -> Option<()> {
        let name = branch_name(self.resolve(branch)?)?;
        out.push_str(&format!("{{{}:", quoted(&name)));

        self.json(branch, reader, out)?;

        out.push('}');
        Some(())
    }
}

impl Encoding {
    fn of(schema: &Schema) -> Option<Encoding> {
        match schema {
            Schema::Null => Some(Encoding::Null),
            Schema::Boolean => Some(Encoding::Boolean),
            Schema::Int | Schema::Date | Schema::TimeMillis => Some(Encoding::Int),
            Schema::Long
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => Some(Encoding::Long),
            Schema::Float => Some(Encoding::Float),
            Schema::Double => Some(Encoding::Double),
            Schema::Bytes
            | Schema::BigDecimal
            | Schema::Uuid(UuidSchema::Bytes)
            | Schema::Decimal(DecimalSchema {
                inner: InnerDecimalSchema::Bytes,
                ..
            }) => Some(Encoding::Bytes),
            Schema::String | Schema::Uuid(UuidSchema::String) => Some(Encoding::String),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Encoding::Null => "null",
            Encoding::Boolean => "boolean",
            Encoding::Int => "int",
            Encoding::Long => "long",
            Encoding::Float => "float",
            Encoding::Double => "double",
            Encoding::Bytes => "bytes",
            Encoding::String => "string",
        }
    }
}

impl Reader<'_> {
    fn take(&mut self, length: usize) -> Option<&[u8]> {
        if length > self.bytes.len() {
            return None;
        }

        let (taken, rest) = self.bytes.split_at(length);
        self.bytes = rest;

        Some(taken)
    }

    fn long(&mut self) -> Option<i64> {
        zigzag(&mut self.bytes)
    }

    fn int(&mut self) -> Option<i32> {
        i32::try_from(self.long()?).ok()
    }

    fn boolean(&mut self) -> Option<bool> {
        match self.take(1)?.first()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    fn float(&mut self) -> Option<f32> {
        Some(f32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn double(&mut self) -> Option<f64> {
        Some(f64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn bytes(&mut self) -> Option<&[u8]> {
        let length = usize::try_from(self.long()?).ok()?;

        self.take(length)
    }

    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.bytes()?.to_vec()).ok()
    }

    fn blocks(&mut self, mut item: impl FnMut(&mut Self) -> Option<()>) -> Option<()> {
        loop {
            let mut count = self.long()?;
            if count == 0 {
                return Some(());
            }

            if count < 0 {
                count = count.checked_neg()?;
                self.long()?;
            }

            let count = usize::try_from(count).ok()?;
            self.limits.take(count)?;
            for _ in 0..count {
                item(self)?;
            }
        }
    }
}

fn collect(schema: &Schema, names: &mut HashMap<Name, Schema>) {
    match schema {
        Schema::Record(record) => {
            if names.insert(record.name.clone(), schema.clone()).is_none() {
                for field in &record.fields {
                    collect(&field.schema, names);
                }
            }
        }
        Schema::Enum(enumeration) => {
            names.insert(enumeration.name.clone(), schema.clone());
        }
        Schema::Fixed(fixed) | Schema::Duration(fixed) | Schema::Uuid(UuidSchema::Fixed(fixed)) => {
            names.insert(fixed.name.clone(), schema.clone());
        }
        Schema::Decimal(decimal) => {
            if let InnerDecimalSchema::Fixed(fixed) = &decimal.inner {
                names.insert(fixed.name.clone(), schema.clone());
            }
        }
        Schema::Array(array) => collect(&array.items, names),
        Schema::Map(map) => collect(&map.types, names),
        Schema::Union(union) => {
            for variant in union.variants() {
                collect(variant, names);
            }
        }
        _ => {}
    }
}

fn branch_name(branch: &Schema) -> Option<String> {
    if let Some(encoding) = Encoding::of(branch) {
        return Some(encoding.name().to_owned());
    }

    match branch {
        Schema::Record(record) => Some(record.name.fullname(None)),
        Schema::Enum(enumeration) => Some(enumeration.name.fullname(None)),
        Schema::Fixed(fixed)
        | Schema::Duration(fixed)
        | Schema::Uuid(UuidSchema::Fixed(fixed))
        | Schema::Decimal(DecimalSchema {
            inner: InnerDecimalSchema::Fixed(fixed),
            ..
        }) => Some(fixed.name.fullname(None)),
        Schema::Array(_) => Some("array".to_owned()),
        Schema::Map(_) => Some("map".to_owned()),
        Schema::Union(_) => Some("union".to_owned()),
        _ => None,
    }
}

fn unscaled(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }

    let fill = if bytes[0] & 0x80 == 0 { 0 } else { 0xff };
    let mut full = [fill; 16];
    full[16 - bytes.len()..].copy_from_slice(bytes);

    Some(i128::from_be_bytes(full))
}

fn quoted(text: &str) -> String {
    serde_json::to_string(text).expect("a string serializes to JSON")
}

fn latin1(bytes: &[u8]) -> String {
    quoted(
        &bytes
            .iter()
            .map(|byte| char::from(*byte))
            .collect::<String>(),
    )
}

fn number(value: f64) -> String {
    match value.is_finite() {
        true => serde_json::to_string(&value).expect("a finite number serializes to JSON"),
        false => quoted(&value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use serde_json::{Value, json};

    use super::AvroSchema;
    use crate::registry::{RegistrySchema, SchemaFormat, SchemaSet};

    fn parsed(definition: &Value) -> Result<AvroSchema, crate::Error> {
        let schema = RegistrySchema::new(1, SchemaFormat::Avro, definition.to_string(), Vec::new());

        AvroSchema::parse(&SchemaSet::new(schema, Vec::new()))
    }

    fn doubling(level: usize) -> Value {
        let fields = match level {
            0 => json!([{"name": "x", "type": "long"}]),
            _ => json!([
                {"name": "a", "type": doubling(level - 1)},
                {"name": "b", "type": format!("T{}", level - 1)}
            ]),
        };

        json!({"type": "record", "name": format!("T{level}"), "fields": fields})
    }

    #[test]
    fn deep_recursive_payloads_are_undecodable() {
        let node = json!({"type": "record", "name": "Node", "fields": [
            {"name": "children", "type": {"type": "array", "items": "Node"}}]});
        let schema = parsed(&node).unwrap();
        let depth = 100_000;

        let deep: Vec<u8> = iter::repeat_n(2, depth)
            .chain(iter::repeat_n(0, depth + 1))
            .collect();

        assert_eq!(schema.decode(&deep), None);
        assert!(schema.decode(&[2, 0, 0]).is_some());
    }

    #[test]
    fn schemas_past_the_limits_are_rejected() {
        let small = parsed(&doubling(4));
        let expanding = parsed(&doubling(20));

        assert!(small.is_ok());
        assert!(matches!(expanding, Err(crate::Error::PayloadSchema { .. })));
    }
}
