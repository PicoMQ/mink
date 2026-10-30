use std::collections::HashMap;
use std::fmt;

use mink_core::Part;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaFormat {
    Avro,
    Protobuf,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SchemaRef {
    ConfluentId(i32),
    ConfluentGuid(Uuid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SchemaKey {
    Confluent(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    name: String,
    key: SchemaKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrySchema {
    key: SchemaKey,
    format: SchemaFormat,
    definition: String,
    references: Vec<Reference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSet {
    schema: RegistrySchema,
    referenced: Vec<RegistrySchema>,
}

impl SchemaFormat {
    pub(crate) const ALL: [SchemaFormat; 3] = [
        SchemaFormat::Avro,
        SchemaFormat::Protobuf,
        SchemaFormat::Json,
    ];

    pub fn name(self) -> &'static str {
        match self {
            SchemaFormat::Avro => "avro",
            SchemaFormat::Protobuf => "protobuf",
            SchemaFormat::Json => "json",
        }
    }

    pub fn from_name(name: &str) -> Option<SchemaFormat> {
        SchemaFormat::ALL
            .into_iter()
            .find(|format| format.name() == name)
    }
}

impl SchemaRef {
    pub fn parse(text: &str) -> Option<SchemaRef> {
        let (kind, value) = text.split_once(':')?;

        match kind {
            "confluent-id" => value.parse().ok().map(SchemaRef::ConfluentId),
            "confluent-guid" => Uuid::parse_str(value).ok().map(SchemaRef::ConfluentGuid),
            _ => None,
        }
    }
}

impl fmt::Display for SchemaRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaRef::ConfluentId(id) => write!(f, "confluent-id:{id}"),
            SchemaRef::ConfluentGuid(guid) => write!(f, "confluent-guid:{guid}"),
        }
    }
}

impl SchemaKey {
    pub fn parse(text: &str) -> Option<SchemaKey> {
        let (kind, value) = text.split_once(':')?;

        match kind {
            "confluent" => value.parse().ok().map(SchemaKey::Confluent),
            _ => None,
        }
    }

    pub fn schema_ref(self) -> SchemaRef {
        match self {
            SchemaKey::Confluent(id) => SchemaRef::ConfluentId(id),
        }
    }
}

impl fmt::Display for SchemaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaKey::Confluent(id) => write!(f, "confluent:{id}"),
        }
    }
}

impl Reference {
    pub fn new(name: impl Into<String>, key: SchemaKey) -> Reference {
        Reference {
            name: name.into(),
            key,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key(&self) -> SchemaKey {
        self.key
    }
}

impl RegistrySchema {
    pub fn new(
        key: SchemaKey,
        format: SchemaFormat,
        definition: impl Into<String>,
        references: Vec<Reference>,
    ) -> RegistrySchema {
        RegistrySchema {
            key,
            format,
            definition: definition.into(),
            references,
        }
    }

    pub fn key(&self) -> SchemaKey {
        self.key
    }

    pub fn format(&self) -> SchemaFormat {
        self.format
    }

    pub fn definition(&self) -> &str {
        &self.definition
    }

    pub fn references(&self) -> &[Reference] {
        &self.references
    }
}

impl SchemaSet {
    pub fn new(schema: RegistrySchema, referenced: Vec<RegistrySchema>) -> SchemaSet {
        SchemaSet { schema, referenced }
    }

    pub async fn load<E, Fut>(
        key: SchemaKey,
        mut lookup: impl FnMut(SchemaKey) -> Fut,
    ) -> Result<SchemaSet, E>
    where
        Fut: Future<Output = Result<RegistrySchema, E>>,
    {
        let mut pending = vec![key];
        let mut found: Vec<RegistrySchema> = Vec::new();

        while let Some(next) = pending.pop() {
            if found.iter().any(|schema| schema.key() == next) {
                continue;
            }

            let schema = lookup(next).await?;

            pending.extend(schema.references().iter().map(Reference::key));
            found.push(schema);
        }

        let schema = found.remove(0);

        Ok(SchemaSet::new(schema, found))
    }

    pub fn schema(&self) -> &RegistrySchema {
        &self.schema
    }

    pub fn referenced(&self) -> &[RegistrySchema] {
        &self.referenced
    }

    pub(crate) fn definitions_by_name(&self) -> HashMap<&str, &str> {
        let mut by_name = HashMap::new();

        for declaring in [&self.schema].into_iter().chain(&self.referenced) {
            for reference in declaring.references() {
                let referenced = self
                    .referenced
                    .iter()
                    .find(|candidate| candidate.key() == reference.key());

                if let Some(referenced) = referenced {
                    by_name.insert(reference.name(), referenced.definition());
                }
            }
        }

        by_name
    }
}

pub fn subject(topic: &str, part: Part) -> String {
    format!("{topic}-{}", part.name())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_refs_and_keys_round_trip() {
        let refs = [
            SchemaRef::ConfluentId(7),
            SchemaRef::ConfluentGuid(Uuid::from_u128(9)),
        ];

        for format in SchemaFormat::ALL {
            assert_eq!(SchemaFormat::from_name(format.name()), Some(format));
        }
        for schema_ref in refs {
            assert_eq!(SchemaRef::parse(&schema_ref.to_string()), Some(schema_ref));
        }
        let key = SchemaKey::Confluent(7);

        assert_eq!(SchemaKey::parse(&key.to_string()), Some(key));
        assert_eq!(key.schema_ref(), SchemaRef::ConfluentId(7));
        assert_eq!(SchemaRef::parse("name:7"), None);
        assert_eq!(SchemaKey::parse("id:7"), None);
        assert_eq!(subject("orders", Part::Value), "orders-value");
    }

    #[tokio::test]
    async fn a_schema_set_holds_every_referenced_schema_once() {
        let key = SchemaKey::Confluent;
        let shared = RegistrySchema::new(key(3), SchemaFormat::Avro, "shared", Vec::new());
        let left = RegistrySchema::new(
            key(2),
            SchemaFormat::Avro,
            "left",
            vec![Reference::new("s", key(3))],
        );
        let main = RegistrySchema::new(
            key(1),
            SchemaFormat::Avro,
            "main",
            vec![Reference::new("l", key(2)), Reference::new("s", key(3))],
        );
        let registry = [main.clone(), left.clone(), shared.clone()];
        let mut lookups = Vec::new();

        let set = SchemaSet::load(key(1), |next| {
            lookups.push(next);

            let found = registry.iter().find(|schema| schema.key() == next).cloned();

            async move { found.ok_or(next) }
        })
        .await
        .unwrap();
        let missing = SchemaSet::load(key(4), |next| async move {
            Err::<RegistrySchema, SchemaKey>(next)
        })
        .await;

        assert_eq!(set.schema(), &main);
        assert_eq!(set.referenced(), [shared, left]);
        assert_eq!(lookups, [key(1), key(3), key(2)]);
        assert_eq!(
            set.definitions_by_name(),
            HashMap::from([("l", "left"), ("s", "shared")])
        );
        assert_eq!(missing, Err(key(4)));
    }
}
