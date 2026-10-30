use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use iceberg::spec::Schema;
use mink_core::{Part, Record};
use mink_iceberg::{FileSchema, TypedSchema};
use mink_schema::{
    BINARY_SCHEMA_ID, Decoders, MergeResult, PartSchemaId, PayloadSchema, Rejection, SchemaId,
    SchemaKey, merge, schema_ids,
};
use mink_store::Store;

use crate::Error;
use crate::registry::{self, Failure, Registry, retrying};

pub(crate) struct Schemas {
    store: Store,
    registry: Option<Box<dyn Registry>>,
    parsed: Mutex<HashMap<SchemaKey, Arc<PayloadSchema>>>,
}

impl Schemas {
    pub(crate) fn new(store: Store, registry: Option<Box<dyn Registry>>) -> Schemas {
        Schemas {
            store,
            registry,
            parsed: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn registry(&self) -> Option<&dyn Registry> {
        self.registry.as_deref()
    }

    pub(crate) async fn latest_file_schema(
        &self,
        topic: &str,
        records: &[Record],
    ) -> Result<FileSchema, Error> {
        let Some(registry) = self.registry() else {
            return Ok(FileSchema::Binary);
        };

        for schema_id in schema_ids(records) {
            let stored = self.store.schemas().result(topic, schema_id).await?;

            if stored.is_none() {
                self.merge_schema(registry, topic, schema_id).await?;
            }
        }

        let latest = self.store.schemas().latest(topic).await?;

        self.typed(topic, latest).await
    }

    pub(crate) async fn file_schema(
        &self,
        topic: &str,
        schema_id: i32,
    ) -> Result<Option<FileSchema>, Error> {
        let stored = self.store.schemas().table_schema(topic, schema_id).await?;
        let Some(schema) = stored else {
            return Ok(None);
        };

        Ok(Some(self.typed(topic, schema).await?))
    }

    pub(crate) async fn table_schema(&self, topic: &str, schema_id: i32) -> Result<Schema, Error> {
        let stored = self.store.schemas().table_schema(topic, schema_id).await?;

        stored.ok_or_else(|| Error::UnknownTableSchema {
            topic: topic.to_owned(),
            id: schema_id,
        })
    }

    async fn typed(&self, topic: &str, schema: Schema) -> Result<FileSchema, Error> {
        if schema.schema_id() == BINARY_SCHEMA_ID {
            return Ok(FileSchema::Binary);
        }

        let decoders = self.decoders(topic).await?;
        let typed = TypedSchema::new(schema, decoders)?;

        Ok(FileSchema::Typed(Box::new(typed)))
    }

    async fn decoders(&self, topic: &str) -> Result<Decoders, Error> {
        let mut decoders = Decoders::default();

        for topic_schema in self.store.schemas().topic_schemas(topic).await? {
            let schema_id = topic_schema.schema_id();

            match topic_schema.result() {
                MergeResult::Merged {
                    registry_key,
                    field_ids,
                    ..
                } => {
                    let schema = self.parsed(*registry_key).await?;

                    decoders.insert_typed(schema_id, schema, field_ids.clone());
                }
                MergeResult::Rejected(_) => decoders.insert_raw(schema_id),
            }
        }

        Ok(decoders)
    }

    async fn merge_schema(
        &self,
        registry: &dyn Registry,
        topic: &str,
        schema_id: PartSchemaId,
    ) -> Result<(), Error> {
        let schemas = self.store.schemas();

        let key = match retrying(|| registry.resolve(schema_id.schema_id())).await {
            Ok(key) => key,
            Err(Failure::Rejected(rejection)) => {
                schemas.merge(topic, schema_id, &Err(rejection)).await?;

                return Ok(());
            }
            Err(Failure::Failed(error)) => return Err(error),
        };

        let canonical = PartSchemaId::new(schema_id.part(), SchemaId::Key(key));
        if schemas.result(topic, canonical).await?.is_none() {
            self.merge_registry_schema(registry, topic, canonical, key)
                .await?;
        }

        if schema_id != canonical {
            schemas
                .copy(topic, canonical, schema_id.schema_id())
                .await?;
        }

        Ok(())
    }

    async fn merge_registry_schema(
        &self,
        registry: &dyn Registry,
        topic: &str,
        schema_id: PartSchemaId,
        key: SchemaKey,
    ) -> Result<(), Error> {
        let schemas = self.store.schemas();
        let part = schema_id.part();

        let payload = match retrying(|| self.payload_schema(registry, topic, part, key)).await {
            Ok(payload) => Ok(payload),
            Err(Failure::Rejected(rejection)) => Err(rejection),
            Err(Failure::Failed(error)) => return Err(error),
        };

        loop {
            let merged = match &payload {
                Ok(payload) => {
                    let latest = schemas.latest(topic).await?;
                    let field_ids = schemas.field_ids(topic, part).await?;

                    merge(&latest, &field_ids, part, payload, key)
                }
                Err(rejection) => Err(*rejection),
            };

            match schemas.merge(topic, schema_id, &merged).await {
                Ok(_) => return Ok(()),
                Err(mink_store::Error::SchemaConflict { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn payload_schema(
        &self,
        registry: &dyn Registry,
        topic: &str,
        part: Part,
        key: SchemaKey,
    ) -> Result<Arc<PayloadSchema>, Failure> {
        if !registry.accepted(topic, part, key).await? {
            return Err(Failure::Rejected(Rejection::NotAccepted));
        }

        let set = registry::fetch(&self.store, registry, key).await?;
        let schema = PayloadSchema::parse(&set).map_err(|_| Rejection::Unsupported)?;
        let schema = Arc::new(schema);

        self.locked().insert(key, schema.clone());
        Ok(schema)
    }

    async fn parsed(&self, key: SchemaKey) -> Result<Arc<PayloadSchema>, Error> {
        if let Some(cached) = self.locked().get(&key).cloned() {
            return Ok(cached);
        }

        let set = registry::stored(&self.store, key).await?;
        let schema = Arc::new(PayloadSchema::parse(&set)?);

        self.locked().insert(key, schema.clone());
        Ok(schema)
    }

    fn locked(&self) -> MutexGuard<'_, HashMap<SchemaKey, Arc<PayloadSchema>>> {
        self.parsed
            .lock()
            .expect("the parsed schemas are never poisoned")
    }
}
