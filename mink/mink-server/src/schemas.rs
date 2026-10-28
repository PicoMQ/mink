use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use iceberg::spec::Schema;
use mink_core::{Part, Record};
use mink_iceberg::{FileSchema, TypedSchema};
use mink_schema::{
    BINARY_SCHEMA_ID, Decoders, MergeResult, PartSchemaRef, PayloadSchema, Rejection, SchemaRef,
    accept, merge, schema_refs, subject,
};
use mink_store::Store;

use crate::Error;
use crate::registry::{self, Failure, Registry, retrying};

pub(crate) struct Schemas {
    store: Store,
    registry: Option<Registry>,
    parsed: Mutex<HashMap<i32, Arc<PayloadSchema>>>,
}

impl Schemas {
    pub(crate) fn new(store: Store, registry: Option<Registry>) -> Schemas {
        Schemas {
            store,
            registry,
            parsed: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn registry(&self) -> Option<&Registry> {
        self.registry.as_ref()
    }

    pub(crate) async fn latest_file_schema(
        &self,
        topic: &str,
        records: &[Record],
    ) -> Result<FileSchema, Error> {
        let Some(registry) = &self.registry else {
            return Ok(FileSchema::Binary);
        };

        for schema_ref in schema_refs(records) {
            let stored = self.store.schemas().result(topic, schema_ref).await?;

            if stored.is_none() {
                self.merge_schema(registry, topic, schema_ref).await?;
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
            let schema_ref = topic_schema.schema_ref();

            match topic_schema.result() {
                MergeResult::Merged {
                    registry_id,
                    field_ids,
                    ..
                } => {
                    let schema = self.parsed(*registry_id).await?;

                    decoders.insert_typed(schema_ref, schema, field_ids.clone());
                }
                MergeResult::Rejected(_) => decoders.insert_raw(schema_ref),
            }
        }

        Ok(decoders)
    }

    async fn merge_schema(
        &self,
        registry: &Registry,
        topic: &str,
        schema_ref: PartSchemaRef,
    ) -> Result<(), Error> {
        let schemas = self.store.schemas();

        let id = match schema_ref.schema_ref() {
            SchemaRef::Id(id) => id,
            SchemaRef::Guid(guid) => {
                let resolved = retrying(|| async move {
                    let id = registry.guid_id(guid).await?;
                    id.ok_or(Failure::Rejected(Rejection::Missing))
                })
                .await;

                match resolved {
                    Ok(id) => id,
                    Err(Failure::Rejected(rejection)) => {
                        schemas.merge(topic, schema_ref, &Err(rejection)).await?;

                        return Ok(());
                    }
                    Err(Failure::Failed(error)) => return Err(error),
                }
            }
        };

        let by_id = PartSchemaRef::new(schema_ref.part(), SchemaRef::Id(id));
        if schemas.result(topic, by_id).await?.is_none() {
            self.merge_registry_schema(registry, topic, by_id, id)
                .await?;
        }

        if schema_ref != by_id {
            schemas.copy(topic, by_id, schema_ref.schema_ref()).await?;
        }

        Ok(())
    }

    async fn merge_registry_schema(
        &self,
        registry: &Registry,
        topic: &str,
        schema_ref: PartSchemaRef,
        id: i32,
    ) -> Result<(), Error> {
        let schemas = self.store.schemas();
        let part = schema_ref.part();

        let payload = match retrying(|| self.payload_schema(registry, topic, part, id)).await {
            Ok(payload) => Ok(payload),
            Err(Failure::Rejected(rejection)) => Err(rejection),
            Err(Failure::Failed(error)) => return Err(error),
        };

        loop {
            let merged = match &payload {
                Ok(payload) => {
                    let latest = schemas.latest(topic).await?;
                    let field_ids = schemas.field_ids(topic, part).await?;

                    merge(&latest, &field_ids, part, payload, id)
                }
                Err(rejection) => Err(*rejection),
            };

            match schemas.merge(topic, schema_ref, &merged).await {
                Ok(_) => return Ok(()),
                Err(mink_store::Error::SchemaConflict { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn payload_schema(
        &self,
        registry: &Registry,
        topic: &str,
        part: Part,
        id: i32,
    ) -> Result<Arc<PayloadSchema>, Failure> {
        let subject = subject(topic, part);

        if !registry.subject_exists(&subject).await? {
            return Err(Failure::Rejected(Rejection::NotAccepted));
        }

        let set = registry::fetch(&self.store, registry, id).await?;
        let registered_under_subject = registry.accepted(id, &subject).await?;
        let schema = Arc::new(accept(&set, registered_under_subject)?);

        self.locked().insert(id, schema.clone());
        Ok(schema)
    }

    async fn parsed(&self, id: i32) -> Result<Arc<PayloadSchema>, Error> {
        if let Some(cached) = self.locked().get(&id).cloned() {
            return Ok(cached);
        }

        let set = registry::stored(&self.store, id).await?;
        let schema = Arc::new(PayloadSchema::parse(&set)?);

        self.locked().insert(id, schema.clone());
        Ok(schema)
    }

    fn locked(&self) -> MutexGuard<'_, HashMap<i32, Arc<PayloadSchema>>> {
        self.parsed
            .lock()
            .expect("the parsed schemas are never poisoned")
    }
}
