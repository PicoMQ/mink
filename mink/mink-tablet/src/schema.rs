//! Schema versions of a table resolved to codecs, key encoders and Arrow schemas, cached by id.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::RecordBatch;
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use mink_common::sync::lock;
use mink_record::{KeyEncoder, Reader, Remap, Row, RowCodec, Scalar, row_codec};
use mink_table::{Bucketing, KvFormat, Schema, SchemaId};

use crate::Error;
use crate::merger::Decoded;
use crate::value::Value;

pub trait Schemas: Send + Sync {
    fn latest(&self) -> (SchemaId, Arc<Schema>);
    fn get(&self, id: SchemaId) -> Option<Arc<Schema>>;
}

#[derive(Debug, Clone)]
pub struct Fixed {
    schemas: Vec<Arc<Schema>>,
}

impl Fixed {
    pub fn new(schemas: Vec<Schema>) -> Self {
        assert!(!schemas.is_empty(), "a table has at least one schema");
        Fixed {
            schemas: schemas.into_iter().map(Arc::new).collect(),
        }
    }

    pub fn single(schema: Schema) -> Self {
        Fixed::new(vec![schema])
    }
}

impl Schemas for Fixed {
    fn latest(&self) -> (SchemaId, Arc<Schema>) {
        let last = self.schemas.len() - 1;
        (SchemaId(last as u32), Arc::clone(&self.schemas[last]))
    }

    fn get(&self, id: SchemaId) -> Option<Arc<Schema>> {
        self.schemas.get(id.0 as usize).cloned()
    }
}

pub struct Version {
    pub id: SchemaId,
    pub schema: Arc<Schema>,
    pub arrow: SchemaRef,
    pub codec: Box<dyn RowCodec>,
    pub keys: KeyEncoder,
    pub key_indexes: Vec<usize>,
    pub auto_increment: Option<usize>,
}

impl Version {
    fn new(
        id: SchemaId,
        schema: Arc<Schema>,
        kv_format: KvFormat,
        bucketing: Bucketing,
    ) -> Result<Self, Error> {
        let key = schema
            .primary_key()
            .ok_or(Error::NoPrimaryKey)?
            .columns()
            .to_vec();
        let keys = KeyEncoder::new(schema.fields(), &key, bucketing)?;
        let key_indexes = schema.primary_key_indexes();
        let codec = row_codec(kv_format, schema.fields())?;
        let arrow = Arc::new(ArrowSchema::from(schema.fields()));
        let auto_increment = schema
            .auto_increment()
            .and_then(|name| schema.columns().iter().position(|c| c.name() == name));

        Ok(Version {
            id,
            schema,
            arrow,
            codec,
            keys,
            key_indexes,
            auto_increment,
        })
    }

    pub fn field_count(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn encode(&self, row: &[Option<Scalar<'_>>]) -> Result<Value, Error> {
        let mut bytes = Vec::new();
        self.codec.encode(row, &mut bytes)?;

        Ok(Value::new(self.id, bytes))
    }

    pub(crate) fn readers<'a>(&self, batch: &'a RecordBatch) -> Result<Vec<Reader<'a>>, Error> {
        let fields = self.schema.fields();
        if batch.num_columns() != fields.len() {
            return Err(mink_record::Error::ColumnIndex {
                index: fields.len() - 1,
                found: batch.num_columns(),
            }
            .into());
        }

        fields
            .iter()
            .zip(batch.columns())
            .map(|(field, array)| Ok(Reader::new(array, field.data_type(), field.name())?))
            .collect()
    }
}

pub struct Versions {
    schemas: Arc<dyn Schemas>,
    kv_format: KvFormat,
    bucketing: Bucketing,
    cache: Mutex<HashMap<SchemaId, Arc<Version>>>,
    remaps: Mutex<HashMap<(SchemaId, SchemaId), Arc<Remap>>>,
}

impl Versions {
    pub fn new(schemas: Arc<dyn Schemas>, kv_format: KvFormat, bucketing: Bucketing) -> Self {
        Versions {
            schemas,
            kv_format,
            bucketing,
            cache: Mutex::new(HashMap::new()),
            remaps: Mutex::new(HashMap::new()),
        }
    }

    pub fn latest(&self) -> Result<Arc<Version>, Error> {
        let (id, schema) = self.schemas.latest();
        self.resolve(id, schema)
    }

    pub fn get(&self, id: SchemaId) -> Result<Arc<Version>, Error> {
        if let Some(version) = self.cache().get(&id) {
            return Ok(Arc::clone(version));
        }

        let schema = self.schemas.get(id).ok_or(Error::SchemaNotExist(id))?;
        self.resolve(id, schema)
    }

    pub fn kv_format(&self) -> KvFormat {
        self.kv_format
    }

    pub fn decode<'a>(&self, value: &'a Value) -> Result<Decoded<'a>, Error> {
        let version = self.get(value.schema_id)?;
        let row: Row<'a> = version.codec.decode(&value.row)?;
        let latest = self.latest()?;
        if version.id == latest.id {
            return Ok(Decoded {
                schema_id: version.id,
                row,
            });
        }
        let remap = self.remap(&version, &latest);

        Ok(Decoded {
            schema_id: latest.id,
            row: remap.row(&row),
        })
    }

    pub fn remap(&self, from: &Version, to: &Version) -> Arc<Remap> {
        let key = (from.id, to.id);
        if let Some(remap) = lock(&self.remaps).get(&key) {
            return Arc::clone(remap);
        }
        let remap = Arc::new(Remap::new(from.schema.fields(), to.schema.fields()));
        lock(&self.remaps).insert(key, Arc::clone(&remap));
        remap
    }

    fn resolve(&self, id: SchemaId, schema: Arc<Schema>) -> Result<Arc<Version>, Error> {
        let mut cache = self.cache();
        if let Some(version) = cache.get(&id) {
            return Ok(Arc::clone(version));
        }

        let version = Arc::new(Version::new(id, schema, self.kv_format, self.bucketing)?);
        cache.insert(id, Arc::clone(&version));

        Ok(version)
    }

    fn cache(&self) -> MutexGuard<'_, HashMap<SchemaId, Arc<Version>>> {
        lock(&self.cache)
    }
}
