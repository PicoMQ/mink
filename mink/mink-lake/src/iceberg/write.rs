//! Write results and committables carrying Iceberg data files, with a JSON wire form.

use iceberg::Error;
use iceberg::spec::{
    DataFile, FormatVersion, Schema, StructType, deserialize_data_file_from_json,
    serialize_data_file_to_json,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::iceberg::compact::RewriteResult;

#[derive(Debug, Clone, PartialEq)]
pub struct WriteResult {
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    pub rewrite: Option<RewriteResult>,
    pub context: FileContext,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileContext {
    pub schema: Schema,
    pub partition_type: StructType,
    pub partition_spec_id: i32,
    pub format_version: FormatVersion,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Committable {
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    pub rewrites: Vec<RewriteResult>,
    pub context: Option<FileContext>,
}

impl Committable {
    pub fn is_empty(&self) -> bool {
        self.data_files.is_empty() && self.delete_files.is_empty() && self.rewrites.is_empty()
    }

    pub fn from_results(results: Vec<WriteResult>) -> Self {
        let mut committable = Committable::default();
        for result in results {
            committable.data_files.extend(result.data_files);
            committable.delete_files.extend(result.delete_files);
            committable.rewrites.extend(result.rewrite);
            committable.context.get_or_insert(result.context);
        }

        committable
    }
}

#[derive(Serialize, Deserialize)]
struct Wire {
    context: FileContext,
    data_files: Vec<String>,
    delete_files: Vec<String>,
    #[serde(default)]
    rewrites: Vec<RewriteWire>,
}

#[derive(Serialize, Deserialize)]
struct RewriteWire {
    snapshot_id: i64,
    deleted: Vec<String>,
    added: Vec<String>,
}

fn encode(files: &[DataFile], context: &FileContext) -> Result<Vec<String>, Error> {
    files
        .iter()
        .map(|file| {
            serialize_data_file_to_json(
                file.clone(),
                &context.partition_type,
                context.format_version,
            )
        })
        .collect()
}

fn decode(files: &[String], context: &FileContext) -> Result<Vec<DataFile>, Error> {
    files
        .iter()
        .map(|json| {
            deserialize_data_file_from_json(
                json,
                context.partition_spec_id,
                &context.partition_type,
                &context.schema,
            )
        })
        .collect()
}

fn encode_rewrites(
    rewrites: &[RewriteResult],
    context: &FileContext,
) -> Result<Vec<RewriteWire>, Error> {
    rewrites
        .iter()
        .map(|rewrite| {
            Ok(RewriteWire {
                snapshot_id: rewrite.snapshot_id,
                deleted: encode(&rewrite.deleted, context)?,
                added: encode(&rewrite.added, context)?,
            })
        })
        .collect()
}

fn decode_rewrites(
    rewrites: &[RewriteWire],
    context: &FileContext,
) -> Result<Vec<RewriteResult>, Error> {
    rewrites
        .iter()
        .map(|wire| {
            Ok(RewriteResult {
                snapshot_id: wire.snapshot_id,
                deleted: decode(&wire.deleted, context)?,
                added: decode(&wire.added, context)?,
            })
        })
        .collect()
}

struct Decoded {
    data_files: Vec<DataFile>,
    delete_files: Vec<DataFile>,
    rewrites: Vec<RewriteResult>,
    context: FileContext,
}

impl Wire {
    fn encode(
        data_files: &[DataFile],
        delete_files: &[DataFile],
        rewrites: &[RewriteResult],
        context: &FileContext,
    ) -> Result<Wire, Error> {
        Ok(Wire {
            data_files: encode(data_files, context)?,
            delete_files: encode(delete_files, context)?,
            rewrites: encode_rewrites(rewrites, context)?,
            context: context.clone(),
        })
    }

    fn decode<E: serde::de::Error>(self) -> Result<Decoded, E> {
        Ok(Decoded {
            data_files: decode(&self.data_files, &self.context).map_err(E::custom)?,
            delete_files: decode(&self.delete_files, &self.context).map_err(E::custom)?,
            rewrites: decode_rewrites(&self.rewrites, &self.context).map_err(E::custom)?,
            context: self.context,
        })
    }
}

impl Serialize for WriteResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Wire::encode(
            &self.data_files,
            &self.delete_files,
            self.rewrite.as_slice(),
            &self.context,
        )
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WriteResult {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut decoded = Wire::deserialize(deserializer)?.decode()?;
        if decoded.rewrites.len() > 1 {
            return Err(serde::de::Error::custom(
                "a write result carries at most one rewrite",
            ));
        }

        Ok(WriteResult {
            data_files: decoded.data_files,
            delete_files: decoded.delete_files,
            rewrite: decoded.rewrites.pop(),
            context: decoded.context,
        })
    }
}

impl Serialize for Committable {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.context {
            Some(context) => Some(
                Wire::encode(
                    &self.data_files,
                    &self.delete_files,
                    &self.rewrites,
                    context,
                )
                .map_err(serde::ser::Error::custom)?,
            ),
            None => None,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Committable {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let Some(wire) = Option::<Wire>::deserialize(deserializer)? else {
            return Ok(Committable::default());
        };
        let decoded = wire.decode()?;

        Ok(Committable {
            data_files: decoded.data_files,
            delete_files: decoded.delete_files,
            rewrites: decoded.rewrites,
            context: Some(decoded.context),
        })
    }
}
