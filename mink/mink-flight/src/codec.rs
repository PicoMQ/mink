//! Turns log batches and key-value rows into Flight frames, and Flight batches back into log batches and puts.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::{FlightData, SchemaAsIpc};
use arrow_ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter,
};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use mink_record::{
    Batch, ChangeType, Changes, Codec, Remap, RowCodec, Rows, Spec, build, codec as log_codec,
    row_codec,
};
use mink_table::{KvFormat, LogFormat, Schema, SchemaId};
use mink_tablet::{Op, Value};

use crate::error::Error;
use crate::proto::ScanBatch;

pub fn arrow(schema: &Schema) -> SchemaRef {
    Arc::new(arrow_schema::Schema::from(schema.fields()))
}

pub fn projected(schema: &SchemaRef, columns: Option<&[usize]>) -> Result<SchemaRef, Error> {
    Ok(match columns {
        Some(columns) => Arc::new(schema.project(columns)?),
        None => schema.clone(),
    })
}

pub fn schema_frame(schema: &SchemaRef) -> FlightData {
    SchemaAsIpc::new(schema, &IpcWriteOptions::default()).into()
}

pub fn batch_frame(batch: &RecordBatch, app_metadata: Bytes) -> Result<FlightData, Error> {
    let (dictionaries, data) = IpcDataGenerator::default().encode(
        batch,
        &mut DictionaryTracker::new(false),
        &IpcWriteOptions::default(),
        &mut CompressionContext::default(),
    )?;
    debug_assert!(dictionaries.is_empty());
    let mut frame = FlightData::from(data);
    frame.app_metadata = app_metadata;

    Ok(frame)
}

pub struct Schemas<'a>(&'a [Schema]);

impl<'a> Schemas<'a> {
    pub fn new(all: &'a [Schema]) -> Self {
        Self(all)
    }

    pub fn get(&self, id: SchemaId) -> Result<&Schema, Error> {
        self.0.get(id.0 as usize).ok_or(Error::SchemaNotExist(id.0))
    }

    pub fn latest(&self) -> (SchemaId, &Schema) {
        let index = self.0.len() - 1;
        (SchemaId(index as u32), &self.0[index])
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct LogFrames<'a> {
    pub schemas: Schemas<'a>,
    pub format: LogFormat,
    pub columns: Option<&'a [usize]>,
    pub high_watermark: i64,
}

impl LogFrames<'_> {
    pub fn schema(&self) -> Result<SchemaRef, Error> {
        projected(&arrow(self.schemas.latest().1), self.columns)
    }

    pub fn frame(&self, bytes: Bytes) -> Result<FlightData, Error> {
        let batch = Batch::parse(bytes)?;
        let header = batch.header();
        let schema = self.schemas.get(header.schema_id)?;
        let (_, latest) = self.schemas.latest();
        let mut remap = Remap::new(schema.fields(), latest.fields());
        if let Some(columns) = self.columns {
            remap = remap.select(columns)?;
        }
        let codec = log_codec(self.format, Default::default());
        let decode = remap.decode_columns();
        let records = batch.records(codec.as_ref(), arrow(schema), decode.as_deref())?;
        let rows = remap.batch(records.batch)?;
        let meta = ScanBatch {
            base_offset: header.base_offset,
            last_offset: header.base_offset + i64::from(header.last_offset_delta),
            commit_timestamp: header.commit_timestamp,
            schema_id: header.schema_id,
            changes: match records.changes {
                Changes::AppendOnly(_) => None,
                Changes::Vector(bytes) => Some(bytes.to_vec()),
            },
            high_watermark: self.high_watermark,
        };
        batch_frame(&rows, serde_json::to_vec(&meta)?.into())
    }
}

pub fn rows(
    values: &[Bytes],
    schemas: &Schemas<'_>,
    format: KvFormat,
    columns: Option<&[usize]>,
) -> Result<RecordBatch, Error> {
    let (_, latest) = schemas.latest();
    let mut rows = Rows::new(latest.fields(), values.len())?;
    let mut codecs: Vec<Option<(Box<dyn RowCodec>, Remap)>> =
        (0..schemas.len()).map(|_| None).collect();
    for bytes in values {
        let value = Value::decode(bytes.clone())?;
        let schema = schemas.get(value.schema_id)?;
        let slot = &mut codecs[value.schema_id.0 as usize];
        let (codec, remap) = match slot {
            Some(codec) => codec,
            None => slot.insert((
                row_codec(format, schema.fields())?,
                Remap::new(schema.fields(), latest.fields()),
            )),
        };
        let row = codec.decode(&value.row)?;
        rows.push(&remap.row(&row))?;
    }
    let batch = rows.finish()?;

    Ok(match columns {
        Some(columns) => batch.project(columns)?,
        None => batch,
    })
}

pub fn ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    let mut writer = StreamWriter::try_new(&mut out, batch.schema_ref())?;
    writer.write(batch)?;
    writer.finish()?;

    Ok(out)
}

pub fn changes(rows: usize, changes: Option<&[u8]>) -> Result<Vec<ChangeType>, Error> {
    match changes {
        None => Ok(vec![ChangeType::AppendOnly; rows]),
        Some(bytes) if bytes.len() != rows => Err(Error::Request(format!(
            "{} change types for {rows} rows",
            bytes.len()
        ))),
        Some(bytes) => Ok(bytes
            .iter()
            .map(|b| ChangeType::from_byte(*b))
            .collect::<Result<_, _>>()?),
    }
}

pub fn ops(changes: &[ChangeType]) -> Vec<Op> {
    changes
        .iter()
        .map(|c| match c {
            ChangeType::Delete | ChangeType::UpdateBefore => Op::Delete,
            _ => Op::Upsert,
        })
        .collect()
}

pub fn log_batch(
    spec: Spec,
    changes: &[ChangeType],
    batch: &RecordBatch,
    codec: &dyn Codec,
) -> Result<Bytes, Error> {
    Ok(build(spec, changes, batch, codec)?.into())
}

pub fn check(batch: &RecordBatch, schema: &Schema) -> Result<(), Error> {
    let expected = arrow(schema);
    let got = batch.schema();
    let same = expected.fields().len() == got.fields().len()
        && expected
            .fields()
            .iter()
            .zip(got.fields())
            .all(|(e, g)| e.name() == g.name() && e.data_type() == g.data_type());
    if same {
        return Ok(());
    }

    let render = |schema: &SchemaRef| -> Vec<String> {
        schema
            .fields()
            .iter()
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect()
    };

    Err(Error::Request(format!(
        "batch columns {:?} do not match schema columns {:?}",
        render(&got),
        render(&expected),
    )))
}
