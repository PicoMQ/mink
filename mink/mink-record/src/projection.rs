//! Column projection applied directly to an encoded Arrow batch by rewriting its IPC metadata and body.

use std::iter;

use arrow_data::layout;
use arrow_ipc::{
    BodyCompression, BodyCompressionArgs, Buffer, FieldNode, MessageArgs, MessageHeader,
    RecordBatchArgs,
};
use arrow_ipc::{Message, RecordBatch};
use arrow_schema::{Field, Schema};
use flatbuffers::FlatBufferBuilder;

use crate::header::{self, HEADER_SIZE};
use crate::{Batch, Error, message};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    nodes: Vec<bool>,
    buffers: Vec<bool>,
}

impl Projection {
    pub fn new(schema: &Schema, columns: &[usize]) -> Result<Self, Error> {
        let fields = schema.fields();
        let mut selected = vec![false; fields.len()];
        let mut previous = None;
        for &column in columns {
            if column >= fields.len() {
                return Err(Error::Projection(format!(
                    "column {column} is out of bounds for {} fields",
                    fields.len()
                )));
            }
            if previous.is_some_and(|p| column <= p) {
                return Err(Error::Projection(format!(
                    "columns must be increasing and distinct, got {columns:?}"
                )));
            }
            selected[column] = true;
            previous = Some(column);
        }

        let mut projection = Projection {
            nodes: Vec::new(),
            buffers: Vec::new(),
        };
        for (field, keep) in fields.iter().zip(selected) {
            projection.flatten(field, keep);
        }

        Ok(projection)
    }

    fn flatten(&mut self, field: &Field, keep: bool) {
        let layout = layout(field.data_type());
        let buffers = layout.buffers.len() + usize::from(layout.can_contain_null_mask);
        self.nodes.push(keep);
        self.buffers.extend(iter::repeat_n(keep, buffers));
        for child in children(field) {
            self.flatten(child, keep);
        }
    }

    // The CRC is left as written: projected batches are not CRC-checked.
    pub fn apply(&self, batch: &Batch) -> Result<Vec<u8>, Error> {
        let bytes = batch.bytes();
        let header = batch.header();
        if header.record_count == 0 {
            return Ok(bytes.to_vec());
        }
        let changes = if header.append_only {
            0
        } else {
            header.record_count as usize
        };
        let arrow_offset = HEADER_SIZE + changes;

        let framed = message::split(&bytes[arrow_offset..])?;
        let record_batch = framed
            .message
            .header_as_record_batch()
            .ok_or_else(|| Error::Ipc("message is not a record batch".into()))?;
        if record_batch.variadicBufferCounts().is_some() {
            return Err(Error::Ipc("view types are not supported".into()));
        }
        let nodes = record_batch.nodes().unwrap_or_default();
        let buffers = record_batch.buffers().unwrap_or_default();
        if nodes.len() != self.nodes.len() || buffers.len() != self.buffers.len() {
            return Err(Error::Projection(format!(
                "batch has {} nodes and {} buffers, schema has {} and {}",
                nodes.len(),
                buffers.len(),
                self.nodes.len(),
                self.buffers.len()
            )));
        }

        let kept_nodes: Vec<FieldNode> = nodes
            .iter()
            .zip(&self.nodes)
            .filter(|(_, keep)| **keep)
            .map(|(node, _)| FieldNode::new(node.length(), node.null_count()))
            .collect();

        let body_length = framed.message.bodyLength();
        let mut kept_buffers = Vec::new();
        let mut copies = Vec::new();
        let mut new_offset = 0i64;
        for (i, (buffer, keep)) in buffers.iter().zip(&self.buffers).enumerate() {
            if !keep {
                continue;
            }
            let next = if i + 1 < buffers.len() {
                buffers.get(i + 1).offset()
            } else {
                body_length
            };
            let padded = next - buffer.offset();
            kept_buffers.push(Buffer::new(new_offset, buffer.length()));
            copies.push((buffer.offset() as usize, padded as usize));
            new_offset += padded;
        }

        let mut fbb = FlatBufferBuilder::new();
        let compression = record_batch.compression().map(|c| {
            BodyCompression::create(
                &mut fbb,
                &BodyCompressionArgs {
                    codec: c.codec(),
                    method: c.method(),
                },
            )
        });
        let nodes = fbb.create_vector(&kept_nodes);
        let buffers = fbb.create_vector(&kept_buffers);
        let record_batch = RecordBatch::create(
            &mut fbb,
            &RecordBatchArgs {
                length: record_batch.length(),
                nodes: Some(nodes),
                buffers: Some(buffers),
                compression,
                variadicBufferCounts: None,
            },
        );
        let message = Message::create(
            &mut fbb,
            &MessageArgs {
                version: framed.message.version(),
                header_type: MessageHeader::RecordBatch,
                header: Some(record_batch.as_union_value()),
                bodyLength: new_offset,
                custom_metadata: None,
            },
        );
        fbb.finish(message, None);

        let mut out =
            Vec::with_capacity(arrow_offset + fbb.finished_data().len() + 8 + new_offset as usize);
        out.extend_from_slice(&bytes[..arrow_offset]);
        message::frame(&mut out, fbb.finished_data())?;
        for (offset, length) in copies {
            let body = framed
                .body
                .get(offset..offset + length)
                .ok_or(Error::Truncated {
                    needed: arrow_offset + framed.body_offset + offset + length,
                    found: bytes.len(),
                })?;
            out.extend_from_slice(body);
        }
        let size = out.len();
        header::set_size(&mut out, size)?;
        Ok(out)
    }
}

fn children(field: &Field) -> Vec<&Field> {
    use arrow_schema::DataType::*;

    match field.data_type() {
        List(child) | LargeList(child) | FixedSizeList(child, _) | Map(child, _) => {
            vec![child.as_ref()]
        }
        Struct(fields) => fields.iter().map(|f| f.as_ref()).collect(),
        _ => Vec::new(),
    }
}
