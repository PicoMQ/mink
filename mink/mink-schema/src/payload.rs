use crate::Error;
use crate::avro::AvroSchema;
use crate::json::JsonSchema;
use crate::merge::Rejection;
use crate::protobuf::ProtobufSchema;
use crate::registry::{SchemaFormat, SchemaSet};
use crate::types::PayloadType;
use crate::value::PayloadValue;

pub struct PayloadSchema {
    parsed: Parsed,
}

enum Parsed {
    Avro(AvroSchema),
    Protobuf(ProtobufSchema),
    Json(JsonSchema),
}

impl PayloadSchema {
    pub fn parse(set: &SchemaSet) -> Result<PayloadSchema, Error> {
        let parsed = match set.schema().format() {
            SchemaFormat::Avro => Parsed::Avro(AvroSchema::parse(set)?),
            SchemaFormat::Protobuf => Parsed::Protobuf(ProtobufSchema::parse(set)?),
            SchemaFormat::Json => Parsed::Json(JsonSchema::parse(set)?),
        };

        Ok(PayloadSchema { parsed })
    }

    pub(crate) fn payload_type(&self) -> &PayloadType {
        match &self.parsed {
            Parsed::Avro(avro) => avro.payload_type(),
            Parsed::Protobuf(protobuf) => protobuf.payload_type(),
            Parsed::Json(json) => json.payload_type(),
        }
    }

    pub(crate) fn decode(&self, payload: &[u8]) -> Option<PayloadValue> {
        match &self.parsed {
            Parsed::Avro(avro) => avro.decode(payload),
            Parsed::Protobuf(protobuf) => protobuf.decode(payload),
            Parsed::Json(json) => json.decode(payload),
        }
    }
}

pub fn accept(set: &SchemaSet) -> Result<PayloadSchema, Rejection> {
    PayloadSchema::parse(set).map_err(|_| Rejection::Unsupported)
}
