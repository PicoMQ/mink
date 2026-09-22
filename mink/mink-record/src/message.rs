//! Framing of a single Arrow IPC message: continuation marker, metadata length and padded flatbuffer.

use arrow_ipc::Message;

use crate::Error;

const CONTINUATION: [u8; 4] = [0xff, 0xff, 0xff, 0xff];
const PREFIX: usize = 8;
const ALIGNMENT: usize = 8;

pub(crate) struct Framed<'a> {
    pub message: Message<'a>,
    pub body: &'a [u8],
    pub body_offset: usize,
}

pub(crate) fn split(bytes: &[u8]) -> Result<Framed<'_>, Error> {
    let truncated = |needed| Error::Truncated {
        needed,
        found: bytes.len(),
    };
    if bytes.len() < PREFIX {
        return Err(truncated(PREFIX));
    }
    if bytes[..4] != CONTINUATION {
        return Err(Error::Ipc("missing continuation marker".into()));
    }

    let metadata = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let metadata = usize::try_from(metadata)
        .map_err(|_| Error::Ipc(format!("negative metadata length {metadata}")))?;
    let body_offset = PREFIX + metadata;
    if bytes.len() < body_offset {
        return Err(truncated(body_offset));
    }

    let message = arrow_ipc::root_as_message(&bytes[PREFIX..body_offset])
        .map_err(|e| Error::Ipc(e.to_string()))?;

    Ok(Framed {
        message,
        body: &bytes[body_offset..],
        body_offset,
    })
}

pub(crate) fn frame(out: &mut Vec<u8>, flatbuffer: &[u8]) -> Result<(), Error> {
    let padded = (flatbuffer.len() + PREFIX).next_multiple_of(ALIGNMENT) - PREFIX;
    let size = i32::try_from(padded)
        .map_err(|_| Error::Ipc(format!("metadata of {padded} bytes too large")))?;

    out.extend_from_slice(&CONTINUATION);
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(flatbuffer);
    out.resize(out.len() + padded - flatbuffer.len(), 0);

    Ok(())
}
