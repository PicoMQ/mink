//! Length-prefixed binary encoding and decoding of primitives, strings, maps and JSON blobs over byte buffers.

use std::collections::BTreeMap;
use std::str;

use bytes::{Buf, BufMut, Bytes};
use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("corrupt encoding: {0}")]
    Corrupt(String),
}

fn need<B: Buf>(buf: &mut B, n: usize, what: &'static str) -> Result<(), Error> {
    if buf.remaining() < n {
        return Err(Error::Corrupt(format!("truncated reading {what}")));
    }
    Ok(())
}

pub fn get_u8<B: Buf>(buf: &mut B) -> Result<u8, Error> {
    need(buf, 1, "u8")?;
    Ok(buf.get_u8())
}

pub fn get_u16<B: Buf>(buf: &mut B) -> Result<u16, Error> {
    need(buf, 2, "u16")?;
    Ok(buf.get_u16_le())
}

pub fn get_u32<B: Buf>(buf: &mut B) -> Result<u32, Error> {
    need(buf, 4, "u32")?;
    Ok(buf.get_u32_le())
}

pub fn get_i32<B: Buf>(buf: &mut B) -> Result<i32, Error> {
    need(buf, 4, "i32")?;
    Ok(buf.get_i32_le())
}

pub fn get_u64<B: Buf>(buf: &mut B) -> Result<u64, Error> {
    need(buf, 8, "u64")?;
    Ok(buf.get_u64_le())
}

pub fn get_i64<B: Buf>(buf: &mut B) -> Result<i64, Error> {
    need(buf, 8, "i64")?;
    Ok(buf.get_i64_le())
}

pub fn put_bytes<B: BufMut>(buf: &mut B, bytes: &[u8]) {
    buf.put_u32_le(bytes.len() as u32);
    buf.put_slice(bytes);
}

pub fn get_bytes<B: Buf>(buf: &mut B) -> Result<Bytes, Error> {
    let len = get_u32(buf)? as usize;
    need(buf, len, "bytes")?;
    Ok(buf.copy_to_bytes(len))
}

pub fn put_str<B: BufMut>(buf: &mut B, s: &str) {
    put_bytes(buf, s.as_bytes());
}

pub fn get_str<B: Buf>(buf: &mut B) -> Result<String, Error> {
    let bytes = get_bytes(buf)?;
    str::from_utf8(&bytes)
        .map_err(|e| Error::Corrupt(format!("invalid utf-8: {e}")))
        .map(str::to_owned)
}

pub fn put_seq<B: BufMut, T>(buf: &mut B, items: &[T], put: impl Fn(&mut B, &T)) {
    buf.put_u32_le(items.len() as u32);
    for item in items {
        put(buf, item);
    }
}

pub fn get_seq<B: Buf, T, E: From<Error>>(
    buf: &mut B,
    mut get: impl FnMut(&mut B) -> Result<T, E>,
) -> Result<Vec<T>, E> {
    let len = get_u32(buf)? as usize;
    let mut items = Vec::with_capacity(len.min(4096));
    for _ in 0..len {
        items.push(get(buf)?);
    }

    Ok(items)
}

pub fn put_u64s<B: BufMut>(buf: &mut B, ids: &[u64]) {
    put_seq(buf, ids, |buf, id| buf.put_u64_le(*id));
}

pub fn get_u64s<B: Buf>(buf: &mut B) -> Result<Vec<u64>, Error> {
    get_seq(buf, get_u64)
}

pub fn put_str_map<B: BufMut>(buf: &mut B, map: &BTreeMap<String, String>) {
    buf.put_u32_le(map.len() as u32);
    for (key, value) in map {
        put_str(buf, key);
        put_str(buf, value);
    }
}

pub fn get_str_map<B: Buf>(buf: &mut B) -> Result<BTreeMap<String, String>, Error> {
    let len = get_u32(buf)? as usize;
    let mut map = BTreeMap::new();
    for _ in 0..len {
        let key = get_str(buf)?;
        let value = get_str(buf)?;
        map.insert(key, value);
    }

    Ok(map)
}

pub fn put_opt_str<B: BufMut>(buf: &mut B, s: Option<&str>) {
    match s {
        Some(s) => {
            buf.put_u8(1);
            put_str(buf, s);
        }
        None => buf.put_u8(0),
    }
}

pub fn get_opt_str<B: Buf>(buf: &mut B) -> Result<Option<String>, Error> {
    match get_u8(buf)? {
        0 => Ok(None),
        1 => Ok(Some(get_str(buf)?)),
        other => Err(Error::Corrupt(format!("option tag {other}"))),
    }
}

pub fn put_json<B: BufMut, T: Serialize>(buf: &mut B, value: &T) {
    let json = serde_json::to_vec(value).expect("json serializes");
    put_bytes(buf, &json);
}

pub fn get_json<B: Buf, T: DeserializeOwned>(buf: &mut B) -> Result<T, Error> {
    let blob = get_bytes(buf)?;
    serde_json::from_slice(&blob).map_err(|e| Error::Corrupt(format!("json: {e}")))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn strings_and_ints_roundtrip() {
        let mut buf = BytesMut::new();
        put_str(&mut buf, "hello");
        put_opt_str(&mut buf, Some("x"));
        put_opt_str(&mut buf, None);
        buf.put_u64_le(42);
        let mut cur = buf.freeze();
        assert_eq!(get_str(&mut cur).unwrap(), "hello");
        assert_eq!(get_opt_str(&mut cur).unwrap().as_deref(), Some("x"));
        assert_eq!(get_opt_str(&mut cur).unwrap(), None);
        assert_eq!(get_u64(&mut cur).unwrap(), 42);
    }
}
