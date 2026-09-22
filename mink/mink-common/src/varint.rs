//! LEB128 variable-length integer encoding and decoding for 32- and 64-bit values,
//! with bounded decoding that rejects truncated or overlong input.

pub fn put_u32(out: &mut Vec<u8>, value: u32) {
    put_u64(out, u64::from(value));
}

pub fn put_u64(out: &mut Vec<u8>, mut value: u64) {
    while value & !0x7f != 0 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub fn put_i32(out: &mut Vec<u8>, value: i32) {
    put_u32(out, value as u32);
}

pub fn put_i64(out: &mut Vec<u8>, value: i64) {
    put_u64(out, value as u64);
}

fn get(input: &mut &[u8], max: usize) -> Option<u64> {
    let mut value = 0u64;
    for (i, &byte) in input.iter().take(max).enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            *input = &input[i + 1..];
            return Some(value);
        }
    }
    None
}

pub fn get_u32(input: &mut &[u8]) -> Option<u32> {
    get(input, 5).map(|v| v as u32)
}

pub fn get_u64(input: &mut &[u8]) -> Option<u64> {
    get(input, 10)
}

pub fn get_i32(input: &mut &[u8]) -> Option<i32> {
    get_u32(input).map(|v| v as i32)
}

pub fn get_i64(input: &mut &[u8]) -> Option<i64> {
    get_u64(input).map(|v| v as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(f: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut out = Vec::new();
        f(&mut out);
        out
    }

    #[test]
    fn widths() {
        assert_eq!(bytes(|o| put_i32(o, 0)), [0x00]);
        assert_eq!(bytes(|o| put_i32(o, 127)), [0x7f]);
        assert_eq!(bytes(|o| put_i32(o, 128)), [0x80, 0x01]);
        assert_eq!(bytes(|o| put_i32(o, 300)), [0xac, 0x02]);
        assert_eq!(bytes(|o| put_i32(o, -1)), [0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert_eq!(
            bytes(|o| put_i32(o, i32::MIN)),
            [0x80, 0x80, 0x80, 0x80, 0x08]
        );
        assert_eq!(bytes(|o| put_i64(o, -1)).len(), 10);
        assert_eq!(
            bytes(|o| put_i64(o, i64::MAX)),
            [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]
        );
    }

    #[test]
    fn round_trips() {
        for v in [0, 1, 127, 128, 300, -1, i32::MIN, i32::MAX] {
            let encoded = bytes(|o| put_i32(o, v));
            let mut input = encoded.as_slice();
            assert_eq!(get_i32(&mut input), Some(v));
            assert!(input.is_empty());
        }
        for v in [0, -1, i64::MIN, i64::MAX, 1 << 40] {
            let encoded = bytes(|o| put_i64(o, v));
            let mut input = encoded.as_slice();
            assert_eq!(get_i64(&mut input), Some(v));
            assert!(input.is_empty());
        }
    }

    #[test]
    fn rejects_truncated_and_overlong() {
        assert_eq!(get_i32(&mut &[0x80u8][..]), None);
        assert_eq!(
            get_i32(&mut &[0x80u8, 0x80, 0x80, 0x80, 0x80, 0x00][..]),
            None
        );
        let mut trailing = &[0x05u8, 0x09][..];
        assert_eq!(get_i32(&mut trailing), Some(5));
        assert_eq!(trailing, [0x09]);
    }
}
