//! Minimal big-endian two's complement encoding of 128-bit integers, byte-compatible with the JVM big integer form.

pub fn to_bytes(value: i128, buf: &mut [u8; 16]) -> &[u8] {
    *buf = value.to_be_bytes();
    let sign = if value < 0 { 0xff } else { 0x00 };

    let mut start = 0;
    while start < 15 && buf[start] == sign && (buf[start + 1] & 0x80) == (sign & 0x80) {
        start += 1;
    }

    &buf[start..]
}

pub fn from_bytes(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }

    let fill = if bytes[0] & 0x80 != 0 { 0xff } else { 0x00 };
    let mut buf = [fill; 16];
    buf[16 - bytes.len()..].copy_from_slice(bytes);

    Some(i128::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn be(v: i128) -> Vec<u8> {
        to_bytes(v, &mut [0; 16]).to_vec()
    }

    #[test]
    fn matches_big_integer_to_byte_array() {
        assert_eq!(be(0), [0x00]);
        assert_eq!(be(1), [0x01]);
        assert_eq!(be(127), [0x7f]);
        assert_eq!(be(128), [0x00, 0x80]);
        assert_eq!(be(255), [0x00, 0xff]);
        assert_eq!(be(256), [0x01, 0x00]);
        assert_eq!(be(-1), [0xff]);
        assert_eq!(be(-128), [0x80]);
        assert_eq!(be(-129), [0xff, 0x7f]);
        assert_eq!(be(-256), [0xff, 0x00]);
        assert_eq!(be(i128::MAX).len(), 16);
        assert_eq!(be(i128::MIN).len(), 16);
        assert_eq!(
            be(1_234_567_890_123_456_789_012_345),
            [
                0x01, 0x05, 0x6e, 0x0f, 0x36, 0xa6, 0x44, 0x3d, 0xe2, 0xdf, 0x79
            ]
        );
    }

    #[test]
    fn round_trips() {
        for v in [
            0,
            1,
            -1,
            127,
            128,
            -128,
            -129,
            i64::MAX as i128,
            i128::MAX,
            i128::MIN,
            1_234_567_890_123_456_789_012_345,
        ] {
            assert_eq!(from_bytes(&be(v)).unwrap(), v);
        }
        assert_eq!(from_bytes(&[]), None);
        assert_eq!(from_bytes(&[0; 17]), None);
    }
}
