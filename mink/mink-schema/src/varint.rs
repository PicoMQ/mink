pub(crate) fn zigzag(bytes: &mut &[u8]) -> Option<i64> {
    let mut value: u64 = 0;

    for shift in (0..64).step_by(7) {
        let (byte, rest) = bytes.split_first()?;
        *bytes = rest;
        value |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            return Some((value >> 1) as i64 ^ -((value & 1) as i64));
        }
    }

    None
}
