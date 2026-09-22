//! Murmur3 32-bit hashing in the exact variant the JVM lake formats use for bucketing.

const C1: u32 = 0xcc9e_2d51;
const C2: u32 = 0x1b87_3593;

pub const NATIVE_SEED: u32 = 42;

pub fn hash32(bytes: &[u8], seed: u32) -> i32 {
    let (blocks, tail) = bytes.as_chunks::<4>();
    let mut h1 = seed;
    for block in blocks {
        h1 = mix_h1(h1, mix_k1(u32::from_le_bytes(*block)));
    }
    if !tail.is_empty() {
        let mut k1 = 0u32;
        for (index, &byte) in tail.iter().enumerate() {
            k1 |= u32::from(byte) << (8 * index);
        }
        h1 ^= mix_k1(k1);
    }
    fmix(h1 ^ bytes.len() as u32) as i32
}

// The Flink/Spark-derived variant Paimon also uses: seed 42, and each tail byte is
// sign-extended and mixed as a whole block instead of folded into one partial block.
pub fn native32(bytes: &[u8]) -> i32 {
    let (blocks, tail) = bytes.as_chunks::<4>();
    let mut h1 = NATIVE_SEED;
    for block in blocks {
        h1 = mix_h1(h1, mix_k1(u32::from_le_bytes(*block)));
    }
    for &byte in tail {
        let k1 = byte as i8 as i32 as u32;
        h1 = mix_h1(h1, mix_k1(k1));
    }
    fmix(h1 ^ bytes.len() as u32) as i32
}

pub fn scramble(code: i32) -> i32 {
    let mut code = mix_k1(code as u32);
    code = code
        .rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe654_6b64);
    let mixed = fmix(code ^ 4) as i32;
    if mixed == i32::MIN { 0 } else { mixed.abs() }
}

fn mix_k1(k1: u32) -> u32 {
    k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2)
}

fn mix_h1(h1: u32, k1: u32) -> u32 {
    (h1 ^ k1)
        .rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe654_6b64)
}

fn fmix(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash32_matches_reference_vectors() {
        assert_eq!(hash32(b"", 0), 0);
        assert_eq!(hash32(b"", 1), 0x514E_28B7);
        assert_eq!(hash32(b"", 0xffff_ffff), 0x81F1_6F39_u32 as i32);
        assert_eq!(hash32(&[0xff, 0xff, 0xff, 0xff], 0), 0x7629_3B50);
        assert_eq!(hash32(&[0x21, 0x43, 0x65, 0x87], 0), 0xF55B_516B_u32 as i32);
        assert_eq!(hash32(&[0x21, 0x43, 0x65], 0), 0x7E4A_8634);
        assert_eq!(hash32(&[0x21, 0x43], 0), 0xA0F7_B07A_u32 as i32);
        assert_eq!(hash32(&[0x21], 0), 0x72661CF4);
        assert_eq!(hash32(b"Hello, world!", 0), 0xc0363e43_u32 as i32);
    }

    #[test]
    fn native32_differs_from_standard_only_in_seed_and_tail() {
        assert_eq!(native32(&[1, 2, 3, 4]), hash32(&[1, 2, 3, 4], NATIVE_SEED));
        assert_ne!(native32(&[1, 2, 3]), hash32(&[1, 2, 3], NATIVE_SEED));
    }

    #[test]
    fn native32_sign_extends_tail_bytes() {
        assert_ne!(native32(&[0x80]), native32(&[0x00]));
        assert_eq!(native32(&[0]), -783_713_497);
    }

    #[test]
    fn scramble_is_non_negative() {
        for code in [0, 1, -1, 42, i32::MAX, i32::MIN, 0x7f7f_7f7f, -0x1234_5678] {
            assert!(scramble(code) >= 0, "{code}");
        }
    }
}
