//! Maps an encoded bucket key to a bucket number, using the hashing scheme of the target lake format.

use mink_common::murmur;

use crate::{BucketId, Error, LakeFormat};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Bucketing {
    Native,
    Paimon,
    Iceberg,
}

impl Bucketing {
    pub fn for_lake(lake: Option<LakeFormat>) -> Self {
        match lake {
            None | Some(LakeFormat::Lance) => Bucketing::Native,
            Some(LakeFormat::Paimon) => Bucketing::Paimon,
            Some(LakeFormat::Iceberg) => Bucketing::Iceberg,
        }
    }

    pub fn bucket(self, key: &[u8], count: u32) -> Result<BucketId, Error> {
        if key.is_empty() {
            return Err(Error::EmptyBucketKey);
        }
        BucketId::check_count(count)?;

        let count = count as i32;
        let bucket = match self {
            Bucketing::Native => murmur::scramble(murmur::native32(key)) % count,
            Bucketing::Paimon => (murmur::native32(key) % count).abs(),
            Bucketing::Iceberg => (murmur::hash32(key, 0) & i32::MAX) % count,
        };

        Ok(BucketId(bucket as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lake_selection() {
        assert_eq!(Bucketing::for_lake(None), Bucketing::Native);
        assert_eq!(
            Bucketing::for_lake(Some(LakeFormat::Lance)),
            Bucketing::Native
        );
        assert_eq!(
            Bucketing::for_lake(Some(LakeFormat::Paimon)),
            Bucketing::Paimon
        );
        assert_eq!(
            Bucketing::for_lake(Some(LakeFormat::Iceberg)),
            Bucketing::Iceberg
        );
    }

    #[test]
    fn rejects_empty_key_and_bad_counts() {
        for bucketing in [Bucketing::Native, Bucketing::Paimon, Bucketing::Iceberg] {
            assert_eq!(bucketing.bucket(b"", 4).unwrap_err(), Error::EmptyBucketKey);
            assert_eq!(bucketing.bucket(b"k", 0).unwrap_err(), Error::BucketCount);
            assert_eq!(
                bucketing.bucket(b"k", 1 << 31).unwrap_err(),
                Error::BucketCount
            );
            assert_eq!(bucketing.bucket(b"k", 1).unwrap(), BucketId(0));
        }
    }
}
