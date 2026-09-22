//! Point and prefix lookups of rows by encoded key within a bucket.

use mink_table::Bucket;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lookup {
    pub bucket: Bucket,
    pub keys: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixLookup {
    pub bucket: Bucket,
    pub prefix: Vec<u8>,
}
