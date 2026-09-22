//! Tells a client which node now leads a bucket.

use mink_table::Bucket;
use serde::{Deserialize, Serialize};

use crate::metadata::NodeInfo;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redirect {
    pub bucket: Option<Bucket>,
    pub to: Option<NodeInfo>,
}
