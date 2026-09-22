//! Every failure a server operation can report to a client or a background loop.

use std::io;

use mink_table::{Bucket, Path};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("this node does not lead {bucket:?} (leader: {leader:?})")]
    NotLeader { bucket: Bucket, leader: Option<i32> },
    #[error("table {0} does not exist")]
    TableNotExist(Path),
    #[error("bucket {0:?} does not exist")]
    BucketNotExist(Bucket),
    #[error("{0:?} is a log table; the request needs a primary key table")]
    NotKvTable(Bucket),
    #[error("{0:?} is a primary key table; the request needs a log table")]
    NotLogTable(Bucket),
    #[error("bucket {0:?} is opening or closing")]
    Unavailable(Bucket),
    #[error("stream of {bucket:?} is still held by live node {node_id}")]
    HeldBy { bucket: Bucket, node_id: i32 },
    #[error("membership: {0}")]
    Membership(String),
    #[error(transparent)]
    Log(#[from] mink_log::Error),
    #[error(transparent)]
    Tablet(#[from] mink_tablet::Error),
    #[error(transparent)]
    Metadata(#[from] mink_metadata::Error),
    #[error(transparent)]
    Stream(#[from] s3stream::Error),
    #[error(transparent)]
    Kv(#[from] mink_kv::Error),
    #[error("local storage: {0}")]
    Io(#[from] io::Error),
}
