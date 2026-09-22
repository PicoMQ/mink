//! Request and response shapes of the Flight protocol, shared by servers and clients.
//! Plain data with JSON encoding and no behavior.

mod address;
mod cluster;
mod config;
mod database;
mod health;
mod lookup;
mod metadata;
mod offset;
mod partition;
mod path;
mod producer;
mod read;
mod rebalance;
mod redirect;
mod snapshot;
mod stats;
mod table;
mod tail;
mod write;

pub mod action;
pub mod header;

pub use address::{MAX_MESSAGE_BYTES, http_uri};
pub use cluster::{ClusterInfo, ClusterNode, CoordinatorInfo};
pub use config::ConfigEntries;
pub use database::{CreateDatabase, DatabaseName, DropDatabase, Exists, Names};
pub use health::Health;
pub use lookup::{Lookup, PrefixLookup};
pub use metadata::{Metadata, NodeInfo};
pub use offset::{ListOffsets, Offset, OffsetSpec};
pub use partition::{PartitionCreated, PartitionInfo, PartitionRequest, Partitions};
pub use path::descriptor_path;
pub use producer::{
    BucketOffset, ProducerOffsets, ProducerOffsetsRegistered, ProducerOffsetsResult, ProducerRef,
    RegisterProducerOffsets,
};
pub use read::{Read, ScanBatch, SnapshotBatch};
pub use rebalance::{BucketMove, Rebalanced};
pub use redirect::Redirect;
pub use snapshot::{BucketRef, KvSnapshot, LakeSnapshot, LakeSnapshotResult, LatestKvSnapshot};
pub use stats::{BucketStats, KvStats, NodeStats, RetentionInfo, TieringStatus};
pub use table::{
    AlterTable, Altered, BucketLeader, CreateTable, Created, DropTable, TableInfo, TableRef,
};
pub use tail::Tail;
pub use write::{Routed, RoutedBucket, Write, WriteBatch, WriterId, Written};

#[cfg(test)]
mod tests {
    use mink_table::{Bucket, BucketId, Id, SchemaId};

    use super::*;

    #[test]
    fn write_commands_round_trip_as_tagged_json() {
        let write = Write::Put {
            bucket: Bucket::new(Id(3), BucketId(1)),
            schema_id: SchemaId(0),
            writer_id: Some(7),
            target_columns: Some(vec![0, 2]),
        };
        let json = serde_json::to_string(&write).unwrap();
        assert!(json.starts_with(r#"{"op":"put""#));
        assert_eq!(serde_json::from_str::<Write>(&json).unwrap(), write);

        let routed = Write::AppendTable {
            path: "db.t".parse().unwrap(),
            schema_id: SchemaId(0),
        };
        let json = serde_json::to_string(&routed).unwrap();
        assert_eq!(json, r#"{"op":"append_table","path":"db.t","schema_id":0}"#);
        assert_eq!(serde_json::from_str::<Write>(&json).unwrap(), routed);
    }

    #[test]
    fn optional_flags_default_off() {
        let drop: DropTable = serde_json::from_str(r#"{"path":"db.t"}"#).unwrap();
        assert!(!drop.ignore_if_not_exists);
        let spec: OffsetSpec = serde_json::from_str(r#"{"kind":"latest"}"#).unwrap();
        assert_eq!(spec, OffsetSpec::Latest);
    }
}
