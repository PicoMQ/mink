//! Names of every Flight action with a one-line description of each.

pub const INIT_WRITER: &str = "init_writer";
pub const LIST_OFFSETS: &str = "list_offsets";
pub const LOOKUP: &str = "lookup";
pub const PREFIX_LOOKUP: &str = "prefix_lookup";
pub const METADATA: &str = "metadata";
pub const CREATE_DATABASE: &str = "create_database";
pub const DROP_DATABASE: &str = "drop_database";
pub const LIST_DATABASES: &str = "list_databases";
pub const DATABASE_EXISTS: &str = "database_exists";
pub const CREATE_TABLE: &str = "create_table";
pub const DROP_TABLE: &str = "drop_table";
pub const LIST_TABLES: &str = "list_tables";
pub const TABLE_EXISTS: &str = "table_exists";
pub const GET_TABLE: &str = "get_table";
pub const ALTER_TABLE: &str = "alter_table";
pub const CREATE_PARTITION: &str = "create_partition";
pub const DROP_PARTITION: &str = "drop_partition";
pub const LIST_PARTITIONS: &str = "list_partitions";
pub const LATEST_KV_SNAPSHOT: &str = "latest_kv_snapshot";
pub const LAKE_SNAPSHOT: &str = "lake_snapshot";
pub const REGISTER_PRODUCER_OFFSETS: &str = "register_producer_offsets";
pub const GET_PRODUCER_OFFSETS: &str = "get_producer_offsets";
pub const DELETE_PRODUCER_OFFSETS: &str = "delete_producer_offsets";
pub const DESCRIBE_CLUSTER: &str = "describe_cluster";
pub const GET_CONFIG: &str = "get_config";
pub const NODE_STATS: &str = "node_stats";
pub const REBALANCE: &str = "rebalance";
pub const HEALTH: &str = "health";

pub const ALL: &[(&str, &str)] = &[
    (INIT_WRITER, "allocate a writer id for idempotent writes"),
    (
        LIST_OFFSETS,
        "earliest, latest or timestamp offset of a bucket",
    ),
    (LOOKUP, "rows of a primary-key bucket by encoded keys"),
    (
        PREFIX_LOOKUP,
        "rows of a primary-key bucket by encoded key prefix",
    ),
    (METADATA, "nodes, coordinator and bucket leaders"),
    (CREATE_DATABASE, "create a database (coordinator)"),
    (DROP_DATABASE, "drop a database (coordinator)"),
    (LIST_DATABASES, "database names"),
    (DATABASE_EXISTS, "whether a database exists"),
    (CREATE_TABLE, "create a table (coordinator)"),
    (DROP_TABLE, "drop a table (coordinator)"),
    (LIST_TABLES, "table names of a database"),
    (TABLE_EXISTS, "whether a table exists"),
    (
        GET_TABLE,
        "descriptor, schemas and bucket leaders of a table",
    ),
    (
        ALTER_TABLE,
        "add columns or change options of a table (coordinator)",
    ),
    (CREATE_PARTITION, "create a partition (coordinator)"),
    (DROP_PARTITION, "drop a partition (coordinator)"),
    (LIST_PARTITIONS, "partitions of a table"),
    (
        LATEST_KV_SNAPSHOT,
        "newest completed KV snapshot of a bucket",
    ),
    (LAKE_SNAPSHOT, "what the lake holds of a table"),
    (
        REGISTER_PRODUCER_OFFSETS,
        "record a sink's start offsets for undo before its first checkpoint (coordinator)",
    ),
    (GET_PRODUCER_OFFSETS, "a sink's recorded start offsets"),
    (
        DELETE_PRODUCER_OFFSETS,
        "forget a sink's start offsets (coordinator)",
    ),
    (
        DESCRIBE_CLUSTER,
        "every node with liveness and leader load, the coordinator, table and bucket counts",
    ),
    (GET_CONFIG, "the effective configuration of this node"),
    (
        NODE_STATS,
        "this node's hosted buckets (offsets, KV state, retention) and, on the coordinator, the tiering schedule",
    ),
    (
        REBALANCE,
        "even bucket leadership out across live nodes; returns the moves (coordinator)",
    ),
    (HEALTH, "liveness of this node: cheap, for probes"),
];
