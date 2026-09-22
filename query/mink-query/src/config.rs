//! What an engine needs to know: where the cluster is, how to reach the lake, and how much to spend.

#[derive(Debug, Clone)]
pub struct Config {
    pub bootstrap: Vec<String>,
    pub database: Option<String>,
    pub lake: Option<mink_lake::Config>,
    pub target_partitions: Option<usize>,
    pub batch_size: usize,
    pub memory_limit: Option<usize>,
}

impl Config {
    pub const CATALOG: &'static str = "mink";

    pub fn new(bootstrap: Vec<String>) -> Self {
        Config {
            bootstrap,
            database: None,
            lake: None,
            target_partitions: None,
            batch_size: 8192,
            memory_limit: None,
        }
    }
}
