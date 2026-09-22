//! Auto-increment id allocation through a metadata counter, or a refusing sequence when a table has none.

use std::sync::Arc;

use async_trait::async_trait;
use mink_metadata::{Counter, Handle};
use mink_table::{Id, Schema};
use mink_tablet::Error;

pub struct Sequence {
    node: Handle,
    counter: Counter,
}

impl Sequence {
    pub fn new(node: Handle, table_id: Id, column_id: u32) -> Self {
        Sequence {
            node,
            counter: Counter::AutoIncrement {
                table_id,
                column_id,
            },
        }
    }
}

pub fn for_table(node: Handle, table_id: Id, schema: &Schema) -> Arc<dyn mink_tablet::Sequence> {
    let column = schema
        .auto_increment()
        .and_then(|index| schema.column(index))
        .and_then(|column| column.id());

    match column {
        Some(column) => Arc::new(Sequence::new(node, table_id, column.0)),
        None => Arc::new(NoSequence),
    }
}

struct NoSequence;

#[async_trait]
impl mink_tablet::Sequence for NoSequence {
    async fn get_and_add(&self, _count: u64) -> Result<u64, Error> {
        Err(Error::Sequence("table has no auto-increment column".into()))
    }
}

#[async_trait]
impl mink_tablet::Sequence for Sequence {
    async fn get_and_add(&self, count: u64) -> Result<u64, Error> {
        self.node
            .allocate(self.counter, count)
            .await
            .map_err(|e| Error::Sequence(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use mink_metadata::LocalSink;
    use mink_tablet::Sequence as _;

    use super::*;

    #[tokio::test]
    async fn counters_are_contiguous_per_column_and_independent_across_them() {
        let (sink, views) = LocalSink::new();
        let node = Handle::new(1, 1, Arc::new(sink), views);
        let a = Sequence::new(node.clone(), Id(7), 2);
        let b = Sequence::new(node, Id(7), 3);

        assert_eq!(a.get_and_add(100).await.unwrap(), 0);
        assert_eq!(a.get_and_add(100).await.unwrap(), 100);
        assert_eq!(b.get_and_add(5).await.unwrap(), 0);
        assert_eq!(a.get_and_add(1).await.unwrap(), 200);
        assert!(a.get_and_add(0).await.is_err());
    }
}
