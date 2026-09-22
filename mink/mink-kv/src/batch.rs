//! An ordered group of puts and deletes applied atomically, tracking its byte size.

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

impl Op {
    pub fn key(&self) -> &Bytes {
        match self {
            Op::Put { key, .. } | Op::Delete { key } => key,
        }
    }

    fn size(&self) -> usize {
        match self {
            Op::Put { key, value } => key.len() + value.len(),
            Op::Delete { key } => key.len(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch {
    ops: Vec<Op>,
    size: usize,
}

impl Batch {
    pub fn new() -> Self {
        Batch::default()
    }

    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.push(Op::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn delete(&mut self, key: impl Into<Bytes>) {
        self.push(Op::Delete { key: key.into() });
    }

    fn push(&mut self, op: Op) {
        self.size += op.size();
        self.ops.push(op);
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn into_ops(self) -> Vec<Op> {
        self.ops
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_len_and_size() {
        let mut batch = Batch::new();
        assert!(batch.is_empty());
        batch.put(&b"ab"[..], &b"cde"[..]);
        batch.delete(&b"xyz"[..]);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.size(), 8);
        assert_eq!(batch.ops()[1].key(), &Bytes::from_static(b"xyz"));
    }
}
