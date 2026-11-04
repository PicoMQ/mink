use mink_core::{File, TableLocation};

use crate::buckets::bucket_and_key;

#[derive(Clone)]
pub(crate) struct Key {
    path: String,
    object: Object,
}

#[derive(Clone)]
pub(crate) enum Object {
    Metadata { topic: String },
    Data(File),
    External,
}

impl Key {
    pub(crate) fn parse(warehouse: &str, path: &str) -> Option<Key> {
        let in_warehouse = path
            .strip_prefix(warehouse)
            .is_some_and(|rest| rest.starts_with('/'));

        let object = match (
            in_warehouse,
            TableLocation::parse_data_file(warehouse, path),
        ) {
            (true, Some(file)) => Object::Data(file),
            (true, None) => Object::Metadata {
                topic: TableLocation::parse_metadata(warehouse, path)?.to_owned(),
            },
            (false, _) => {
                bucket_and_key(path)?;
                Object::External
            }
        };

        Some(Key {
            path: path.to_owned(),
            object,
        })
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn object(&self) -> &Object {
        &self.object
    }

    pub(crate) fn file(&self) -> Option<&File> {
        match &self.object {
            Object::Data(file) => Some(file),
            Object::Metadata { .. } | Object::External => None,
        }
    }
}
