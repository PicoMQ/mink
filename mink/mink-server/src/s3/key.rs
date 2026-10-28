use mink_core::{File, TableLocation};

pub(crate) struct Key {
    path: String,
    object: Object,
}

pub(crate) enum Object {
    Metadata { topic: String },
    Data(File),
}

impl Key {
    pub(crate) fn parse(warehouse: &str, path: &str) -> Option<Key> {
        let object = match TableLocation::parse_data_file(warehouse, path) {
            Some(file) => Object::Data(file),
            None => Object::Metadata {
                topic: TableLocation::parse_metadata(warehouse, path)?.to_owned(),
            },
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
            Object::Metadata { .. } => None,
        }
    }

    pub(crate) fn topic(&self) -> &str {
        match &self.object {
            Object::Metadata { topic } => topic,
            Object::Data(file) => file.partition().topic(),
        }
    }
}
