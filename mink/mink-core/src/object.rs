#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    TableMetadata,
    ManifestList,
    Manifest,
    Statistics,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Object {
    path: String,
    kind: ObjectKind,
}

impl ObjectKind {
    pub const ALL: [ObjectKind; 4] = [
        ObjectKind::TableMetadata,
        ObjectKind::ManifestList,
        ObjectKind::Manifest,
        ObjectKind::Statistics,
    ];

    pub fn name(self) -> &'static str {
        match self {
            ObjectKind::TableMetadata => "table_metadata",
            ObjectKind::ManifestList => "manifest_list",
            ObjectKind::Manifest => "manifest",
            ObjectKind::Statistics => "statistics",
        }
    }

    pub fn from_name(name: &str) -> Option<ObjectKind> {
        ObjectKind::ALL.into_iter().find(|kind| kind.name() == name)
    }
}

impl Object {
    pub fn new(path: impl Into<String>, kind: ObjectKind) -> Object {
        Object {
            path: path.into(),
            kind,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn kind(&self) -> ObjectKind {
        self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_round_trip_through_their_names() {
        for kind in ObjectKind::ALL {
            assert_eq!(ObjectKind::from_name(kind.name()), Some(kind));
        }

        assert_eq!(ObjectKind::from_name("data"), None);
    }
}
