//! Encodes a table path as the Flight descriptor path segments.

use mink_table::Path;

pub fn descriptor_path(path: &Path) -> Vec<String> {
    vec![
        path.database().as_str().to_owned(),
        path.table().as_str().to_owned(),
    ]
}
