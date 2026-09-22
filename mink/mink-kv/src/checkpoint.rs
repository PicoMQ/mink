//! A checkpoint directory scanned into its files, telling shared table files from private ones.

use std::fs;
use std::path::{Path, PathBuf};

use crate::Error;

pub const TABLE_FILE_SUFFIX: &str = ".sst";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFile {
    pub path: PathBuf,
    pub size: u64,
    pub shared: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub dir: PathBuf,
    pub files: Vec<CheckpointFile>,
}

impl Checkpoint {
    pub fn scan(dir: &Path) -> Result<Self, Error> {
        let mut files = Vec::new();
        walk(dir, dir, &mut files)?;
        files.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(Checkpoint {
            dir: dir.to_path_buf(),
            files,
        })
    }

    pub fn shared_files(&self) -> impl Iterator<Item = &CheckpointFile> {
        self.files.iter().filter(|f| f.shared)
    }

    pub fn private_files(&self) -> impl Iterator<Item = &CheckpointFile> {
        self.files.iter().filter(|f| !f.shared)
    }

    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    pub fn discard(self) -> Result<(), Error> {
        fs::remove_dir_all(&self.dir)?;

        Ok(())
    }
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<CheckpointFile>) -> Result<(), Error> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk(root, &path, out)?;
        } else if kind.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| Error::Corrupt(e.to_string()))?
                .to_path_buf();
            let shared = relative
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(TABLE_FILE_SUFFIX));
            out.push(CheckpointFile {
                path: relative,
                size: entry.metadata()?.len(),
                shared,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_classifies_and_sorts() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("sstables")).unwrap();
        fs::write(dir.path().join("sstables/2.sst"), [0; 10]).unwrap();
        fs::write(dir.path().join("sstables/1.sst"), [0; 5]).unwrap();
        fs::write(dir.path().join("MANIFEST"), [0; 3]).unwrap();

        let checkpoint = Checkpoint::scan(dir.path()).unwrap();
        let paths: Vec<_> = checkpoint
            .files
            .iter()
            .map(|f| f.path.to_str().unwrap())
            .collect();
        assert_eq!(paths, vec!["MANIFEST", "sstables/1.sst", "sstables/2.sst"]);
        assert_eq!(checkpoint.shared_files().count(), 2);
        assert_eq!(checkpoint.private_files().count(), 1);
        assert_eq!(checkpoint.total_size(), 18);

        checkpoint.discard().unwrap();
        assert!(!dir.path().exists());
    }
}
