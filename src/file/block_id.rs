use std::path::{Path, PathBuf};

pub struct BlockId {
    path: PathBuf,
    id: u64,
}

impl BlockId {
    pub fn new(path: PathBuf, id: u64) -> Self {
        Self { path, id }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}
