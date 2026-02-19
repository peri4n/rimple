use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq)]
pub struct BlockId {
    path: PathBuf,
    block_no: u64,
}

impl BlockId {
    pub fn new(path: PathBuf, block_no: u64) -> Self {
        Self { path, block_no }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn block_no(&self) -> u64 {
        self.block_no
    }
}
