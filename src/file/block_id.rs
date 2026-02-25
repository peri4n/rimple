use std::{fmt::Display, path::{Path, PathBuf}};

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

impl Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}/{}", self.path(), self.block_no()))
    }
}
