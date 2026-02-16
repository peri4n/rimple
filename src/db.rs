use crate::file::file_manager::FileManager;
use std::path::Path;

pub struct SimpleDB {
    file_manager: FileManager,
}

impl SimpleDB {
    pub fn new(dirname: impl AsRef<Path>, block_size: usize) -> std::io::Result<Self> {
        let file_manager = FileManager::new(dirname, block_size)?;

        Ok(SimpleDB { file_manager })
    }

    pub fn file_manager(&self) -> &FileManager {
        &self.file_manager
    }
}
