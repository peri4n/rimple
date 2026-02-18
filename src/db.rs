use crate::{file::manager::FileManager, log::manager::LogManager};
use std::{path::Path, sync::{Arc, Mutex}};

pub struct SimpleDB {
    file_manager: Arc<FileManager>,
    log_manager: Arc<Mutex<LogManager>>,
}

impl SimpleDB {

        pub const LOG_FILE: &'static str = "simpledb.log";

    pub fn new(dirname: impl AsRef<Path>, block_size: usize) -> std::io::Result<Self> {
        let file_manager = Arc::new(FileManager::new(&dirname, block_size)?);
        let log_manager = Arc::new(Mutex::new(LogManager::new(file_manager.clone(), dirname.as_ref().join(Self::LOG_FILE))?));

        Ok(SimpleDB { file_manager, log_manager })
    }

    pub fn file_manager(&self) -> &FileManager {
        &self.file_manager
    }

    pub fn log_manager(&self) -> &Mutex<LogManager> {
        &self.log_manager
    }
}
