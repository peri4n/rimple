use log::info;

use crate::{buffer::manager::BufferManager, file::manager::FileManager, log::manager::LogManager};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

pub struct SimpleDB {
    file_manager: Arc<FileManager>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl SimpleDB {
    pub const LOG_FILE: &'static str = "simpledb.log";

    pub fn new(dirname: impl AsRef<Path>, block_size: usize) -> std::io::Result<Self> {
        info!("Start to initialize the database in folder {:?} with block size {}", dirname.as_ref(), block_size);
        let file_manager = Arc::new(FileManager::new(&dirname, block_size)?);
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            file_manager.clone(),
            dirname.as_ref().join(Self::LOG_FILE),
        )?));

        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            file_manager.clone(),
            log_manager.clone(),
            8, // default number of buffers
        )));

        info!("Database initialization done");
        Ok(SimpleDB {
            file_manager,
            log_manager,
            buffer_manager,
        })
    }

    pub fn file_manager(&self) -> &FileManager {
        &self.file_manager
    }

    pub fn log_manager(&self) -> &Mutex<LogManager> {
        &self.log_manager
    }
}
