use std::{path::Path, sync::Arc};

use crate::{
    file::{BlockId, FileManager},
    log::manager::LogManager,
};

pub struct Transaction {
    file_manager: Arc<FileManager>,
    log_manager: Arc<LogManager>,
}

impl Transaction {
    pub fn new(file_manager: Arc<FileManager>, log_manager: Arc<LogManager>) -> Self {
        Transaction {
            file_manager,
            log_manager,
        }
    }

    pub fn commit(&mut self) {
        todo!()
    }

    pub fn rollback(&mut self) {
        todo!()
    }

    pub fn recover(&mut self) {
        todo!()
    }

    pub fn pin(&mut self, block_id: &BlockId) {
        todo!()
    }

    pub fn unpin(&mut self, block_id: &BlockId) {
        todo!()
    }

    pub fn get_int(&mut self, block_id: &BlockId, offset: usize) -> i32 {
        todo!()
    }

    pub fn set_int(&mut self, block_id: &BlockId, offset: usize, value: i32, log: bool) {
        todo!()
    }

    pub fn get_string(&mut self, block_id: &BlockId, offset: usize) -> String {
        todo!()
    }

    pub fn set_string(&mut self, block_id: &BlockId, offset: usize, value: &str, log: bool) {
        todo!()
    }

    pub fn available_buffers(&self) -> usize {
        todo!()
    }

    pub fn size(&self, path: &Path) -> usize {
        todo!()
    }

    pub fn append(&mut self, path: &Path) -> BlockId {
        todo!()
    }

    pub fn block_size(&self) -> usize {
        todo!()
    }
}
