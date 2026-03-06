use anyhow::anyhow;
use std::sync::{Arc, Mutex};

use crate::{
    buffer::{buffer::Buffer, manager::BufferManager},
    log::manager::LogManager,
    tx::{recovery::logrecord::commit_record::CommitRecord, transaction::Transaction},
};

pub struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    transaction: Arc<Transaction>,
    tx_num: i32,
}

impl RecoveryManager {
    pub fn new(
        transaction: Arc<Transaction>,
        tx_num: i32,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
    ) -> Self {
        RecoveryManager {
            log_manager,
            buffer_manager,
            transaction,
            tx_num,
        }
    }

    pub fn commit(&mut self) -> anyhow::Result<()> {
        let mut buffer_manager = self
            .buffer_manager
            .lock()
            .map_err(|e| anyhow!("Mutex of buffer manager poisoned: {}", e))?;
        buffer_manager.flush_all(self.tx_num)?;
        let mut log_manager = self
            .log_manager
            .lock()
            .map_err(|e| anyhow!("Mutex of log manager poisoned: {}", e))?;
        let lsn = CommitRecord::write_to_log(&mut log_manager, self.tx_num)?;
        log_manager.flush(lsn)?;
        Ok(())
    }

    pub fn rollback(&mut self) {
        todo!()
    }

    pub fn recover(&mut self) {
        todo!()
    }

    pub fn set_int(&self, buffer: &mut Buffer, offset: usize, value: i32) {
        todo!()
    }

    pub fn set_string(&self, buffer: &mut Buffer, offset: usize, value: &str) {
        todo!()
    }

    fn do_rollback(&mut self) {
        todo!()
    }

    fn do_recovery(&mut self) {
        todo!()
    }
}
