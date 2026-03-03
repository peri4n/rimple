use std::sync::Arc;

use crate::{buffer::manager::BufferManager, log::manager::LogManager, tx::transaction::Transaction};

pub struct RecoveryManager {
    log_manager: Arc<LogManager>,
    buffer_manager: Arc<BufferManager>,
    transaction: Transaction,
    tx_num: i32,
}

impl RecoveryManager {
    pub fn new(tx: Transaction, tx_num: i32, log_manager: Arc<LogManager>, buffer_manager: Arc<BufferManager>) -> Self {
        RecoveryManager {
            log_manager,
            buffer_manager,
            transaction: tx,
            tx_num,
        }
    }

    pub fn recover(&mut self) {
        todo!()
    }
}
