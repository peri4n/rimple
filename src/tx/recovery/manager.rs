use anyhow::anyhow;
use std::sync::{Arc, Mutex};

use crate::{
    buffer::{buffer::Buffer, manager::BufferManager},
    log::manager::LogManager,
    tx::{
        recovery::logrecord::{
            TxOp, checkpoint_record::CheckpointRecord, commit_record::CommitRecord, from_page,
            rollback_record::RollbackRecord, set_i32_record::SetI32Record,
            set_string_record::SetStringRecord,
        },
        transaction::Transaction,
    },
};

pub struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    transaction: Arc<Mutex<Transaction>>,
    tx_num: i32,
}

impl RecoveryManager {
    pub fn new(
        transaction: Arc<Mutex<Transaction>>,
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
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num)?;
        let lsn = CommitRecord::write_to_log(self.log_manager.clone(), self.tx_num)?;
        self.log_manager.lock().unwrap().flush(lsn)
    }

    pub fn rollback(&mut self) -> anyhow::Result<()> {
        self.do_rollback();
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num)?;
        let lsn = RollbackRecord::write_to_log(self.log_manager.clone(), self.tx_num)?;
        self.log_manager.lock().unwrap().flush(lsn)
    }

    pub fn recover(&mut self) -> anyhow::Result<()> {
        self.do_recover();
        self.buffer_manager
            .lock()
            .unwrap()
            .flush_all(self.tx_num)
            .unwrap();
        let lsn = CheckpointRecord::write_to_log(self.log_manager.clone(), self.tx_num).unwrap();
        self.log_manager.lock().unwrap().flush(lsn)
    }

    pub fn set_int(
        &self,
        buffer: &mut Buffer,
        offset: usize,
        _new_value: i32,
    ) -> anyhow::Result<usize> {
        let old_val = buffer.contents().get_integer(offset)?;
        let block_id = buffer
            .block_id()
            .ok_or_else(|| anyhow!("Buffer is not assigned to a block"))?;
        SetI32Record::write_to_log(
            self.log_manager.clone(),
            self.tx_num,
            block_id,
            offset,
            old_val,
        )
    }

    pub fn set_string(
        &self,
        buffer: &mut Buffer,
        offset: usize,
        _new_value: &str,
    ) -> anyhow::Result<usize> {
        let old_val = buffer.contents().get_string(offset)?;
        let block_id = buffer
            .block_id()
            .ok_or_else(|| anyhow!("Buffer is not assigned to a block"))?;
        SetStringRecord::write_to_log(
            self.log_manager.clone(),
            self.tx_num,
            block_id,
            offset,
            &old_val,
        )
    }

    fn do_rollback(&mut self) -> anyhow::Result<()> {
        for entry in self.log_manager.lock().unwrap().iter()? {
            let log_record = from_page(&entry)?;
            if log_record.op() == TxOp::Start {
                return Ok(());
            }

            let mut tx = self.transaction.lock().unwrap();
            log_record.undo(&mut tx);
        }
        return Ok(());
    }

    fn do_recover(&mut self) -> anyhow::Result<()> {
        let mut finished_txs = vec![];
        for entry in self.log_manager.lock().unwrap().iter().unwrap() {
            let log_record = from_page(&entry).unwrap();
            // TODO: maybe a match statement would be better here
            if log_record.op() == TxOp::Checkpoint {
                return Ok(());
            }
            if log_record.op() == TxOp::Commit || log_record.op() == TxOp::Rollback {
                finished_txs.push(log_record.tx_num());
            } else if !finished_txs.contains(&log_record.tx_num()) {
                let mut tx = self.transaction.lock().unwrap();
                log_record.undo(&mut tx);
            }
        }
        Ok(())
    }
}
