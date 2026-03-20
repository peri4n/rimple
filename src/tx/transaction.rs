use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::manager::BufferManager,
    file::{BlockId, FileManager},
    log::manager::LogManager,
    tx::{
        bufferlist::BufferList,
        concurrency::{lock_table::LockTable, manager::ConcurrencyManager},
        recovery::manager::RecoveryManager,
    },
};

static END_OF_FILE: u64 = 42;

pub struct Transaction {
    file_manager: Arc<FileManager>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    recovery_manager: Arc<Mutex<RecoveryManager>>,
    concurrency_manager: ConcurrencyManager,
    tx_num: i32,
    buffer_list: BufferList,
}

impl Transaction {
    pub fn new(
        file_manager: Arc<FileManager>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        tx_num: Arc<Mutex<i32>>,
        lock_table: Arc<Mutex<LockTable>>,
    ) -> Self {
        let tx_num = next_tx_num(tx_num);
        let recovery_manager = Arc::new(Mutex::new(RecoveryManager::new(
            tx_num,
            log_manager.clone(),
            buffer_manager.clone(),
        )));
        Self {
            file_manager,
            buffer_manager: buffer_manager.clone(),
            recovery_manager,
            concurrency_manager: ConcurrencyManager::new(lock_table),
            tx_num,
            buffer_list: BufferList::new(buffer_manager),
        }
    }

    pub fn commit(&mut self) -> anyhow::Result<()> {
        let mut recovery_manager = self
            .recovery_manager
            .lock()
            .expect("Mutex of recovery manager poisoned");
        recovery_manager.commit()?;

        Ok(())
    }

    pub fn rollback(&mut self) -> anyhow::Result<()> {
        self.recovery_manager.lock().unwrap().rollback()?;
        self.concurrency_manager.release()?;
        self.buffer_list.unpin_all()?;

        Ok(())
    }

    pub fn recover(&mut self) -> anyhow::Result<()> {
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num)?;
        self.recovery_manager.lock().unwrap().recover()
    }

    pub fn pin(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        self.buffer_list.pin(block_id)
    }

    pub fn unpin(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        self.buffer_list.unpin(block_id)
    }

    pub fn get_int(&mut self, block_id: &BlockId, offset: usize) -> anyhow::Result<i32> {
        let buff = self
            .buffer_list
            .get_buffer(block_id)
            .unwrap()
            .lock()
            .unwrap();
        buff.contents().get_integer(offset)
    }

    pub fn set_int(
        &mut self,
        block_id: &BlockId,
        offset: usize,
        value: i32,
        log: bool,
    ) -> anyhow::Result<()> {
        self.concurrency_manager.x_lock(block_id)?;
        let mut buff = self
            .buffer_list
            .get_buffer(block_id)
            .unwrap()
            .lock()
            .unwrap();
        let mut lsn: i32 = -1;
        if log {
            let rm = self.recovery_manager.lock().unwrap();
            lsn = rm.set_int(&mut buff, offset, value)?.try_into().unwrap();
        }
        let p = buff.contents_mut();
        p.set_integer(offset, value)?;
        buff.set_modified(self.tx_num, lsn);

        Ok(())
    }

    pub fn get_string(&mut self, block_id: &BlockId, offset: usize) -> anyhow::Result<String> {
        self.concurrency_manager.s_lock(block_id)?;
        let buff = self
            .buffer_list
            .get_buffer(block_id)
            .unwrap()
            .lock()
            .unwrap();
        buff.contents().get_string(offset)
    }

    pub fn set_string(
        &mut self,
        block_id: &BlockId,
        offset: usize,
        value: &str,
        log: bool,
    ) -> anyhow::Result<()> {
        self.concurrency_manager.x_lock(block_id)?;
        let mut buff = self
            .buffer_list
            .get_buffer(block_id)
            .unwrap()
            .lock()
            .unwrap();
        let mut lsn: i32 = -1;
        if log {
            let rm = self.recovery_manager.lock().unwrap();
            lsn = rm.set_string(&mut buff, offset, value)?.try_into().unwrap();
        }
        let p = buff.contents_mut();
        p.set_string(offset , value)?;
        buff.set_modified(self.tx_num, lsn);

        Ok(())
    }

    pub fn available_buffers(&self) -> usize {
        self.buffer_manager.lock().unwrap().available()
    }

    pub fn size(&mut self, path: &Path) -> anyhow::Result<u64> {
        let dummyblk = BlockId::new(path.to_path_buf(), END_OF_FILE);
        self.concurrency_manager.s_lock(&dummyblk)?;
        self.file_manager.size(path)
    }

    pub fn append(&mut self, path: &Path) -> anyhow::Result<BlockId> {
        let dummyblk = BlockId::new(path.to_path_buf(), END_OF_FILE);
        self.concurrency_manager.x_lock(&dummyblk)?;
        self.file_manager.append_block(path)
    }

    pub fn block_size(&self) -> usize {
        self.file_manager.block_size()
    }
}

fn next_tx_num(tx_num: Arc<Mutex<i32>>) -> i32 {
    let mut num = tx_num.lock().expect("Mutex of tx_num poisoned");
    *num += 1;
    *num
}
