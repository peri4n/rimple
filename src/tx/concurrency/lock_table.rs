use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::file::BlockId;

#[derive(Debug, thiserror::Error)]
enum LockTableError {
    #[error("Lock table is currently unavailable")]
    LockAbort,
}

#[derive(Default)]
pub struct LockTable {
    locks: Arc<Mutex<HashMap<BlockId, i32>>>,
}

impl LockTable {
    pub fn new() -> Self {
        LockTable {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn s_lock(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        if let Ok(mut locks) = self.locks.try_lock()
            && !Self::has_x_lock(&locks, block_id)
        {
            *locks.entry(block_id.clone()).or_insert(0) += 1; // will not be negative
            return Ok(());
        }

        Err(From::from(LockTableError::LockAbort))
    }

    pub fn x_lock(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        todo!()
    }

    pub fn release(&mut self, block_id: &BlockId) -> anyhow::Result<()> {
        if let Ok(mut locks) = self.locks.try_lock() {
            if let Some(count) = locks.get_mut(block_id) {
                *count -= 1;
                if *count == 0 {
                    locks.remove(block_id);
                }
                return Ok(());
            }
        }
        todo!()
    }

    fn has_x_lock(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
        Self::get_lock_val(locks, blk) < 0
    }
    fn has_other_s_locks(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
        Self::get_lock_val(locks, blk) > 1
    }
    fn get_lock_val(locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> i32 {
        match locks.get(&blk) {
            Some(&ival) => ival,
            None => 0,
        }
    }
}
