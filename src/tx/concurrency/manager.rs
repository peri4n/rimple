use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{file::PageId, tx::concurrency::lock_table::LockTable};

/// Manages locks for a single transaction. Each transaction has its own ConcurrencyManager instance, which keeps track of the locks it holds.
/// The lock table is shared across all transactions, and the ConcurrencyManager interacts with it to acquire and release locks.
pub struct ConcurrencyManager {
    // static member, there should only be one lock table for the entire system
    lock_tbl: Arc<Mutex<LockTable>>,

    // TODO: Refactor string here.
    // It should be an enum with variants SharedLock and ExclusiveLock
    locks: Arc<Mutex<HashMap<PageId, String>>>,
}

// TODO: Look through
// https://github.com/cutsea110/simpledb/blob/master/src/tx/concurrency/manager.rs#L77 to see if
// the locking has to be reworked
impl ConcurrencyManager {
    pub fn new(lock_tbl: Arc<Mutex<LockTable>>) -> Self {
        Self {
            lock_tbl,
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn s_lock(&mut self, page_id: &PageId) -> anyhow::Result<()> {
        if let Ok(mut locks) = self.locks.try_lock()
            && !locks.contains_key(page_id)
        {
            self.lock_tbl.lock().unwrap().s_lock(page_id)?;
            locks.insert(page_id.clone(), "S".to_string());
        }

        Ok(())
    }

    pub fn x_lock(&mut self, page_id: &PageId) -> anyhow::Result<()> {
        if !self.has_x_lock(page_id) {
            self.s_lock(page_id)?;
            self.lock_tbl.lock().unwrap().x_lock(page_id)?;
            self.locks
                .lock()
                .unwrap()
                .insert(page_id.clone(), "X".to_string());
        }

        Ok(())
    }

    pub fn release(&mut self) -> anyhow::Result<()> {
        for blk in self.locks.lock().unwrap().keys() {
            self.lock_tbl.lock().unwrap().unlock(blk)?;
        }
        self.locks.lock().unwrap().clear();
        Ok(())
    }

    fn has_x_lock(&self, page_id: &PageId) -> bool {
        if let Some(lock_type) = self.locks.lock().unwrap().get(page_id) {
            lock_type.eq(&"X")
        } else {
            false
        }
    }
}
