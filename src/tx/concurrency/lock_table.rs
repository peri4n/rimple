use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::file::PageId;

#[derive(Debug, thiserror::Error)]
enum LockTableError {
    #[error("Lock table is currently unavailable")]
    LockAbort,
}

#[derive(Default)]
pub struct LockTable {
    locks: Arc<Mutex<HashMap<PageId, i32>>>,
}

/// Handles locking for transactions.
/// Each page can have multiple shared locks (indicated by a positive integer value in the locks map),
/// or a single exclusive lock (indicated by a negative integer value in the locks map).
/// If the page is not locked, it will not be present in the locks map.
impl LockTable {
    pub fn new() -> Self {
        LockTable {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Acquires a shared lock on the specified page.
    pub fn s_lock(&mut self, page_id: &PageId) -> anyhow::Result<()> {
        if let Ok(mut locks) = self.locks.try_lock()
            && !has_x_lock(&locks, page_id)
        {
            *locks.entry(page_id.clone()).or_insert(0) += 1; // will not be negative
            return Ok(());
        }

        Err(From::from(LockTableError::LockAbort))
    }

    /// Acquires an exclusive lock on the specified page.
    pub fn x_lock(&mut self, page_id: &PageId) -> anyhow::Result<()> {
        if let Ok(mut locks) = self.locks.try_lock()
            && !has_other_s_locks(&locks, page_id)
        {
            *locks.entry(page_id.clone()).or_insert(-1) = -1; // means eXclusive lock
            return Ok(());
        }

        Err(From::from(LockTableError::LockAbort))
    }

    /// Releases the lock on the specified page.
    /// If there are multiple shared locks, it will decrement the count.
    /// If there is only one lock (either shared or exclusive), it will remove the entry from the locks map.
    pub fn unlock(&mut self, page_id: &PageId) -> anyhow::Result<()> {
        if let Ok(mut locks) = self.locks.try_lock()
            && let Some(count) = locks.get_mut(page_id)
        {
            if *count > 1 {
                *count -= 1;
            } else {
                locks.remove(page_id);
            }
            return Ok(());
        }

        Err(From::from(LockTableError::LockAbort))
    }
}

fn has_x_lock(locks: &MutexGuard<HashMap<PageId, i32>>, blk: &PageId) -> bool {
    get_lock_val(locks, blk) < 0
}
fn has_other_s_locks(locks: &MutexGuard<HashMap<PageId, i32>>, blk: &PageId) -> bool {
    get_lock_val(locks, blk) > 1
}
fn get_lock_val(locks: &MutexGuard<HashMap<PageId, i32>>, blk: &PageId) -> i32 {
    match locks.get(blk) {
        Some(&ival) => ival,
        None => 0,
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn multiple_shared_locks_are_allowed() {
        let mut table = LockTable::new();
        table.s_lock(&PageId::new("testfile".into(), 1)).unwrap();
        table.s_lock(&PageId::new("testfile".into(), 2)).unwrap();
    }

    #[test]
    fn exclusive_lock_blocks_other_locks() {
        let mut table = LockTable::new();
        table.x_lock(&PageId::new("testfile".into(), 1)).unwrap();
        table
            .s_lock(&PageId::new("testfile".into(), 1))
            .unwrap_err();
        table.unlock(&PageId::new("testfile".into(), 1)).unwrap();
        table.s_lock(&PageId::new("testfile".into(), 1)).unwrap();
    }
}
