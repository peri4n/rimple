use std::sync::{Arc, Mutex};

use crate::{file::Page, log::manager::LogManager, tx::recovery::logrecord::LogRecord};

pub struct RollbackRecord {
    tx_num: i32,
}

impl RollbackRecord {
    pub fn new(page: Page) -> anyhow::Result<Self> {
        let tpos = std::mem::size_of::<i32>();
        let tx_num = page.get_integer(tpos)?;
        Ok(RollbackRecord { tx_num })
    }

    pub(crate) fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        tx_num: i32,
    ) -> anyhow::Result<usize> {
        let mut page = Page::with_size(std::mem::size_of::<i32>() * 2);
        page.set_integer(0, crate::tx::recovery::logrecord::TxOp::Rollback as i32)?;
        page.set_integer(std::mem::size_of::<i32>(), tx_num)?;

        log_manager.lock().unwrap().append(&page.content())
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> crate::tx::recovery::logrecord::TxOp {
        crate::tx::recovery::logrecord::TxOp::Rollback
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx: &mut crate::tx::transaction::Transaction) {
        // No undo action needed for rollback record.
    }
}
