use crate::{file::Page, log::manager::LogManager, tx::recovery::logrecord::{LogRecord, TxOp}};

pub struct CommitRecord {
    tx_num: i32,
}
impl CommitRecord {
    fn new(page: Page) -> anyhow::Result<Self> {
        let tpos = std::mem::size_of::<i32>();
        let tx_num = page.get_integer(tpos)?;
        Ok(CommitRecord { tx_num })
    }

    pub(crate) fn write_to_log(log_manager: &mut LogManager, tx_num: i32) -> anyhow::Result<usize> {
        let mut page = Page::with_size(std::mem::size_of::<i32>() * 2);
        page.set_integer(0, TxOp::Commit as i32)?;
        page.set_integer(std::mem::size_of::<i32>(), tx_num)?;

        log_manager.append(&page.content())
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> crate::tx::recovery::logrecord::TxOp {
        TxOp::Commit
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx: &mut crate::tx::transaction::Transaction) {
        // No undo action needed for commit record.
    }
}
