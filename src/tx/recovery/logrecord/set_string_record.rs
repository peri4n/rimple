use std::{
    mem,
    sync::{Arc, Mutex},
};

use crate::{
    file::{BlockId, Page},
    log::manager::LogManager,
    tx::{
        recovery::logrecord::{LogRecord, TxOp},
        transaction::Transaction,
    },
};

pub struct SetStringRecord {
    tx_num: i32,
    block_id: BlockId,
    offset: usize,
    value: String,
}

impl SetStringRecord {
    //TODO: I really don't like that we are passing a page here.
    // The real interface is: "Restore it from a byte slice".
    pub fn new(page: Page) -> anyhow::Result<Self> {
        let tpos = mem::size_of::<i32>();
        let tx_num = page.get_integer(tpos)?;
        let fpos = tpos + mem::size_of::<i32>();
        let file_name = page.get_string(fpos)?;
        let bpos = fpos + Page::max_length(&file_name);
        let block_num = page.get_integer(bpos)? as u64;
        let opos = bpos + mem::size_of::<i32>();
        let offset = page.get_integer(opos)? as usize;
        let vpos = opos + mem::size_of::<i32>();
        let value = page.get_string(vpos)?;
        Ok(SetStringRecord {
            tx_num,
            block_id: BlockId::new(file_name.into(), block_num),
            offset,
            value,
        })
    }

    pub(crate) fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        tx_num: i32,
        block_id: &BlockId,
        offset: usize,
        value: &str,
    ) -> anyhow::Result<usize> {
        let tpos = mem::size_of::<i32>();
        let fpos = tpos + mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(block_id.path().to_str().unwrap()); // the unwrap seams odd
        let opos = bpos + mem::size_of::<i32>();
        let vpos = opos + mem::size_of::<i32>();
        let record_size = vpos + Page::max_length(value);

        let mut page = Page::with_size(record_size);
        page.set_integer(0, TxOp::SetString as i32)?;
        page.set_integer(tpos, tx_num)?;
        page.set_string(fpos, block_id.path().to_str().unwrap())?;
        page.set_integer(bpos, block_id.block_no() as i32)?;
        page.set_integer(opos, offset as i32)?;
        page.set_string(vpos, value)?;
        log_manager.lock().unwrap().append(page.content())
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> TxOp {
        TxOp::SetString
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block_id);

        // don't log the undo operation, otherwise it will cause an infinite loop of undoing the undo.
        tx.set_string(&self.block_id, self.offset, &self.value, false);
        tx.unpin(&self.block_id);
    }
}

impl std::fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETSTRING {} {:?} {} {} {}>",
            self.tx_num,
            self.block_id.path(),
            self.block_id.block_no(),
            self.offset,
            self.value
        )
    }
}
