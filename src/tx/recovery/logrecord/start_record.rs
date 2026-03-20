use std::{mem, sync::{Arc, Mutex}};

use crate::{
    file::Page,
    log::manager::LogManager,
    tx::recovery::logrecord::{LogRecord, TxOp, UndoContext},
};

pub struct StartRecord {
    tx_num: i32,
}

impl StartRecord {
    pub fn new(page: Page) -> anyhow::Result<Self> {
        let tpos = mem::size_of::<i32>();
        let tx_num = page.get_integer(tpos)?;
        Ok(StartRecord { tx_num })
    }

    pub(crate) fn write_to_log(log_manager: Arc<Mutex<LogManager>>, tx_num: i32) -> anyhow::Result<usize> {
        let mut page = Page::with_size(mem::size_of::<i32>() * 2);
        page.set_integer(0, TxOp::Start as i32)?;
        page.set_integer(mem::size_of::<i32>(), tx_num)?;
        log_manager.lock().unwrap().append(page.content())
    }
}

impl LogRecord for StartRecord {
    fn op(&self) -> TxOp {
        TxOp::Start
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _ctx: &mut UndoContext) -> anyhow::Result<()> {
        Ok(())
    }
}
