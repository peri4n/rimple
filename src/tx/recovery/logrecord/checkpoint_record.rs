use std::sync::{Arc, Mutex};

use crate::{
    file::Page,
    log::manager::LogManager,
    tx::recovery::logrecord::{LogRecord, TxOp, UndoContext},
};

pub struct CheckpointRecord {}

impl CheckpointRecord {
    pub fn new() -> Self {
        CheckpointRecord {}
    }

    pub(crate) fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        tx_num: i32,
    ) -> anyhow::Result<usize> {
        let mut page = Page::with_size(std::mem::size_of::<i32>() * 2);
        page.set_integer(0, TxOp::Checkpoint as i32)?;
        page.set_integer(std::mem::size_of::<i32>(), tx_num)?;
        log_manager.lock().unwrap().append(page.content())
    }
}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> TxOp {
        TxOp::Checkpoint
    }

    fn tx_num(&self) -> i32 {
        -1
    }

    fn undo(&self, _ctx: &mut UndoContext) -> anyhow::Result<()> {
        Ok(())
    }
}
