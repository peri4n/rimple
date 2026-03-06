use std::mem;

use crate::{file::{BlockId, Page}, tx::{recovery::logrecord::LogRecord, transaction::Transaction}};

pub struct SetI32Record {
    tx_num: i32,
    block_id: BlockId,
    offset: i32,
    value: i32,
}

impl SetI32Record {
    pub fn new(page: Page) -> anyhow::Result<Self> {
        let tpos = mem::size_of::<i32>();
        let tx_num = page.get_integer(tpos)?;
        let fpos = tpos + mem::size_of::<i32>();
        let file_name = page.get_string(fpos)?;
        let bpos = fpos + Page::max_length(&file_name);
        let block_num = page.get_integer(bpos)? as u64;
        let opos = bpos + mem::size_of::<i32>();
        let offset = page.get_integer(opos)?;
        let vpos = opos + mem::size_of::<i32>();
        let value = page.get_integer(vpos)?;
        Ok(SetI32Record {
            tx_num,
            block_id: BlockId::new(file_name.into(), block_num),
            offset,
            value,
        })
    }

    pub(crate) fn write_to_log(
        log_manager: &mut crate::log::manager::LogManager,
        tx_num: i32,
        block_id: &BlockId,
        offset: i32,
        value: i32,
    ) -> anyhow::Result<usize> {
        let tpos = mem::size_of::<i32>();
        let fpos = tpos + mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(block_id.path().to_str().unwrap()); // the unwrap seams odd
        let opos = bpos + mem::size_of::<i32>();
        let vpos = opos + mem::size_of::<i32>();
        let record_size = vpos + mem::size_of::<i32>();

        let mut page = Page::with_size(record_size);
        page.set_integer(0, crate::tx::recovery::logrecord::TxOp::SetI32 as i32)?;
        page.set_integer(tpos, tx_num)?;
        page.set_string(fpos, block_id.path().to_str().unwrap())?;
        page.set_integer(bpos, block_id.block_no() as i32)?;
        page.set_integer(opos, offset)?;
        page.set_integer(vpos, value)?;

        log_manager.append(&page.content())
    }
}

impl LogRecord for SetI32Record {
    fn op(&self) -> crate::tx::recovery::logrecord::TxOp {
        crate::tx::recovery::logrecord::TxOp::SetI32
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block_id);
        tx.set_int(&self.block_id, self.offset as usize, self.value, false);
        tx.unpin(&self.block_id);
    }
}
